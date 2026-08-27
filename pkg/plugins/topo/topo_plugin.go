// Package topo 实现拓扑感知调度插件（§7.3.2 / §8.2）。
//
// 职责：把 GPU 物理拓扑纳入过滤与打分。
//   - PreFilter：解析 Pod GPU 请求数，决定强制等级（nvlink / none）。
//   - Filter：节点 GPU 数量 + NVLink 域容量 + 拓扑健康分级（T2）。
//   - Score：共享 best-fit 决策的 TopoAffinity + GangAffinity + Balance（§8.2）。
package topo

import (
	"math"

	"github.com/chenxihui/TopoGang/pkg/allocator"
	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// Policy 是拓扑需求声明（§6.1 TopologyPolicy）。
type Policy string

const (
	// PolicyNone 尽力而为（默认）。
	PolicyNone Policy = "none"
	// PolicyNvlink 强制：必须存在单个 NVLink 域能容纳全部 GPU。
	PolicyNvlink Policy = "nvlink"
	// PolicyPCIe 预留。
	PolicyPCIe Policy = "pcie"
)

// HealthTier 是节点拓扑健康分级（§7.1 T2）。
type HealthTier int

const (
	// HealthHealthy 健康。
	HealthHealthy HealthTier = iota
	// HealthDataMissing 心跳正常但拓扑数据缺失：仅数量过滤、不选卡（s9）。
	HealthDataMissing
	// HealthHeartbeatStale 心跳过期：完全停止新分配（Filter 不返回该节点）。
	HealthHeartbeatStale
)

// NodeView 是 Filter/Score 所需的节点拓扑视图。
type NodeView struct {
	// Name 节点名。
	Name string
	// TotalGPUs 节点 GPU 总数（数量来源，§7.3.2 s9）。
	TotalGPUs int
	// FreeGPUs 空闲 GPU 数（来自 allocatable - AllocationTracker）。
	FreeGPUs int
	// Domains NVLink 域列表（§8.1）。
	Domains []topo.Domain
	// Health 拓扑健康分级（T2）。
	Health HealthTier
	// InManagedDomain 是否管理域内节点（S1）。
	InManagedDomain bool
}

// NodeFreeByDomain 返回节点各域空闲 GPU 数。
// 需要 allocator 查询，通过注入的函数获取。
type FreeQuery func(nodeName, domainID string) int

// TopoPlugin 是拓扑感知插件。
type TopoPlugin struct {
	// Tracker GPU 分配账本（§7.3.3）。
	Tracker *allocator.AllocationTracker
	// Free FreeQuery 提供域空闲数查询（可注入，便于测试）。
	Free FreeQuery
	// Params best-fit 软打分权重（§8.1）。
	Params topo.DomainScoreParams
	// ScoreWeights 打分权重（§8.2）。
	ScoreWeights ScoreWeights
}

// ScoreWeights 是 §8.2 的打分权重。
type ScoreWeights struct {
	// W1 TopoAffinity（默认 5）。
	W1 float64
	// W2 GangAffinity（默认 3）。
	W2 float64
	// W3 Balance（默认 2）。
	W3 float64
}

// DefaultScoreWeights 返回 §8.2 默认权重。
func DefaultScoreWeights() ScoreWeights {
	return ScoreWeights{W1: 5, W2: 3, W3: 2}
}

// NewTopoPlugin 构造插件。
func NewTopoPlugin(tr *allocator.AllocationTracker) *TopoPlugin {
	return &TopoPlugin{
		Tracker:     tr,
		Params:      topo.DefaultDomainScoreParams(),
		ScoreWeights: DefaultScoreWeights(),
	}
}

// PodGPURequest 是 Pod 的 GPU 需求。
type PodGPURequest struct {
	// Count 请求 GPU 数。
	Count int
	// Policy 强制等级。
	Policy Policy
	// SiblingGPUs 兄弟 Pod 已占 GPU（node -> gpuID 集合，GangAffinity）。
	SiblingGPUs map[string]map[string]bool
}

// Filter 实现节点过滤（§7.3.2）。
//
// 返回值：Allow 表示节点可作为候选；Reason 为拒绝原因。
func (p *TopoPlugin) Filter(req PodGPURequest, n NodeView) FilterResult {
	// T2：心跳过期节点完全停止分配（Filter 不返回该节点）
	if n.Health == HealthHeartbeatStale {
		return FilterResult{Allow: false, Reason: "node-heartbeat-stale"}
	}

	// ① 节点空闲 GPU 数 ≥ 请求数（数量过滤）
	if n.FreeGPUs < req.Count {
		return FilterResult{Allow: false, Reason: "insufficient-gpu-count"}
	}

	// ② 强制 nvlink：必须存在单个 NVLink 域能容纳 count 张空闲 GPU（§7.3.2）
	if req.Policy == PolicyNvlink {
		if !p.hasSingleDomainCapacity(n, req.Count) {
			return FilterResult{Allow: false, Reason: "no-single-nvlink-domain-capacity"}
		}
	}

	// ③ 数据缺失节点：仅数量过滤通过，不参与选卡（s9）
	if n.Health == HealthDataMissing {
		return FilterResult{Allow: true, DataMissing: true}
	}

	return FilterResult{Allow: true}
}

// FilterResult 是 Filter 返回。
type FilterResult struct {
	Allow bool
	// DataMissing 节点拓扑数据缺失（仅数量过滤，不选卡，s9）。
	DataMissing bool
	Reason      string
}

// hasSingleDomainCapacity 判断是否存在单个 NVLink 域能容纳 count 张空闲 GPU。
func (p *TopoPlugin) hasSingleDomainCapacity(n NodeView, count int) bool {
	for _, d := range n.Domains {
		if p.Free != nil {
			if p.Free(n.Name, d.ID) >= count {
				return true
			}
		} else if p.Tracker != nil {
			if p.Tracker.FreeCount(n.Name, d.ID) >= count {
				return true
			}
		} else if len(d.GPUIndexes) >= count {
			// 无 allocator：按域容量近似
			return true
		}
	}
	return false
}

// Score 计算节点分数（§8.2）。
//
// Score = W1·TopoAffinity + W2·GangAffinity + W3·Balance
func (p *TopoPlugin) Score(req PodGPURequest, n NodeView) float64 {
	topoAff := p.topoAffinity(n, req)
	gangAff := p.gangAffinity(n, req)
	balance := p.balance(n)
	return p.ScoreWeights.W1*topoAff + p.ScoreWeights.W2*gangAff + p.ScoreWeights.W3*balance
}

// topoAffinity 计算 TopoAffinity（§8.2）：
//   - 节点存在某域空闲 ≥ k（best-fit 命中）：1.0
//   - 跨域但 PIX：0.6 - 0.1·跨域边数
//   - 有 SYS 边：0.2
//   - 拓扑缺失/不健康：0（仅按数量，不惩罚）
func (p *TopoPlugin) topoAffinity(n NodeView, req PodGPURequest) float64 {
	if n.Health != HealthHealthy {
		return 0 // 拓扑缺失/不健康：不惩罚（§8.2）
	}
	// best-fit 命中：某域可容纳
	if p.hasSingleDomainCapacity(n, req.Count) {
		return 1.0
	}
	// 跨域：按域间链路类型近似（此处以最大跨域边类型惩罚）
	// 简化：若存在跨域，按 0.6 - 0.1·域数 退化
	if len(n.Domains) >= 2 {
		return math.Max(0.2, 0.6-0.1*float64(len(n.Domains)-1))
	}
	return 0.2
}

// gangAffinity 计算 GangAffinity（§8.2）：
//   - SameNodeAff：同节点兄弟 GPU 链路代价归一化
//   - NodeGangPacking：已占节点数越少分越高（聚拢跨节点 Gang，R5）
func (p *TopoPlugin) gangAffinity(n NodeView, req PodGPURequest) float64 {
	siblingGPUs := req.SiblingGPUs[n.Name]
	sameNodeAff := 0.0
	if len(siblingGPUs) > 0 {
		// 简化：有兄弟在同节点视为高亲和
		sameNodeAff = 1.0
	}
	// NodeGangPacking：1/(1+|N_used|)，此处由调用方注入总已占节点数
	nodePacking := p.nodePacking(len(req.SiblingGPUs))
	return 0.7*sameNodeAff + 0.3*nodePacking
}

// nodePacking 计算跨节点聚拢分（§8.2 R5）。
func (p *TopoPlugin) nodePacking(usedNodeCount int) float64 {
	return 1.0 / (1.0 + float64(usedNodeCount))
}

// balance 计算资源平衡度（§8.2）：1 - 已分配/总量。
func (p *TopoPlugin) balance(n NodeView) float64 {
	if n.TotalGPUs <= 0 {
		return 0
	}
	used := n.TotalGPUs - n.FreeGPUs
	return 1.0 - float64(used)/float64(n.TotalGPUs)
}
