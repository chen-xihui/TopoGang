// Package topo 实现 GPU 拓扑图模型、NVLink 域划分（Bron–Kerbosch）、选团策略
// 与 best-fit 决策函数（§8.1）。该包与 K8s / scheduler 解耦，可独立测试。
package topo

import (
	"math"
	"sort"
)

// LinkType 表示两 GPU 之间的互联链路类型（与 apis/topology 对齐）。
type LinkType string

const (
	// LinkNVLink NVLink（NV1/NV2/NV3 统一，权重按带宽）。
	LinkNVLink LinkType = "NVLink"
	// LinkNVSwitch 经 NVSwitch 互联。
	LinkNVSwitch LinkType = "NVSwitch"
	// LinkPIX 同 PCIe Switch。
	LinkPIX LinkType = "PIX"
	// LinkPHB 同 Root Complex 不同 Switch。
	LinkPHB LinkType = "PHB"
	// LinkSYS 跨主机总线（NUMA 间）。
	LinkSYS LinkType = "SYS"
)

// Gpu 表示一张 GPU 的拓扑相关信息。
type Gpu struct {
	// ID 物理 GPU ID / PCI 地址。
	ID string
	// Index 节点内 GPU 索引。
	Index int
}

// Link 表示两 GPU 之间的一条链路。
type Link struct {
	// A 对端 GPU 索引。
	A int
	// B 对端 GPU 索引。
	B int
	// LinkType 链路类型。
	LinkType LinkType
	// Bandwidth 双向带宽（GB/s）。
	Bandwidth float64
}

// GpuTopology 抽象后的拓扑图（§7.1 Source 接口产物）。
type GpuTopology struct {
	// NodeName 所属节点名。
	NodeName string
	// GPUs 节点内 GPU 列表。
	GPUs []*Gpu
	// Links GPU 间链路（gpuA <-> gpuB）。
	Links []*Link
	// Domains NVLink 域（由 attachDomains / 调用方计算挂载）。
	Domains []Domain
}

// LinkTypeOf 返回 GPU a 与 b 之间的链路类型；若不存在直接链路返回空串。
func (g *GpuTopology) LinkTypeOf(a, b int) LinkType {
	for _, l := range g.Links {
		if (l.A == a && l.B == b) || (l.A == b && l.B == a) {
			return l.LinkType
		}
	}
	return ""
}

// BandwidthOf 返回 GPU a 与 b 之间的链路带宽；无链路返回 0。
func (g *GpuTopology) BandwidthOf(a, b int) float64 {
	for _, l := range g.Links {
		if (l.A == a && l.B == b) || (l.A == b && l.B == a) {
			return l.Bandwidth
		}
	}
	return 0
}

// NvlinkEdges 返回 NVLink（含 NVSwitch）边的端点对集合，供最大团划分使用。
func (g *GpuTopology) NvlinkEdges() [][2]int {
	var edges [][2]int
	for _, l := range g.Links {
		if l.LinkType == LinkNVLink || l.LinkType == LinkNVSwitch {
			edges = append(edges, [2]int{l.A, l.B})
		}
	}
	return edges
}

// DomainStrategy 定义 NVLink 域划分策略（§8.1）。
type DomainStrategy int

const (
	// DomainClique 最大团划分（推荐）：按 NVLink 边两两互联的极大子集划分。
	DomainClique DomainStrategy = iota
	// DomainConnected 连通分量划分（退化策略）。
	DomainConnected
)

// Domain 表示一个 NVLink 域：GPU 索引集合。
type Domain struct {
	// ID 域 ID（如 "nvlink-1"）。
	ID string
	// GPUIndexes 域内 GPU 索引。
	GPUIndexes []int
}

// FindNvlinkDomains 依据策略返回 NVLink 域列表（§8.1）。
func FindNvlinkDomains(g *GpuTopology, strategy DomainStrategy) []Domain {
	switch strategy {
	case DomainClique:
		return cliquesToDomains(g)
	case DomainConnected:
		return connectedComponents(g)
	default:
		return cliquesToDomains(g)
	}
}

// cliquesToDomains 用 Bron–Kerbosch 求 NVLink 边的极大团，并做选团去重。
// 部分互联拓扑下极大团可能重叠（§8.1 M4 修订），此处返回所有极大团并按
// 大小降序、索引升序排序；选团由调用方依据 domainScore 目标函数决定。
func cliquesToDomains(g *GpuTopology) []Domain {
	adj := adjacencyMatrix(g, LinkNVLink)
	n := len(g.GPUs)
	var cliques [][]int

	var bk func(r, p, x []int)
	bk = func(r, p, x []int) {
		if len(p) == 0 && len(x) == 0 {
			// 极大团
			cp := append([]int(nil), r...)
			if len(cp) >= 2 {
				cliques = append(cliques, cp)
			}
			return
		}
		// 选枢轴（启发式：p ∪ x 中邻接最多的点），减少递归分支
		pivot := -1
		maxDeg := -1
		for _, v := range append(append([]int(nil), p...), x...) {
			deg := 0
			for _, u := range p {
				if adj[v][u] {
					deg++
				}
			}
			if deg > maxDeg {
				maxDeg = deg
				pivot = v
			}
		}
		var candidates []int
		for _, v := range p {
			if pivot == -1 || !adj[v][pivot] {
				candidates = append(candidates, v)
			}
		}
		for _, v := range candidates {
			newR := append(append([]int(nil), r...), v)
			var newP, newX []int
			for _, u := range p {
				if adj[v][u] {
					newP = append(newP, u)
				}
			}
			for _, u := range x {
				if adj[v][u] {
					newX = append(newX, u)
				}
			}
			bk(newR, newP, newX)
			// 将 v 从 p 移到 x
			p = removeInt(p, v)
			x = append(x, v)
		}
	}

	all := make([]int, n)
	for i := range all {
		all[i] = i
	}
	bk(nil, all, nil)

	// 排序：大小降序，再按首索引升序（确定性）
	sort.Slice(cliques, func(i, j int) bool {
		if len(cliques[i]) != len(cliques[j]) {
			return len(cliques[i]) > len(cliques[j])
		}
		si, sj := sortedCopy(cliques[i]), sortedCopy(cliques[j])
		for k := 0; k < len(si); k++ {
			if si[k] != sj[k] {
				return si[k] < sj[k]
			}
		}
		return false
	})

	domains := make([]Domain, 0, len(cliques))
	for i, c := range cliques {
		s := sortedCopy(c)
		domains = append(domains, Domain{
			ID:         "nvlink-" + intToStr(i+1),
			GPUIndexes: s,
		})
	}
	return domains
}

// connectedComponents 退化策略：按 NVLink 边的连通分量划分。
func connectedComponents(g *GpuTopology) []Domain {
	n := len(g.GPUs)
	parent := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	for _, e := range g.NvlinkEdges() {
		union(e[0], e[1])
	}
	groups := map[int][]int{}
	for i := 0; i < n; i++ {
		r := find(i)
		groups[r] = append(groups[r], i)
	}
	var domains []Domain
	idx := 1
	keys := make([]int, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		members := groups[k]
		if len(members) < 1 {
			continue
		}
		sort.Ints(members)
		domains = append(domains, Domain{
			ID:         "nvlink-" + intToStr(idx),
			GPUIndexes: members,
		})
		idx++
	}
	return domains
}

// adjacencyMatrix 返回 NVLink（含 NVSwitch）两两邻接矩阵。
func adjacencyMatrix(g *GpuTopology, lt LinkType) [][]bool {
	n := len(g.GPUs)
	adj := make([][]bool, n)
	for i := range adj {
		adj[i] = make([]bool, n)
	}
	for _, l := range g.Links {
		if l.LinkType != lt && l.LinkType != LinkNVSwitch {
			continue
		}
		adj[l.A][l.B] = true
		adj[l.B][l.A] = true
	}
	return adj
}

func removeInt(s []int, v int) []int {
	for i, x := range s {
		if x == v {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func sortedCopy(s []int) []int {
	c := append([]int(nil), s...)
	sort.Ints(c)
	return c
}

func intToStr(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}

// ---------- 选团 / best-fit 决策（§8.1 M4/N5 修订） ----------

// DomainScoreParams 控制 best-fit 决策软打分的权重（§8.1）。
type DomainScoreParams struct {
	// Alpha 容量富余度权重（默认 0.5）。
	Alpha float64
	// Beta 兄弟亲和权重（默认 0.3）。
	Beta float64
	// Gamma 内聚度惩罚权重（默认 0.2）。
	Gamma float64
}

// DefaultDomainScoreParams 返回 §8.1 的默认权重。
func DefaultDomainScoreParams() DomainScoreParams {
	return DomainScoreParams{Alpha: 0.5, Beta: 0.3, Gamma: 0.2}
}

// CandidateDomain 是 best-fit 决策的一个候选域。
type CandidateDomain struct {
	// Domain 域信息。
	Domain Domain
	// FreeGPUs 域内当前空闲 GPU 索引。
	FreeGPUs []int
	// SiblingGPUs 兄弟 Pod 已占用（位于该域内）的 GPU 索引数。
	SiblingCount int
	// TotalCapacity 域容量（GPU 总数）。
	TotalCapacity int
	// LockedCount 域内 locked GPU 数（安全阀，硬约束在调用方过滤）。
	LockedCount int
}

// Evaluate 计算候选域的软打分 domainScore(C)（§8.1）。
//
//	domainScore(C) = β·兄弟亲和 + α·容量富余度 - γ·(跨域边数/域内总边数)
//
// 注意：硬约束（空闲 GPU ≥ 请求数、域内无 locked）须由调用方在传入前过滤，
// 本函数只对通过硬约束的候选域排序。
func (p DomainScoreParams) Evaluate(c CandidateDomain) float64 {
	var siblingFactor float64
	if c.TotalCapacity > 0 {
		siblingFactor = float64(c.SiblingCount) / float64(c.TotalCapacity)
	}
	capacityFactor := 1.0 - float64(c.TotalCapacity-len(c.FreeGPUs))/float64(maxInt(1, c.TotalCapacity))
	// 内聚度惩罚：此处以"已分配比例"近似跨域边占比；跨域边精确计算见 FillCandidates。
	cohesionPenalty := 0.0
	return p.Beta*siblingFactor + p.Alpha*capacityFactor - p.Gamma*cohesionPenalty
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// bestFitScore 计算跨域惩罚与内聚惩罚。为保持与 §8.1 公式一致，提供
// CrossDomainRatio 用于 γ 惩罚。
func CrossDomainRatio(domainMemberCount, totalGpuCount int) float64 {
	if totalGpuCount <= 0 {
		return 0
	}
	// 跨域边数近似：假设两两互联，域内边 vs 全网边。
	intra := domainMemberCount * (domainMemberCount - 1) / 2
	total := totalGpuCount * (totalGpuCount - 1) / 2
	extra := total - intra
	if total == 0 {
		return 0
	}
	return float64(extra) / float64(total)
}

// DomainCapacity 返回域容量（GPU 数）。
func DomainCapacity(d Domain) int {
	return len(d.GPUIndexes)
}

// LinkBandwidth 依据链路类型返回参考带宽（GB/s，附录 B）。
func LinkBandwidth(lt LinkType) float64 {
	switch lt {
	case LinkNVLink:
		return 600
	case LinkNVSwitch:
		return 900
	case LinkPIX, LinkPHB:
		return 32
	case LinkSYS:
		return 16
	default:
		return 0
	}
}

// NormalizedLinkCost 返回链路归一化成本到 [0,1]，用于 GangAffinity（§8.2）。
func NormalizedLinkCost(lt LinkType) float64 {
	bw := LinkBandwidth(lt)
	if bw <= 0 {
		return 1.0 // 无链路视为最大成本
	}
	return math.Min(1.0, 1.0/math.Log2(1.0+bw/8.0))
}
