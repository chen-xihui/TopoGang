// Package allocator 实现 GPU AllocationTracker（§7.3.3）。
//
// 调度器内存中的 GPU 分配账本，以调度器 Reserve/Unreserve 事件为唯一写入源（M1 修订）。
// 关键特性：
//   - epoch：任何 Reserve/Release 递增，用于 GangPrecheck 预检缓存失效（S2）。
//   - locked：Agent 观测到"物理占用超前于调度器视图"时对 GPU 设置的封锁标记（N2/T1 安全阀），
//     任何 SelectGPUs 先排除 locked GPU，防超卖。
//   - 管理域约束（S1/T1）：仅管理域内节点参与 GPU 级记账与选卡；域外仅按数量过滤。
package allocator

import (
	"sort"
	"sync"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// AllocationTracker 是调度器侧"每张 GPU 归哪个 Pod"的权威视图（§7.3.3）。
// 线程安全。
type AllocationTracker struct {
	mu sync.RWMutex

	// nodeName -> gpuID -> podUID
	allocations map[string]map[string]string
	// nodeName -> gpuID（节点内全部 GPU，来自 NodeGpuTopology）
	gpus map[string]map[string]bool
	// nodeName -> gpuID -> nvlinkDomain
	gpuDomains map[string]map[string]string
	// nodeName -> domainID -> gpuID 集合（域分桶视图）
	domainBuckets map[string]map[string][]string
	// locked: nodeName -> gpuID（超卖安全阀，N2/T1；独立于 allocation，不篡改记账）
	locked map[string]map[string]bool
	// 心跳过期/拓扑不健康节点（T2）：完全停止新分配
	unhealthy map[string]bool

	// epoch 单调递增版本号（任何 Reserve/Release 递增，S2）。
	epoch uint64
}

// NewAllocationTracker 构造 AllocationTracker。
func NewAllocationTracker() *AllocationTracker {
	return &AllocationTracker{
		allocations:  map[string]map[string]string{},
		gpus:         map[string]map[string]bool{},
		gpuDomains:   map[string]map[string]string{},
		domainBuckets: map[string]map[string][]string{},
		locked:       map[string]map[string]bool{},
		unhealthy:    map[string]bool{},
	}
}

// NodeGPUInfo 描述节点的拓扑信息（用于初始化 tracker）。
type NodeGPUInfo struct {
	// NodeName 节点名。
	NodeName string
	// GPUs gpuID 列表。
	GPUs []string
	// GpuDomain gpuID -> nvlinkDomain。
	GpuDomain map[string]string
	// InManagedDomain 是否管理域内节点（S1/T1）。
	InManagedDomain bool
}

// AddNode 注册节点的 GPU 拓扑（来自 NodeGpuTopology）。
// 管理域外节点不参与记账（仅数量过滤，§7.3.3 S1）。
func (t *AllocationTracker) AddNode(info NodeGPUInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gpus[info.NodeName] = map[string]bool{}
	t.gpuDomains[info.NodeName] = map[string]string{}
	t.domainBuckets[info.NodeName] = map[string][]string{}
	for _, gpuID := range info.GPUs {
		t.gpus[info.NodeName][gpuID] = true
		domain := info.GpuDomain[gpuID]
		t.gpuDomains[info.NodeName][gpuID] = domain
		t.domainBuckets[info.NodeName][domain] = append(t.domainBuckets[info.NodeName][domain], gpuID)
	}
	// 排序保证确定性
	for d := range t.domainBuckets[info.NodeName] {
		sort.Strings(t.domainBuckets[info.NodeName][d])
	}
}

// RemoveNode 移除节点的 GPU 视图（节点删除）。
func (t *AllocationTracker) RemoveNode(nodeName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.allocations, nodeName)
	delete(t.gpus, nodeName)
	delete(t.gpuDomains, nodeName)
	delete(t.domainBuckets, nodeName)
	delete(t.locked, nodeName)
	delete(t.unhealthy, nodeName)
}

// Allocate 记账：gpuID 分配给 podUID。返回 epoch 是否递增。
// 返回错误如果 GPU 已分配或已被 locked。
func (t *AllocationTracker) Allocate(node, gpuID, podUID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.locked[node][gpuID] {
		return &LockedError{Node: node, GPU: gpuID}
	}
	if _, ok := t.gpus[node]; !ok {
		return &NodeError{Node: node}
	}
	if t.allocations[node] == nil {
		t.allocations[node] = map[string]string{}
	}
	if existing, ok := t.allocations[node][gpuID]; ok && existing != podUID {
		return &AlreadyAllocatedError{Node: node, GPU: gpuID, Owner: existing}
	}
	t.allocations[node][gpuID] = podUID
	t.epoch++
	return nil
}

// Release 释放 gpuID（Pod 删除/Unreserve）。返回是否实际释放。
func (t *AllocationTracker) Release(node, gpuID, podUID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.allocations[node] == nil {
		return false
	}
	owner, ok := t.allocations[node][gpuID]
	if !ok {
		return false
	}
	// 仅允许属主释放（防止串抢）
	if podUID != "" && owner != podUID {
		return false
	}
	delete(t.allocations[node], gpuID)
	t.epoch++
	return true
}

// IsFree 判断 gpuID 是否空闲（未分配且未 locked）。
func (t *AllocationTracker) IsFree(node, gpuID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.locked[node][gpuID] {
		return false
	}
	_, allocated := t.allocations[node][gpuID]
	return !allocated
}

// FreeGPUs 返回节点某域的空闲 GPU（§7.3.3）。
func (t *AllocationTracker) FreeGPUs(node, domain string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var free []string
	for _, gpuID := range t.domainBuckets[node][domain] {
		if t.isFreeLocked(node, gpuID) {
			free = append(free, gpuID)
		}
	}
	sort.Strings(free)
	return free
}

// FreeCount 返回节点某域空闲 GPU 数。
func (t *AllocationTracker) FreeCount(node, domain string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := 0
	for _, gpuID := range t.domainBuckets[node][domain] {
		if t.isFreeLocked(node, gpuID) {
			n++
		}
	}
	return n
}

// LockGPU 将 gpuID 标记为 locked（超卖安全阀，N2/T1）。
// 不写入分配表（不篡改记账），仅独立封锁列表。
func (t *AllocationTracker) LockGPU(node, gpuID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.locked[node] == nil {
		t.locked[node] = map[string]bool{}
	}
	t.locked[node][gpuID] = true
	// locked 不递增 epoch（不改变可用 GPU 数量的记账，仅封锁；避免预检缓存误失效）
}

// UnlockGPU 解锁 gpuID（占用消失后，§7.3.3）。
func (t *AllocationTracker) UnlockGPU(node, gpuID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.locked[node] != nil {
		delete(t.locked[node], gpuID)
	}
}

// IsLocked 判断 gpuID 是否 locked。
func (t *AllocationTracker) IsLocked(node, gpuID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.locked[node][gpuID]
}

// LockedGPUs 返回节点 locked GPU 集合。
func (t *AllocationTracker) LockedGPUs(node string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []string
	for gpuID := range t.locked[node] {
		out = append(out, gpuID)
	}
	sort.Strings(out)
	return out
}

// MarkUnhealthy 标记节点心跳过期（T2）：完全停止新分配。
func (t *AllocationTracker) MarkUnhealthy(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.unhealthy[node] = true
}

// MarkHealthy 恢复节点健康。
func (t *AllocationTracker) MarkHealthy(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.unhealthy, node)
}

// IsUnhealthy 判断节点是否心跳过期。
func (t *AllocationTracker) IsUnhealthy(node string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.unhealthy[node]
}

// Epoch 返回当前 epoch（S2 预检缓存失效）。
func (t *AllocationTracker) Epoch() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.epoch
}

// GPUAllocation 表示一条分配记录（对账用）。
type GPUAllocation struct {
	Node string
	GPU  string
	Pod  string
}

// Allocations 返回当前全部分配（对账/重建）。
func (t *AllocationTracker) Allocations() []GPUAllocation {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []GPUAllocation
	for node, m := range t.allocations {
		for gpuID, podUID := range m {
			out = append(out, GPUAllocation{Node: node, GPU: gpuID, Pod: podUID})
		}
	}
	return out
}

// isFreeLocked 需在持锁下调用。
func (t *AllocationTracker) isFreeLocked(node, gpuID string) bool {
	if t.locked[node][gpuID] {
		return false
	}
	_, allocated := t.allocations[node][gpuID]
	return !allocated
}

// SelectGPUs 按 best-fit 决策选择 count 张 GPU（§8.1 M2 共享决策，§8.3）。
// domains 以 (domainID, 域内空闲 gpuID[]) 传入，gpuID 为物理 UUID。
// 硬约束过滤：排除容量不足域与含 locked GPU 的域；软打分复用 topo.BestFitDomain。
// 兄弟信息 siblingIDs（gpuID->bool）用于域内亲和打分。
// 返回选中的 GPU 列表；若无法凑足返回错误。
func (t *AllocationTracker) SelectGPUs(node string, count int, domains []topo.Domain, siblingIDs map[string]bool, params topo.DomainScoreParams) ([]string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.unhealthy[node] {
		return nil, &NodeUnhealthyError{Node: node}
	}

	// 构造候选域（硬约束：排除含 locked 域、空闲为空的域）
	candidates := make([]topo.FillCandidate, 0, len(domains))
	for _, d := range domains {
		// 硬约束：域内含 locked GPU 则排除整个域（§8.1 N5）
		if t.domainHasLocked(node, d.ID) {
			continue
		}
		domainFree := t.freeInDomain(node, d.ID)
		if len(domainFree) == 0 {
			continue
		}
		siblingCount := 0
		for _, gpuID := range domainFree {
			if siblingIDs[gpuID] {
				siblingCount++
			}
		}
		// FillCandidate.FreeGPUs 语义为"空闲 GPU 数"（best-fit 打分用容量富余度）。
		candidates = append(candidates, topo.FillCandidate{
			Domain:           d,
			FreeGPUs:         intListOfLen(len(domainFree)),
			SiblingCount:     siblingCount,
			TotalCapacity:    len(d.GPUIndexes),
			CrossDomainRatio: topo.CrossDomainRatio(len(d.GPUIndexes), t.nodeGPUTotal(node)),
		})
	}

	best := topo.BestFitDomain(candidates, params)
	if best == nil {
		return nil, &NoFitError{Node: node}
	}
	// 从最优域取 GPU（不足则跨域组合退化，§8.3 步骤 3）
	selected := t.takeFromDomain(node, best.Domain.ID, count)
	if len(selected) < count {
		return nil, &NoFitError{Node: node}
	}
	return selected, nil
}

// freeInDomain 返回域内空闲 gpuID（含 locked 过滤）。需持锁。
func (t *AllocationTracker) freeInDomain(node, domain string) []string {
	var free []string
	for _, gpuID := range t.domainBuckets[node][domain] {
		if t.isFreeLocked(node, gpuID) {
			free = append(free, gpuID)
		}
	}
	return free
}

// domainHasLocked 判断域内是否有 locked GPU（硬约束，§8.1 N5）。需持锁。
func (t *AllocationTracker) domainHasLocked(node, domain string) bool {
	for _, gpuID := range t.domainBuckets[node][domain] {
		if t.locked[node][gpuID] {
			return true
		}
	}
	return false
}

// nodeGPUTotal 返回节点 GPU 总数。需持锁。
func (t *AllocationTracker) nodeGPUTotal(node string) int {
	return len(t.gpus[node])
}

// takeFromDomain 从最优域取 count 张 GPU；域内不足时跨域补足（贪心：按域空闲数从多到少）。
// 需持锁。
func (t *AllocationTracker) takeFromDomain(node, bestDomain string, count int) []string {
	var selected []string
	// 先取最优域
	for _, gpuID := range t.domainBuckets[node][bestDomain] {
		if len(selected) >= count {
			break
		}
		if t.isFreeLocked(node, gpuID) {
			selected = append(selected, gpuID)
		}
	}
	// 不足则跨域补足
	if len(selected) < count {
		// 按域空闲数从多到少排序（确定性）
		domains := sortedDomainsByFree(t, node)
		for _, d := range domains {
			if d == bestDomain {
				continue
			}
			if t.domainHasLocked(node, d) {
				continue
			}
			for _, gpuID := range t.domainBuckets[node][d] {
				if len(selected) >= count {
					break
				}
				if t.isFreeLocked(node, gpuID) {
					selected = append(selected, gpuID)
				}
			}
		}
	}
	return selected
}

// sortedDomainsByFree 返回节点域 ID，按空闲 GPU 数从多到少排序。需持锁。
func sortedDomainsByFree(t *AllocationTracker, node string) []string {
	domains := make([]string, 0, len(t.domainBuckets[node]))
	for d := range t.domainBuckets[node] {
		domains = append(domains, d)
	}
	sort.Slice(domains, func(i, j int) bool {
		fi := len(t.freeInDomain(node, domains[i]))
		fj := len(t.freeInDomain(node, domains[j]))
		if fi != fj {
			return fi > fj
		}
		return domains[i] < domains[j]
	})
	return domains
}

// intListOfLen 构造长度为 n 的占位 int 切片（best-fit 打分仅用 len 表示空闲数）。
func intListOfLen(n int) []int {
	return make([]int, n)
}

// ---------- 错误类型 ----------

// LockedError GPU 被封锁。
type LockedError struct{ Node, GPU string }

func (e *LockedError) Error() string { return "gpu " + e.GPU + " on " + e.Node + " is locked" }

// NodeError 节点未知。
type NodeError struct{ Node string }

func (e *NodeError) Error() string { return "unknown node " + e.Node }

// AlreadyAllocatedError GPU 已被占用。
type AlreadyAllocatedError struct{ Node, GPU, Owner string }

func (e *AlreadyAllocatedError) Error() string {
	return "gpu " + e.GPU + " on " + e.Node + " already allocated to " + e.Owner
}

// NodeUnhealthyError 节点心跳过期。
type NodeUnhealthyError struct{ Node string }

func (e *NodeUnhealthyError) Error() string { return "node " + e.Node + " is unhealthy" }

// NoFitError 无满足放置。
type NoFitError struct{ Node string }

func (e *NoFitError) Error() string { return "no fit for GPU request on " + e.Node }
