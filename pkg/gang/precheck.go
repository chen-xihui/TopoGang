package gang

import (
	"sort"
	"sync"
)

// PrecheckNode 是 GangPrecheck 模拟所需的节点视图。
type PrecheckNode struct {
	// Name 节点名。
	Name string
	// FreeGPUs 该节点空闲 GPU 总数。
	FreeGPUs int
	// Domains 该节点空闲 GPU 按 NVLink 域分布：domainID -> 空闲数。
	Domains map[string]int
	// Healthy 节点拓扑是否健康；false（心跳过期）不参与模拟（§7.1 T2）。
	Healthy bool
}

// PrecheckSimulator 对整组做可调度性模拟（§7.3.1 GangPrecheck）。
type PrecheckSimulator interface {
	// Simulate 返回是否能将 k 个（每个请求 gpuCount 卡）成员放置到节点快照中。
	Simulate(nodes []PrecheckNode, gpuCount, k int) bool
}

// GreedySimulator 贪心放置模拟器：按域内优先逐个放置（§7.3.1 步骤 3）。
type GreedySimulator struct{}

// NewGreedySimulator 构造贪心模拟器。
func NewGreedySimulator() *GreedySimulator { return &GreedySimulator{} }

// Simulate 贪心：对每个成员，优先放空闲 GPU ≥ gpuCount 的域，否则放跨域组合。
// 全部 k 个成员都能放下才返回 true。
func (s *GreedySimulator) Simulate(nodes []PrecheckNode, gpuCount, k int) bool {
	// 复制空闲计数（不修改原快照）
	copies := make([]PrecheckNode, 0, len(nodes))
	for _, n := range nodes {
		if !n.Healthy {
			continue // 心跳过期节点不参与（T2）
		}
		nc := n
		nc.Domains = make(map[string]int, len(n.Domains))
		for k2, v := range n.Domains {
			nc.Domains[k2] = v
		}
		copies = append(copies, nc)
	}

	for i := 0; i < k; i++ {
		if !placeOne(copies, gpuCount) {
			return false
		}
	}
	return true
}

// placeOne 尝试放置一个成员：优先从单个域取满，否则跨域组合。
func placeOne(nodes []PrecheckNode, gpuCount int) bool {
	// 优先：某节点某域空闲 ≥ gpuCount（域内装箱，§8.3）
	for i := range nodes {
		for domain, free := range nodes[i].Domains {
			if free >= gpuCount {
				nodes[i].Domains[domain] = free - gpuCount
				return true
			}
		}
	}
	// 其次：单节点内跨域凑足（PIX/跨域组合，退而求其次）
	for i := range nodes {
		nodeFree := 0
		for _, free := range nodes[i].Domains {
			nodeFree += free
		}
		if nodeFree >= gpuCount {
			consumeFromNode(nodes[i].Domains, gpuCount)
			return true
		}
	}
	// 最后：跨节点（Gang 允许跨节点）
	remaining := gpuCount
	for i := range nodes {
		for domain, free := range nodes[i].Domains {
			if remaining <= 0 {
				return true
			}
			take := minInt(free, remaining)
			nodes[i].Domains[domain] = free - take
			remaining -= take
		}
	}
	return remaining <= 0
}

func consumeFromNode(domains map[string]int, gpuCount int) {
	remaining := gpuCount
	// 确定性的消费顺序
	keys := make([]string, 0, len(domains))
	for k := range domains {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if remaining <= 0 {
			return
		}
		take := minInt(domains[k], remaining)
		domains[k] -= take
		remaining -= take
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// PrecheckCacheKey 是 GangPrecheck 结果缓存的 key（§7.3.1 S2 修订）：
// (节点拓扑 generation, AllocationTracker epoch, 组未调度成员数 k)。
type PrecheckCacheKey struct {
	// TopoGeneration 节点拓扑 generation 聚合值（简化为节点数校验）。
	TopoGeneration int64
	// AllocationEpoch AllocationTracker 的 epoch（任何 Reserve/Release 递增）。
	AllocationEpoch uint64
	// UnscheduledCount 组未调度成员数 k。
	UnscheduledCount int
	// GpuCountPerMember 每成员 GPU 请求数（t6：同构校验后缓存复用前提）。
	GpuCountPerMember int
}

// PrecheckCache 缓存组级预检结论（§7.3.1 R7/S2）。
type PrecheckCache struct {
	mu     sync.RWMutex
	groups map[string]map[PrecheckCacheKey]bool // groupKey -> (cacheKey -> result)
}

// NewPrecheckCache 构造预检缓存。
func NewPrecheckCache() *PrecheckCache {
	return &PrecheckCache{groups: map[string]map[PrecheckCacheKey]bool{}}
}

// Get 返回缓存结论及是否命中。
func (c *PrecheckCache) Get(groupKey string, key PrecheckCacheKey) (result bool, hit bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if g, ok := c.groups[groupKey]; ok {
		if v, ok2 := g[key]; ok2 {
			return v, true
		}
	}
	return false, false
}

// Set 写入缓存结论。
func (c *PrecheckCache) Set(groupKey string, key PrecheckCacheKey, result bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.groups[groupKey]; !ok {
		c.groups[groupKey] = map[PrecheckCacheKey]bool{}
	}
	c.groups[groupKey][key] = result
}

// InvalidateGroup 清空某组的全部缓存（组状态变化，如回退）。
func (c *PrecheckCache) InvalidateGroup(groupKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.groups, groupKey)
}

// GangPrecheck 是组级预检的编排入口（§7.3.1 步骤 0-5）。
//
// 作用域（R2）：仅对未放行组（PreScheduling/Scheduling）执行；已放行组补位跳过。
// 返回 true 表示整组可放置（可继续调度），false 表示整组拒绝（不进 Reserve）。
func GangPrecheck(
	cache *PrecheckCache,
	groupKey string,
	sim PrecheckSimulator,
	nodes []PrecheckNode,
	gpuCount, unscheduledCount int,
	topoGeneration int64,
	allocEpoch uint64,
) bool {
	key := PrecheckCacheKey{
		TopoGeneration:    topoGeneration,
		AllocationEpoch:   allocEpoch,
		UnscheduledCount:  unscheduledCount,
		GpuCountPerMember: gpuCount,
	}
	// S2/R7：快照未变直接复用缓存结论
	if result, hit := cache.Get(groupKey, key); hit {
		return result
	}

	result := sim.Simulate(nodes, gpuCount, unscheduledCount)
	cache.Set(groupKey, key, result)
	return result
}
