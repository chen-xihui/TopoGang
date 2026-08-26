package gang

import "testing"

func node(name string, free int, domains map[string]int, healthy bool) PrecheckNode {
	if healthy && domains == nil {
		// 未指定域时按单域默认（全部空闲归入一个域）
		domains = map[string]int{"d1": free}
	}
	return PrecheckNode{Name: name, FreeGPUs: free, Domains: domains, Healthy: healthy}
}

// 用例 1（§10.3）：minMember=8，集群仅 6 卡 -> 整组无法满足。
func TestGangPrecheck_NotEnough(t *testing.T) {
	cache := NewPrecheckCache()
	sim := NewGreedySimulator()
	nodes := []PrecheckNode{
		node("n1", 6, map[string]int{"d1": 6}, true),
	}
	// 8 成员各 1 卡，但只有 6 卡
	ok := GangPrecheck(cache, "g1", sim, nodes, 1, 8, 1, 1)
	if ok {
		t.Fatal("expected precheck to fail (only 6 GPUs for 8 members)")
	}
}

// 补卡后整组可满足。
func TestGangPrecheck_Enough(t *testing.T) {
	cache := NewPrecheckCache()
	sim := NewGreedySimulator()
	nodes := []PrecheckNode{
		node("n1", 8, map[string]int{"d1": 8}, true),
	}
	ok := GangPrecheck(cache, "g1", sim, nodes, 1, 8, 1, 1)
	if !ok {
		t.Fatal("expected precheck to pass (8 GPUs for 8 members)")
	}
}

// R2 作用域：已放行组（Running）补位成员应跳过预检（由调用方决定，此处测模拟本身）。
func TestGreedySimulator_DomainAware(t *testing.T) {
	// 每成员请求 4 卡，节点1 的域1 只有 2 卡、域2 有 4 卡 -> 域2 可放 1 个成员
	sim := NewGreedySimulator()
	nodes := []PrecheckNode{
		node("n1", 6, map[string]int{"d1": 2, "d2": 4}, true),
	}
	if !sim.Simulate(nodes, 4, 1) {
		t.Fatal("expected 1 member of 4 GPUs to fit in domain d2")
	}
}

// S2 缓存复用：快照未变时同 key 命中，不重复模拟。
func TestPrecheckCache_ReuseOnSameKey(t *testing.T) {
	cache := NewPrecheckCache()
	sim := NewGreedySimulator()
	nodes := []PrecheckNode{node("n1", 8, map[string]int{"d1": 8}, true)}

	ok1 := GangPrecheck(cache, "g1", sim, nodes, 1, 8, 1, 1)
	ok2 := GangPrecheck(cache, "g1", sim, nodes, 1, 8, 1, 1)
	if ok1 != ok2 {
		t.Fatal("same key should give same result")
	}
	// 命中缓存
	if _, hit := cache.Get("g1", PrecheckCacheKey{TopoGeneration: 1, AllocationEpoch: 1, UnscheduledCount: 8, GpuCountPerMember: 1}); !hit {
		t.Fatal("expected cache hit on same key")
	}
}

// S2 epoch 变化 -> 缓存失效（key 不同），结果重新计算。
func TestPrecheckCache_EpochInvalidation(t *testing.T) {
	cache := NewPrecheckCache()
	sim := NewGreedySimulator()

	// epoch=1：8 卡满足 8 成员
	nodes := []PrecheckNode{node("n1", 8, map[string]int{"d1": 8}, true)}
	GangPrecheck(cache, "g1", sim, nodes, 1, 8, 1, 1)

	// 其他组 Reserve 后 epoch 递增到 2，节点只剩 6 卡
	nodes2 := []PrecheckNode{node("n1", 6, map[string]int{"d1": 6}, true)}
	ok := GangPrecheck(cache, "g1", sim, nodes2, 1, 8, 1, 2)
	if ok {
		t.Fatal("expected precheck to fail after epoch change (fewer GPUs)")
	}
	// 新 key 应命中
	if _, hit := cache.Get("g1", PrecheckCacheKey{TopoGeneration: 1, AllocationEpoch: 2, UnscheduledCount: 8, GpuCountPerMember: 1}); !hit {
		t.Fatal("expected cache hit on new epoch key")
	}
}

// T2：心跳过期节点不参与模拟。
func TestGangPrecheck_UnhealthyNodeExcluded(t *testing.T) {
	cache := NewPrecheckCache()
	sim := NewGreedySimulator()
	nodes := []PrecheckNode{
		node("n1", 8, map[string]int{"d1": 8}, false), // 心跳过期
	}
	ok := GangPrecheck(cache, "g1", sim, nodes, 1, 8, 1, 1)
	if ok {
		t.Fatal("expected precheck to fail when only node is unhealthy")
	}
}

func TestPrecheckCache_InvalidateGroup(t *testing.T) {
	cache := NewPrecheckCache()
	sim := NewGreedySimulator()
	nodes := []PrecheckNode{node("n1", 8, map[string]int{"d1": 8}, true)}
	GangPrecheck(cache, "g1", sim, nodes, 1, 8, 1, 1)

	cache.InvalidateGroup("g1")
	if _, hit := cache.Get("g1", PrecheckCacheKey{TopoGeneration: 1, AllocationEpoch: 1, UnscheduledCount: 8, GpuCountPerMember: 1}); hit {
		t.Fatal("expected cache cleared after invalidate")
	}
}
