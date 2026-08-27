package allocator

import (
	"errors"
	"slices"
	"testing"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// setupTracker 构造一个 8 卡双域节点（域 d1: GPU0-3, 域 d2: GPU4-7）。
func setupTracker() *AllocationTracker {
	t := NewAllocationTracker()
	t.AddNode(NodeGPUInfo{
		NodeName: "node-a",
		GPUs:     []string{"GPU0", "GPU1", "GPU2", "GPU3", "GPU4", "GPU5", "GPU6", "GPU7"},
		GpuDomain: map[string]string{
			"GPU0": "d1", "GPU1": "d1", "GPU2": "d1", "GPU3": "d1",
			"GPU4": "d2", "GPU5": "d2", "GPU6": "d2", "GPU7": "d2",
		},
		InManagedDomain: true,
	})
	return t
}

func twoDomains() []topo.Domain {
	return []topo.Domain{
		{ID: "d1", GPUIndexes: []int{0, 1, 2, 3}},
		{ID: "d2", GPUIndexes: []int{4, 5, 6, 7}},
	}
}

func TestAllocate_Release(t *testing.T) {
	tr := setupTracker()
	if err := tr.Allocate("node-a", "GPU0", "pod-1"); err != nil {
		t.Fatalf("allocate failed: %v", err)
	}
	if tr.IsFree("node-a", "GPU0") {
		t.Fatal("GPU0 should be allocated")
	}
	if !tr.IsFree("node-a", "GPU1") {
		t.Fatal("GPU1 should be free")
	}
	// epoch 递增
	if tr.Epoch() != 1 {
		t.Fatalf("expected epoch 1, got %d", tr.Epoch())
	}
	if !tr.Release("node-a", "GPU0", "pod-1") {
		t.Fatal("release should succeed")
	}
	if !tr.IsFree("node-a", "GPU0") {
		t.Fatal("GPU0 should be free after release")
	}
	if tr.Epoch() != 2 {
		t.Fatalf("expected epoch 2, got %d", tr.Epoch())
	}
}

func TestAllocate_DoubleAllocationError(t *testing.T) {
	tr := setupTracker()
	_ = tr.Allocate("node-a", "GPU0", "pod-1")
	err := tr.Allocate("node-a", "GPU0", "pod-2")
	var aae *AlreadyAllocatedError
	if !errors.As(err, &aae) {
		t.Fatalf("expected AlreadyAllocatedError, got %v", err)
	}
}

func TestAllocate_LockedError(t *testing.T) {
	tr := setupTracker()
	tr.LockGPU("node-a", "GPU0")
	err := tr.Allocate("node-a", "GPU0", "pod-1")
	var le *LockedError
	if !errors.As(err, &le) {
		t.Fatalf("expected LockedError, got %v", err)
	}
}

// N2/T1 安全阀：locked GPU 不出现在 FreeGPUs，SelectGPUs 不选。
func TestLockedGPU_ExcludedFromSelection(t *testing.T) {
	tr := setupTracker()
	tr.LockGPU("node-a", "GPU0")
	// 整个域 d1 含 locked -> 硬约束排除 d1（§8.1 N5）
	selected, err := tr.SelectGPUs("node-a", 1, twoDomains(), nil, topo.DefaultDomainScoreParams())
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(selected) != 1 {
		t.Fatalf("expected 1 GPU, got %v", selected)
	}
	// 因 d1 含 locked 被排除，应选 d2 的 GPU
	for _, g := range selected {
		if g == "GPU0" {
			t.Fatal("locked GPU0 should not be selected")
		}
	}
}

// SelectGPUs 域内装箱：请求 4 卡，全部落单一域（§8.3）。
func TestSelectGPUs_SingleDomain(t *testing.T) {
	tr := setupTracker()
	selected, err := tr.SelectGPUs("node-a", 4, twoDomains(), nil, topo.DefaultDomainScoreParams())
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(selected) != 4 {
		t.Fatalf("expected 4 GPUs, got %v", selected)
	}
	// 全部落在同一域（d1 或 d2）
	domain := tr.gpuDomains["node-a"][selected[0]]
	for _, g := range selected {
		if tr.gpuDomains["node-a"][g] != domain {
			t.Fatalf("GPUs should be in same domain, got %v in different domains", selected)
		}
	}
}

// 兄弟亲和：两个域空闲数相同时，兄弟 Pod 所在域应被优先选择（§8.2，β 打破平局）。
func TestSelectGPUs_SiblingAffinity(t *testing.T) {
	tr := setupTracker()
	// 兄弟 Pod 占 d1 的 GPU0；另一非兄弟 Pod 占 d2 的 GPU4，使两域空闲数相同（各 3）
	_ = tr.Allocate("node-a", "GPU0", "sibling-pod")
	_ = tr.Allocate("node-a", "GPU4", "other-pod")
	// 请求 3 卡，两域都能满足（各 3 空闲），兄弟亲和（β）应使 d1 胜出
	selected, err := tr.SelectGPUs("node-a", 3, twoDomains(), map[string]bool{"GPU1": true}, topo.DefaultDomainScoreParams())
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("expected 3 GPUs, got %v", selected)
	}
	// 兄弟亲和应使 d1 胜出（GPU1-3）
	for _, g := range selected {
		if tr.gpuDomains["node-a"][g] != "d1" {
			t.Fatalf("expected all GPUs in sibling domain d1, got %v", selected)
		}
	}
}

// 心跳过期节点：SelectGPUs 拒绝（T2）。
func TestSelectGPUs_UnhealthyNodeRejected(t *testing.T) {
	tr := setupTracker()
	tr.MarkUnhealthy("node-a")
	_, err := tr.SelectGPUs("node-a", 1, twoDomains(), nil, topo.DefaultDomainScoreParams())
	var nue *NodeUnhealthyError
	if !errors.As(err, &nue) {
		t.Fatalf("expected NodeUnhealthyError, got %v", err)
	}
}

// 容量不足：请求超过节点 GPU 数 -> NoFit。
func TestSelectGPUs_NoFit(t *testing.T) {
	tr := setupTracker()
	_, err := tr.SelectGPUs("node-a", 9, twoDomains(), nil, topo.DefaultDomainScoreParams())
	var nfe *NoFitError
	if !errors.As(err, &nfe) {
		t.Fatalf("expected NoFitError, got %v", err)
	}
}

// epoch 单调递增：每次 Reserve/Release 递增（S2 预检缓存失效）。
func TestEpoch_Monotonic(t *testing.T) {
	tr := setupTracker()
	e0 := tr.Epoch()
	_ = tr.Allocate("node-a", "GPU0", "p")
	if tr.Epoch() <= e0 {
		t.Fatal("epoch should increase on allocate")
	}
	e1 := tr.Epoch()
	_ = tr.Release("node-a", "GPU0", "p")
	if tr.Epoch() <= e1 {
		t.Fatal("epoch should increase on release")
	}
}

// FreeGPUs 按域返回。
func TestFreeGPUs_ByDomain(t *testing.T) {
	tr := setupTracker()
	_ = tr.Allocate("node-a", "GPU0", "p")
	free := tr.FreeGPUs("node-a", "d1")
	if len(free) != 3 {
		t.Fatalf("expected 3 free in d1, got %v", free)
	}
	if contains(free, "GPU0") {
		t.Fatal("GPU0 allocated, should not be free")
	}
}

func TestLockUnlock(t *testing.T) {
	tr := setupTracker()
	tr.LockGPU("node-a", "GPU0")
	if !tr.IsLocked("node-a", "GPU0") {
		t.Fatal("GPU0 should be locked")
	}
	tr.UnlockGPU("node-a", "GPU0")
	if tr.IsLocked("node-a", "GPU0") {
		t.Fatal("GPU0 should be unlocked")
	}
}

func contains(s []string, v string) bool {
	return slices.Contains(s, v)
}
