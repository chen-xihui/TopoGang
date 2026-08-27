package allocator

import "testing"

func setupReconcile() *AllocationTracker {
	tr := NewAllocationTracker()
	tr.AddNode(NodeGPUInfo{
		NodeName:       "node-a",
		GPUs:           []string{"GPU0", "GPU1", "GPU2", "GPU3"},
		GpuDomain:      map[string]string{"GPU0": "d1", "GPU1": "d1", "GPU2": "d1", "GPU3": "d1"},
		InManagedDomain: true,
	})
	return tr
}

// N2 类型①：记账超前（tracker 有记录、agent 观测空闲）-> 核对清理。
func TestReconcile_TrackerAheadCleanup(t *testing.T) {
	tr := setupReconcile()
	_ = tr.Allocate("node-a", "GPU0", "pod-1")

	// agent 观测 GPU0 空闲（Pod 已删除但释放事件未送达）
	actions := ReconcileDrifts(tr, []AgentAllocation{
		{Node: "node-a", GPU: "GPU0", OccupiedByPod: ""},
	})
	if len(actions) != 1 || actions[0].Type != DriftTypeCleanup {
		t.Fatalf("expected 1 cleanup action, got %+v", actions)
	}
	// 记账已清理
	if tr.IsAllocated("node-a", "GPU0") {
		t.Fatal("tracker allocation should be cleaned up")
	}
}

// N2 类型②：物理占用超前（agent 观测占用、tracker 空闲）-> locked 安全阀。
func TestReconcile_AgentAheadLock(t *testing.T) {
	tr := setupReconcile()
	// tracker 无 GPU1 记录，但 agent 观测 GPU1 被 Pod 占用（物理真相）
	actions := ReconcileDrifts(tr, []AgentAllocation{
		{Node: "node-a", GPU: "GPU1", OccupiedByPod: "ghost-pod"},
	})
	if len(actions) != 1 || actions[0].Type != DriftTypeLock {
		t.Fatalf("expected 1 lock action, got %+v", actions)
	}
	if !tr.IsLocked("node-a", "GPU1") {
		t.Fatal("GPU1 should be locked (safe valve, N2)")
	}
	// locked 后不可分配
	err := tr.Allocate("node-a", "GPU1", "new-pod")
	var le *LockedError
	if err == nil || !asLockedError(err, &le) {
		t.Fatalf("expected LockedError for locked GPU, got %v", err)
	}
}

// 正常一致：无漂移。
func TestReconcile_NoDrift(t *testing.T) {
	tr := setupReconcile()
	_ = tr.Allocate("node-a", "GPU0", "pod-1")
	// agent 观测一致：GPU0 归 pod-1，GPU1 空闲
	actions := ReconcileDrifts(tr, []AgentAllocation{
		{Node: "node-a", GPU: "GPU0", OccupiedByPod: "pod-1"},
		{Node: "node-a", GPU: "GPU1", OccupiedByPod: ""},
	})
	if len(actions) != 0 {
		t.Fatalf("expected no drift actions, got %+v", actions)
	}
}

// locked 不影响记账（§7.3.3：不篡改记账）。
func TestReconcile_LockDoesNotRecord(t *testing.T) {
	tr := setupReconcile()
	_ = tr.Allocate("node-a", "GPU0", "pod-1")
	actions := ReconcileDrifts(tr, []AgentAllocation{
		{Node: "node-a", GPU: "GPU0", OccupiedByPod: "ghost"}, // tracker 认为 pod-1，agent 观测 ghost
	})
	// GPU0 tracker 已分配（pod-1），agent 观测 ghost -> 类型②（agent 观测的 owner 不同但 tracker 认为已分配，不触发 lock）
	// 仅当 tracker 完全空闲时 lock。此处 tracker 已分配，agent 观测占用但 tracker 也占用 -> 无 lock
	_ = actions
	if tr.IsLocked("node-a", "GPU0") {
		t.Fatal("GPU0 already tracked by scheduler, should not be locked (owner is scheduler-managed)")
	}
}

func asLockedError(err error, out **LockedError) bool {
	le, ok := err.(*LockedError)
	if ok {
		*out = le
	}
	return ok
}
