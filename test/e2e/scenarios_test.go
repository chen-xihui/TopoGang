package e2e

import (
	"testing"

	core "github.com/chenxihui/TopoGang/pkg/gang"
	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// 用例 1（§10.3）：Gang 原子性——minMember=8，集群仅 6 卡 -> 0 个成员被调度。
func TestScenario1_GangAtomicity_Insufficient(t *testing.T) {
	// 单节点 8 卡，但只有 6 卡可分配（模拟集群仅 6 空闲）
	cl := NewCluster(1)
	// 占 2 卡使剩余 6
	_ = cl.AllocateOnNode("node-0", "other", 2)

	g := NewGroupSim("ns", "g1", 8)
	rejected := 0
	for i := 0; i < 8; i++ {
		dec, ok := g.ScheduleMember(cl, "w"+int2str(i), 1)
		if !ok {
			rejected++
		}
		_ = dec
	}
	// 集群仅 6 卡，8 成员无法全部放置 -> 成员应被 GangPrecheck 拒绝（不进 Reserve）
	if len(g.AllocatedNode) != 0 {
		t.Fatalf("expected 0 allocated (GangPrecheck blocks), got %d", len(g.AllocatedNode))
	}
	_ = rejected
}

// 用例 1 正向：补卡后 8 成员全部放行。
func TestScenario1_GangAtomicity_Sufficient(t *testing.T) {
	cl := NewCluster(1) // 8 卡
	g := NewGroupSim("ns", "g1", 8)
	var lastDecision core.Decision
	for i := 0; i < 8; i++ {
		lastDecision, _ = g.ScheduleMember(cl, "w"+int2str(i), 1)
	}
	// 第 8 个成员触发原子放行
	if lastDecision != core.DecisionSuccess || g.State.ScheduledByGroup != 8 {
		t.Fatalf("expected all 8 released, decision=%v scheduled=%d", lastDecision, g.State.ScheduledByGroup)
	}
	// 全部 8 成员放行
	for i := 0; i < 8; i++ {
		if !g.Members["w"+int2str(i)] {
			t.Fatalf("member w%d should be allowed", i)
		}
	}
}

// 用例 8（N1）：minMember=8, maxSchedulingBatch=4 -> 无死等、全部放行。
func TestScenario8_BatchCounting(t *testing.T) {
	cl := NewCluster(1)
	g := NewGroupSim("ns", "g1", 8)
	g.State.Spec.MaxSchedulingBatch = 4 // 强制 batch=4 < minMember=8

	waitCount := 0
	releaseAt := -1
	for i := 0; i < 8; i++ {
		dec, ok := g.ScheduleMember(cl, "w"+int2str(i), 1)
		if !ok {
			waitCount++
			continue
		}
		if dec == core.DecisionWait {
			waitCount++
		} else if dec == core.DecisionSuccess && g.State.ScheduledByGroup == 8 {
			releaseAt = i
		}
	}
	if releaseAt != 7 {
		t.Fatalf("expected release at 8th member (7), got %d (deadlock or off-by-one)", releaseAt)
	}
	if g.State.ScheduledByGroup != 8 {
		t.Fatalf("expected all released, got %d", g.State.ScheduledByGroup)
	}
	_ = waitCount
}

// 用例 9（N3）：预检通过后某成员确定不可调度 -> 整组快速失败（不等超时）。
func TestScenario9_FastFail(t *testing.T) {
	cl := NewCluster(1)
	g := NewGroupSim("ns", "g1", 4)
	// 前 2 个成员进入 waiting
	g.ScheduleMember(cl, "w0", 1)
	g.ScheduleMember(cl, "w1", 1)
	// 模拟某成员在 Unreserve 时上报失败（Filter 无候选节点，R11）
	core.ReleaseWaiting(g.State, "GangMemberUnschedulable")
	// 整组 waiting 被拒绝、scheduledByGroup 清零
	if len(g.State.Waiting) != 0 {
		t.Fatalf("expected all waiting rejected on fast-fail, got %d", len(g.State.Waiting))
	}
	if g.State.ScheduledByGroup != 0 {
		t.Fatalf("expected scheduledByGroup=0 after fast-fail (S4), got %d", g.State.ScheduledByGroup)
	}
}

// 用例 10（N2）：超卖安全阀——agent 观测占用超前 -> GPU locked，不再分配。
func TestScenario10_OversellSafeValve(t *testing.T) {
	cl := NewCluster(1)
	// agent 观测 GPU-0-0 被未知 Pod 占用（tracker 空闲）
	cl.Tracker.LockGPU("node-0", "GPU-0-0")
	// 之后 SelectGPUs 不应选到该 GPU
	selected, err := cl.Tracker.SelectGPUs("node-0", 1, cl.Domains["node-0"], nil, topo.DefaultDomainScoreParams())
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if selected[0] == "GPU-0-0" {
		t.Fatal("locked GPU should not be selected (safe valve, N2)")
	}
}

// 用例 14（S1/T1）：心跳过期节点不参与调度。
func TestScenario14_UnhealthyNode(t *testing.T) {
	cl := NewCluster(1)
	cl.Tracker.MarkUnhealthy("node-0")
	nv := cl.Nodes["node-0"]
	nv.Healthy = false
	cl.Nodes["node-0"] = nv

	g := NewGroupSim("ns", "g1", 2)
	// 无健康节点可放置
	_, ok := g.ScheduleMember(cl, "w0", 1)
	if ok {
		t.Fatal("expected no scheduling on unhealthy-only cluster")
	}
}

// 用例 16（T4/t7）：组放行后回退清零，补位成员进入等待而非被直接放行。
func TestScenario16_RollbackClearsScheduledByGroup(t *testing.T) {
	cl := NewCluster(1)
	g := NewGroupSim("ns", "g1", 3)
	// 放行 3 成员
	for i := 0; i < 3; i++ {
		g.ScheduleMember(cl, "w"+int2str(i), 1)
	}
	if g.State.ScheduledByGroup != 3 {
		t.Fatalf("expected 3 released, got %d", g.State.ScheduledByGroup)
	}
	g.State.Phase = core.PhaseRunning

	// 回退清零（S4）
	g.State.ResetAfterRollback()
	g.State.Phase = core.PhasePending
	if g.State.ScheduledByGroup != 0 {
		t.Fatalf("expected scheduledByGroup=0 after rollback, got %d", g.State.ScheduledByGroup)
	}

	// 补位成员：应进入等待，不被"已放行"分支直接放行
	dec, _ := g.ScheduleMember(cl, "new1", 1)
	if dec != core.DecisionWait {
		t.Fatalf("expected new member to Wait after rollback, got %v (T4 defense)", dec)
	}
}

// 用例 17（T5）：孤儿 Pod（有 annotation 但组不存在）返回 Wait；无组单 Pod Success。
func TestScenario17_OrphanPod(t *testing.T) {
	// 孤儿：有 annotation、组 nil -> Wait
	r := core.Permit(core.PermitInput{
		PodID: "orphan", HasGroupAnnotation: true, Group: nil,
	})
	if r.Decision != core.DecisionWait {
		t.Fatalf("orphan should Wait (T5), got %v", r.Decision)
	}
	// 无组单 Pod -> Success
	r = core.Permit(core.PermitInput{
		PodID: "solo", HasGroupAnnotation: false, Group: nil,
	})
	if r.Decision != core.DecisionSuccess {
		t.Fatalf("single pod should Success, got %v", r.Decision)
	}
}

// 用例 2：超时回退。
func TestScenario2_TimeoutRollback(t *testing.T) {
	g := NewGroupSim("ns", "g1", 4)
	// 只调度 2 个成员，不凑齐
	cl := NewCluster(1)
	g.ScheduleMember(cl, "w0", 1)
	g.ScheduleMember(cl, "w1", 1)
	if g.State.ScheduledByGroup != 0 {
		t.Fatalf("expected not released yet, got %d", g.State.ScheduledByGroup)
	}
	// 状态机超时 -> 回退清零（由 GroupSim 的 timer 或状态机触发）
	g.State.ResetAfterRollback()
	if g.State.ScheduledByGroup != 0 {
		t.Fatalf("expected scheduledByGroup cleared on timeout (S4)")
	}
}
