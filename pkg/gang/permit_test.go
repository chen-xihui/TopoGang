package gang

import (
	"testing"
	"time"
)

// fakeWaitingPod 实现 WaitingPod 接口，记录 Allow/Reject 调用。
type fakeWaitingPod struct {
	id       string
	allowed  bool
	rejected bool
	reason   string
}

func (f *fakeWaitingPod) ID() string           { return f.id }
func (f *fakeWaitingPod) Allow()               { f.allowed = true }
func (f *fakeWaitingPod) Reject(reason string) { f.rejected = true; f.reason = reason }

// newPermitInput 构造 PermitInput，注入测试桩。
func newPermitInput(gs *GroupState, podID string, hasAnnotation bool, pods map[string]*fakeWaitingPod) PermitInput {
	genCounter := int64(0)
	return PermitInput{
		PodID:              podID,
		HasGroupAnnotation: hasAnnotation,
		Group:              gs,
		NewWaitingPod: func(id string) WaitingPod {
			p := &fakeWaitingPod{id: id}
			pods[id] = p
			return p
		},
		BumpGeneration: func() int64 {
			genCounter++
			return genCounter
		},
	}
}

func specOf(minMember int32) GroupSpec {
	return GroupSpec{MinMember: minMember, ScheduleTimeout: time.Hour}
}

// R1 回归：恰好 minMember 成员的组，第 N 个成员触发放行（off-by-one 修复）。
func TestPermit_ExactlyNTriggersRelease(t *testing.T) {
	SetFakeTimer(true)
	defer SetFakeTimer(false)

	gs := NewGroupState("ns", "g1", specOf(3))
	pods := map[string]*fakeWaitingPod{}

	// 第 1、2 个成员：Wait
	for _, id := range []string{"p0", "p1"} {
		r := Permit(newPermitInput(gs, id, true, pods))
		if r.Decision != DecisionWait {
			t.Fatalf("member %s: expected Wait, got %v", id, r.Decision)
		}
	}
	// 第 3 个成员：应触发放行（ReleaseAll），且是 Success
	r := Permit(newPermitInput(gs, "p2", true, pods))
	if r.Decision != DecisionSuccess {
		t.Fatalf("3rd member: expected Success (release), got %v", r.Decision)
	}
	if !r.ReleaseAll {
		t.Fatal("expected ReleaseAll on 3rd member")
	}
	// 全部 3 个成员都被放行
	for _, id := range []string{"p0", "p1", "p2"} {
		if !pods[id].allowed {
			t.Fatalf("member %s should be allowed", id)
		}
	}
	if gs.ScheduledByGroup != 3 {
		t.Fatalf("expected ScheduledByGroup=3, got %d", gs.ScheduledByGroup)
	}
	// waiting 已清空
	if len(gs.Waiting) != 0 {
		t.Fatalf("expected empty waiting after release, got %d", len(gs.Waiting))
	}
}

// N1 回归：minMember > maxSchedulingBatch 时组仍能凑齐放行（无死等）。
func TestPermit_MinMemberGreaterThanBatch(t *testing.T) {
	SetFakeTimer(true)
	defer SetFakeTimer(false)

	gs := NewGroupState("ns", "g1", GroupSpec{
		MinMember:         8,
		MaxSchedulingBatch: 4,
		ScheduleTimeout:    time.Hour,
	})
	pods := map[string]*fakeWaitingPod{}

	// 逐个到达 Permit；batch 限制由 EnterBatch 模拟，Permit 返回 Wait 后不占名额（N1）
	for i := 0; i < 8; i++ {
		id := "p" + string(rune('0'+i))
		r := Permit(newPermitInput(gs, id, true, pods))
		if i < 7 {
			if r.Decision != DecisionWait {
				t.Fatalf("member %s: expected Wait, got %v", id, r.Decision)
			}
		} else {
			if !r.ReleaseAll {
				t.Fatalf("8th member should trigger release")
			}
		}
	}
	if gs.ScheduledByGroup != 8 {
		t.Fatalf("expected ScheduledByGroup=8, got %d", gs.ScheduledByGroup)
	}
	for i := 0; i < 8; i++ {
		id := "p" + string(rune('0'+i))
		if !pods[id].allowed {
			t.Fatalf("member %s should be allowed", id)
		}
	}
}

// batch 并发限制（§8.4）：active 达到上限后 CanEnterBatch=false。
func TestEnterBatch_Limit(t *testing.T) {
	gs := NewGroupState("ns", "g1", GroupSpec{MinMember: 100, MaxSchedulingBatch: 4, ScheduleTimeout: time.Hour})
	for i := 0; i < 4; i++ {
		if !gs.EnterBatch() {
			t.Fatalf("expected enter batch succeed at %d", i)
		}
	}
	if gs.EnterBatch() {
		t.Fatal("expected 5th enter to fail (batch full)")
	}
	gs.ExitBatch()
	if !gs.EnterBatch() {
		t.Fatal("expected enter to succeed after exit")
	}
}

// S4 回归：组超时回退后 ScheduledByGroup 清零，重新排队必须重新凑齐。
func TestPermit_TimeoutRollbackClearsScheduledByGroup(t *testing.T) {
	SetFakeTimer(true)
	defer SetFakeTimer(false)

	gs := NewGroupState("ns", "g1", specOf(3))
	pods := map[string]*fakeWaitingPod{}
	// 第 3 个成员放行
	r := Permit(newPermitInput(gs, "p0", true, pods))
	_ = Permit(newPermitInput(gs, "p1", true, pods))
	r = Permit(newPermitInput(gs, "p2", true, pods))
	if !r.ReleaseAll {
		t.Fatal("expected release on 3rd member")
	}
	if gs.ScheduledByGroup != 3 {
		t.Fatalf("expected 3, got %d", gs.ScheduledByGroup)
	}
	gs.Phase = PhaseRunning

	// 模拟超时/失败回退（§9.1 S4）：组离开 Running，清零
	gs.ResetAfterRollback()
	gs.Phase = PhasePending
	if gs.ScheduledByGroup != 0 {
		t.Fatalf("expected ScheduledByGroup=0 after rollback, got %d", gs.ScheduledByGroup)
	}

	// 回退后新批次：必须重新凑齐 3 个才放行；第 1 个成员必须是 Wait（不被"已放行"分支误放）
	pods2 := map[string]*fakeWaitingPod{}
	r = Permit(newPermitInput(gs, "n0", true, pods2))
	if r.Decision != DecisionWait {
		t.Fatalf("after rollback first member should Wait, got %v", r.Decision)
	}
	if r.ReleaseAll {
		t.Fatal("after rollback should NOT release immediately")
	}
}

// T4/t7 回归：组已放行但 phase 非 Running（如回退后）时，"已放行"分支不命中。
func TestPermit_NotRunningDoesNotRelease(t *testing.T) {
	SetFakeTimer(true)
	defer SetFakeTimer(false)

	gs := NewGroupState("ns", "g1", specOf(3))
	gs.ScheduledByGroup = 3 // 缓存残留旧批次放行值（模拟调度器缓存未刷新，T4）
	gs.Phase = PhasePending // 但 phase 已回退

	pods := map[string]*fakeWaitingPod{}
	r := Permit(newPermitInput(gs, "p0", true, pods))
	if r.Decision != DecisionWait {
		t.Fatalf("expected Wait (phase not Running), got %v", r.Decision)
	}
	if r.ReleaseAll {
		t.Fatal("expected no release when phase not Running")
	}
}

// T4/t7 正向：组已放行且 Running 时，补位成员直接 Success（容错路径）。
func TestPermit_RunningReleasedAllowsNewMember(t *testing.T) {
	gs := NewGroupState("ns", "g1", specOf(3))
	gs.ScheduledByGroup = 3
	gs.Phase = PhaseRunning

	r := Permit(newPermitInput(gs, "new-member", true, nil))
	if r.Decision != DecisionSuccess {
		t.Fatalf("expected Success for Running group, got %v", r.Decision)
	}
	if r.ReleaseAll {
		t.Fatal("new member should not re-trigger release")
	}
}

// T5 回归：无组单 Pod 直接 Success；孤儿 Pod（有 annotation 但组 nil）返回 Wait。
func TestPermit_OrphanVsSinglePod(t *testing.T) {
	// 无组单 Pod：Success
	r := Permit(PermitInput{
		PodID:              "solo",
		HasGroupAnnotation: false,
		Group:              nil,
	})
	if r.Decision != DecisionSuccess {
		t.Fatalf("single pod: expected Success, got %v", r.Decision)
	}

	// 孤儿 Pod（有 annotation，组不存在）：Wait 限时重试（T5）
	r = Permit(PermitInput{
		PodID:              "orphan",
		HasGroupAnnotation: true,
		Group:              nil,
	})
	if r.Decision != DecisionWait {
		t.Fatalf("orphan: expected Wait, got %v", r.Decision)
	}
	if r.Reason == "" {
		t.Fatal("orphan should carry a reason")
	}
}

// S3 回归：组 Failed 时拒绝成员，避免无限重排循环。
func TestPermit_FailedGroupRejects(t *testing.T) {
	gs := NewGroupState("ns", "g1", specOf(3))
	gs.Phase = PhaseFailed
	r := Permit(newPermitInput(gs, "p0", true, nil))
	if r.Decision != DecisionReject {
		t.Fatalf("expected Reject for Failed group, got %v", r.Decision)
	}
}

// R6 回归：组级超时定时器以首次进入等待为基准（成员逐个到达不重置）。
func TestPermit_TimerStartsOnFirstWait(t *testing.T) {
	SetFakeTimer(true)
	defer SetFakeTimer(false)

	gs := NewGroupState("ns", "g1", specOf(3))
	pods := map[string]*fakeWaitingPod{}
	_ = Permit(newPermitInput(gs, "p0", true, pods))
	if gs.timer == nil {
		t.Fatal("expected timer started on first wait")
	}
	// 后续成员不替换定时器
	_ = Permit(newPermitInput(gs, "p1", true, pods))
	if gs.timer == nil {
		t.Fatal("expected timer preserved")
	}
}

// N3 快速失败：任一成员不可调度时整组 Reject 重排。
func TestReleaseWaiting_FastFail(t *testing.T) {
	SetFakeTimer(true)
	defer SetFakeTimer(false)

	gs := NewGroupState("ns", "g1", specOf(3))
	pods := map[string]*fakeWaitingPod{}
	_ = Permit(newPermitInput(gs, "p0", true, pods))
	_ = Permit(newPermitInput(gs, "p1", true, pods))

	ReleaseWaiting(gs, "GangMemberUnschedulable")
	// 全部 waiting 被 Reject
	if !pods["p0"].rejected || !pods["p1"].rejected {
		t.Fatal("expected all waiting pods rejected on fast-fail")
	}
	if len(gs.Waiting) != 0 {
		t.Fatalf("expected waiting cleared, got %d", len(gs.Waiting))
	}
	// 回退清零（S4）
	if gs.ScheduledByGroup != 0 {
		t.Fatalf("expected ScheduledByGroup=0 after fast-fail, got %d", gs.ScheduledByGroup)
	}
}

// 超时触发：定时器回调拒绝整组。
func TestPermit_TimeoutFiresRejectAll(t *testing.T) {
	SetFakeTimer(true)
	defer SetFakeTimer(false)

	gs := NewGroupState("ns", "g1", specOf(3))
	pods := map[string]*fakeWaitingPod{}
	_ = Permit(newPermitInput(gs, "p0", true, pods))
	_ = Permit(newPermitInput(gs, "p1", true, pods))

	// 触发定时器
	ft := gs.timer.(*fakeTimer)
	ft.Fire()

	if !pods["p0"].rejected || !pods["p1"].rejected {
		t.Fatal("expected timeout to reject all waiting pods")
	}
	// 超时拒绝后：waiting 清空、ScheduledByGroup 清零（S4）、定时器重置
	if len(gs.Waiting) != 0 {
		t.Fatalf("expected waiting cleared after timeout, got %d", len(gs.Waiting))
	}
	if gs.ScheduledByGroup != 0 {
		t.Fatalf("expected ScheduledByGroup=0 after timeout, got %d", gs.ScheduledByGroup)
	}
	if gs.timer != nil {
		t.Fatal("expected timer cleared after timeout")
	}
	// 新成员可启动新一轮定时器（R6：超时后重新排队）
	_ = Permit(newPermitInput(gs, "n0", true, pods))
	if gs.timer == nil {
		t.Fatal("expected fresh timer for new batch after timeout")
	}
}
