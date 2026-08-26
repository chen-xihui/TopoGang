package state

import (
	"testing"
	"time"
)

func baseView() GroupView {
	return GroupView{
		Phase:      PhasePending,
		MinMember:  8,
		Now:        time.Now(),
		CreationTime: time.Now(),
	}
}

func TestPendingToPreScheduling_OnFirstMember(t *testing.T) {
	sm := New(Options{})
	v := baseView()
	v.Members.Total = 1
	d := sm.Observe(v)
	if d.NextPhase != PhasePreScheduling {
		t.Fatalf("expected PreScheduling, got %s (%s)", d.NextPhase, d.Reason)
	}
}

func TestReleaseClosedLoop_ToRunning(t *testing.T) {
	sm := New(Options{})
	v := baseView()
	v.Phase = PhaseScheduling
	v.ScheduledByGroup = 8 // 调度器已放行
	v.MinMember = 8
	d := sm.Observe(v)
	if d.NextPhase != PhaseRunning {
		t.Fatalf("expected Running on release closed-loop, got %s", d.NextPhase)
	}
}

func TestAllSucceeded_ToSucceeded(t *testing.T) {
	sm := New(Options{})
	v := baseView()
	v.Phase = PhaseRunning
	v.Members.Total = 8
	v.Members.Success = 8
	d := sm.Observe(v)
	if d.NextPhase != PhaseSucceeded {
		t.Fatalf("expected Succeeded, got %s", d.NextPhase)
	}
}

func TestScheduleTimeout_RollbackToPending(t *testing.T) {
	sm := New(Options{ScheduleTimeout: 30 * time.Second})
	v := baseView()
	v.Phase = PhaseScheduling
	v.CreationTime = time.Now().Add(-40 * time.Second) // 已超时
	d := sm.Observe(v)
	if d.NextPhase != PhasePending {
		t.Fatalf("expected Pending on timeout, got %s", d.NextPhase)
	}
	if !d.UpdateScheduledByGroup || d.ScheduledByGroup != 0 {
		t.Fatalf("expected scheduledByGroup cleared on timeout (S4), got %+v", d)
	}
}

func TestScheduleTimeout_NotExceeded(t *testing.T) {
	sm := New(Options{ScheduleTimeout: 600 * time.Second})
	v := baseView()
	v.Phase = PhaseScheduling
	v.CreationTime = time.Now().Add(-10 * time.Second)
	d := sm.Observe(v)
	if d.NextPhase == PhasePending {
		t.Fatalf("expected not timed out, got Pending")
	}
}

// T3 回归：Failed 终态存在但仍在观察窗口内（Job 正在重试）不判 Failed。
func TestFailure_WithinWindow_NoFalsePositive(t *testing.T) {
	sm := New(Options{FailureObservationWindow: 60 * time.Second})
	v := baseView()
	v.Phase = PhaseRunning
	v.Members.Total = 8
	v.Members.Failed = 1
	v.Members.Running = 7
	ls := time.Now()
	v.LastScheduleTime = &ls // 刚放行，窗口内
	d := sm.Observe(v)
	if d.NextPhase == PhaseFailed {
		t.Fatalf("expected NOT Failed within observation window (Job retry), got %s", d.NextPhase)
	}
}

// T3 正向：Failed 终态持续超过观察窗口且无新成员 -> Failed。
func TestFailure_ExceededWindow_Failed(t *testing.T) {
	sm := New(Options{FailureObservationWindow: 60 * time.Second})
	v := baseView()
	v.Phase = PhaseRunning
	v.Members.Total = 8
	v.Members.Failed = 1
	v.Members.Running = 7
	ls := time.Now().Add(-120 * time.Second) // 超窗口
	v.LastScheduleTime = &ls
	d := sm.Observe(v)
	if d.NextPhase != PhaseFailed {
		t.Fatalf("expected Failed after window exceeded, got %s", d.NextPhase)
	}
}

// t3 回归：Failed 后出现同组新成员 -> 自动重置 Pending。
func TestFailed_NewMember_ResetToPending(t *testing.T) {
	sm := New(Options{})
	v := baseView()
	v.Phase = PhaseFailed
	v.Members.Pending = 1 // 新成员到来（重建 Job）
	d := sm.Observe(v)
	if d.NextPhase != PhasePending {
		t.Fatalf("expected Pending on new member after failed, got %s", d.NextPhase)
	}
	if d.Action != ActionReset {
		t.Fatal("expected reset action on failed->pending")
	}
}

func TestShouldResetAfterRollback(t *testing.T) {
	if !ShouldResetAfterRollback(PhaseRunning, PhasePending) {
		t.Fatal("Running->Pending should reset")
	}
	if !ShouldResetAfterRollback(PhaseRunning, PhaseFailed) {
		t.Fatal("Running->Failed should reset")
	}
	if ShouldResetAfterRollback(PhaseRunning, PhaseSucceeded) {
		t.Fatal("Running->Succeeded should NOT reset")
	}
	if ShouldResetAfterRollback(PhasePending, PhaseRunning) {
		t.Fatal("Pending->Running should NOT reset")
	}
}

func TestAllTerminalWithFailure_Failed(t *testing.T) {
	sm := New(Options{})
	v := baseView()
	v.Phase = PhaseRunning
	v.Members.Total = 8
	v.Members.Success = 7
	v.Members.Failed = 1
	d := sm.Observe(v)
	if d.NextPhase != PhaseFailed {
		t.Fatalf("expected Failed when all terminal with a failure, got %s", d.NextPhase)
	}
}
