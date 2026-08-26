package gang

import "time"

// Decision 是 Permit 的返回决定。
type Decision int

const (
	// DecisionSuccess 放行成员。
	DecisionSuccess Decision = iota
	// DecisionWait 挂起等待组级批准。
	DecisionWait
	// DecisionReject 拒绝成员。
	DecisionReject
)

// PermitResult 封装 Permit 决定与附加信息。
type PermitResult struct {
	// Decision 决定。
	Decision Decision
	// Reason 拒绝/等待原因。
	Reason string
	// ReleaseAll 本次调用是否触发了整组原子放行（供日志/指标）。
	ReleaseAll bool
	// ReleasedGeneration 触发放行时的 generation（CAS 递增）。
	ReleasedGeneration int64
}

// PermitInput 是 Permit 判定的输入。
type PermitInput struct {
	// PodID 成员 Pod 唯一 ID。
	PodID string
	// HasGroupAnnotation 该 Pod 是否携带 group-name annotation（区分孤儿，T5）。
	HasGroupAnnotation bool
	// Group 该 Pod 所属的组状态；nil 表示组不存在。
	Group *GroupState
	// NewWaitingPod 由适配层注入的当前成员句柄创建函数（仅在需要等待时调用）。
	NewWaitingPod func(podID string) WaitingPod
	// OnReleaseAll 整组放行回调（供适配层写 released-generation annotation）。
	OnReleaseAll func(generation int64)
	// BumpGeneration 返回下一次 generation（CAS 递增）。
	BumpGeneration func() int64
}

// Permit 实现 All-or-Nothing 原子放行（§7.3.1 伪代码）。
//
// 逻辑分支：
//  1. 无组单 Pod（无 annotation 且组 nil） -> Success（s2 修订）。
//  2. 孤儿 Pod（有 annotation 但组 nil）    -> Wait 限时重试（T5）。
//  3. 组已放行且 Running（补位成员容错路径）-> Success（T4/t7 phase 防御）。
//  4. 组 Failed                             -> Reject（S3）。
//  5. 未凑齐：AddWaitingPod 挂起，凑齐 minMember 即原子放行（R1）。
func Permit(in PermitInput) PermitResult {
	// 分支 1 & 2：组不存在
	if in.Group == nil {
		if !in.HasGroupAnnotation {
			// 无组单 Pod：直接放行，不进 waiting（s2）
			return PermitResult{Decision: DecisionSuccess}
		}
		// 孤儿 Pod：返回 Wait 限时重试，超阈值由孤儿定时器 Reject（s4/T5）
		return PermitResult{Decision: DecisionWait, Reason: "orphan-pod-group-not-found"}
	}

	gs := in.Group

	// 分支 3：组已放行且 Running（补位成员容错路径；T4/t7 带 phase 防御）
	if gs.HasReleased() {
		return PermitResult{Decision: DecisionSuccess}
	}

	// 分支 4：组 Failed，直接拒绝（S3），避免无限重排循环
	if gs.IsFailed() {
		return PermitResult{Decision: DecisionReject, Reason: "group-failed"}
	}

	// 分支 5：先加入 waiting，再判断是否凑齐（R1：消除 off-by-one——
	// 第 minMember 个成员加入后 len(waiting)=minMember 恰好满足）。
	wp := in.NewWaitingPod(in.PodID)
	gs.Waiting = append(gs.Waiting, wp)
	gs.WaitingByPod[in.PodID] = wp
	gs.MemberOutcomes[in.PodID] = OutcomeWaiting
	// N1：成员完成调度决策（Permit 提交）后不再占用 batch 名额。
	// active -> waiting 转移，保证 minMember > batch 时组仍能凑齐。
	gs.ExitBatch()

	// T4/t7 防御：phase 非 Running 一律视为未放行，计数从 0 起算，
	// 不信任缓存中可能残留的旧批次 ScheduledByGroup（回退后缓存未刷新场景）。
	releasedCount := int32(0)
	if gs.IsRunning() {
		releasedCount = gs.ScheduledByGroup
	}
	if releasedCount+int32(len(gs.Waiting)) >= gs.Spec.MinMember {
		// 凑齐：整组原子放行
		gs.ScheduledByGroup = gs.Spec.MinMember
		gen := int64(1)
		if in.BumpGeneration != nil {
			gen = in.BumpGeneration()
		}
		gs.ReleasedGeneration = gen

		// 放行所有 WaitingPod（含当前成员）
		for _, w := range gs.Waiting {
			w.Allow()
		}
		gs.Waiting = nil
		gs.WaitingByPod = map[string]WaitingPod{}

		if in.OnReleaseAll != nil {
			in.OnReleaseAll(gen)
		}
		return PermitResult{
			Decision:           DecisionSuccess,
			ReleaseAll:         true,
			ReleasedGeneration: gen,
		}
	}

	// 尚未凑齐：启动组级超时定时器（R6：以首次进入等待为基准，后续不重置）
	if gs.timer == nil && !gs.timerDone {
		var t timerIface
		if newTimerFactory != nil {
			t = newTimerFactory(gs.Spec.ScheduleTimeout, func() {
				gs.timerDone = true
				gs.rejectAll("PodGroupTimeout")
			})
		} else {
			t = time.AfterFunc(gs.Spec.ScheduleTimeout, func() {
				gs.timerDone = true
				gs.rejectAll("PodGroupTimeout")
			})
		}
		gs.timer = t
	}
	return PermitResult{Decision: DecisionWait}
}

// rejectAll 拒绝组内全部 WaitingPod 并重置状态（超时/失败路径）。
func (s *GroupState) rejectAll(reason string) {
	for _, w := range s.Waiting {
		w.Reject(reason)
	}
	s.Waiting = nil
	s.WaitingByPod = map[string]WaitingPod{}
	s.clearTimer()
	// 回退清零（S4）：组离开等待后必须重新凑齐
	s.ScheduledByGroup = 0
	s.PrecheckOK = false
}

// ReleaseWaiting 快速失败路径（N3）：任一成员确定不可调度时，整组 Reject 重排。
func ReleaseWaiting(gs *GroupState, reason string) {
	gs.rejectAll(reason)
}

// FakeTimer 测试辅助：允许注入假定时器替代 time.AfterFunc。
// 调用方须在测试中调用 SetFakeTimer 并负责触发回调。
type fakeTimer struct {
	fn     func()
	fired  bool
	cancel chan struct{}
}

// SetFakeTimer 替换定时器工厂（测试用）。传入 nil 恢复真实定时器。
func SetFakeTimer(enabled bool) {
	if enabled {
		newTimerFactory = func(d time.Duration, f func()) timerIface {
			t := &fakeTimer{fn: f, cancel: make(chan struct{})}
			return t
		}
		return
	}
	newTimerFactory = nil
}

type timerIface interface {
	Stop() bool
}

func (f *fakeTimer) Stop() bool {
	select {
	case <-f.cancel:
	default:
		close(f.cancel)
	}
	f.fired = true
	return true
}

// Fire 手动触发定时器回调（测试用）。
func (f *fakeTimer) Fire() {
	if f.fn != nil {
		f.fn()
	}
}

var newTimerFactory func(d time.Duration, f func()) timerIface
