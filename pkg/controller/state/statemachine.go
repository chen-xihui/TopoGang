// Package state 实现 PodGroup Controller 的纯状态机逻辑（§7.2 / §9.1）。
//
// 与 K8s / controller-runtime 解耦，可独立单测。输入为成员 Pod 观测与
// released-generation annotation 事件，输出为 phase / scheduledByGroup 决策。
package state

import "time"

// Phase 对应 PodGroup 的调度阶段（与 apis/scheduling 对齐）。
type Phase string

const (
	PhasePending       Phase = "Pending"
	PhasePreScheduling Phase = "PreScheduling"
	PhaseScheduling    Phase = "Scheduling"
	PhaseRunning       Phase = "Running"
	PhaseSucceeded     Phase = "Succeeded"
	PhaseFailed        Phase = "Failed"
	PhaseUnknown       Phase = "Unknown"
)

// MemberStatus 表示组内一个成员 Pod 的终态观测。
type MemberStatus int

const (
	// MemberPending 尚未调度/运行。
	MemberPending MemberStatus = iota
	// MemberRunning 运行中。
	MemberRunning
	// MemberSucceeded 成功退出。
	MemberSucceeded
	// MemberFailed 失败终态（restartPolicy: Never 且不重建）。
	MemberFailed
)

// GroupView 是状态机所需的组级输入快照。
type GroupView struct {
	// Phase 当前 phase。
	Phase Phase
	// MinMember 组内最少成功调度成员数。
	MinMember int32
	// ScheduledByGroup 当前已放行成员数（来自 released-generation 闭环）。
	ScheduledByGroup int32
	// Members 组内成员 Pod 状态统计。
	Members MemberCounts
	// ReleasedGeneration 当前 released-generation annotation 值。
	ReleasedGeneration int64
	// LastScheduleTime 最近一次放行时间（观察窗口 T 判定用）。
	LastScheduleTime *time.Time
	// CreationTime 组创建时间（排队时长计算用）。
	CreationTime time.Time
	// Now 当前时间（注入，测试用）。
	Now time.Time
}

// MemberCounts 统计组内成员 Pod 状态。
type MemberCounts struct {
	Total    int32
	Running  int32
	Success  int32
	Failed   int32
	Pending  int32
}

// Options 状态机参数。
type Options struct {
	// ScheduleTimeout 组排队超时（默认 600s，对应 scheduleTimeoutSeconds）。
	ScheduleTimeout time.Duration
	// FailureObservationWindow 失败终态观察窗口 T（默认 60s，§7.2 T3）。
	FailureObservationWindow time.Duration
}

// Decision 是状态机迁移的结果。
type Decision struct {
	// NextPhase 目标 phase。
	NextPhase Phase
	// SetScheduledByGroup 是否需更新 scheduledByGroup（回退清零 S4 / 放行闭环）。
	UpdateScheduledByGroup bool
	// ScheduledByGroup 目标值。
	ScheduledByGroup int32
	// Reason 迁移原因。
	Reason string
	// Action 需要 Controller 执行的副作用。
	Action Action
}

// Action 描述状态机建议的副作用。
type Action int

const (
	// ActionNone 无副作用。
	ActionNone Action = iota
	// ActionBumpReleasedGeneration 触发放行（写 released-generation annotation）。
	ActionBumpReleasedGeneration
	// ActionReset 回退清零（组离开 Running）。
	ActionReset
)

// defaultOptions 返回默认参数。
func defaultOptions() Options {
	return Options{
		ScheduleTimeout:          600 * time.Second,
		FailureObservationWindow: 60 * time.Second,
	}
}

// New 构造状态机。
func New(opts Options) *StateMachine {
	if opts.ScheduleTimeout <= 0 {
		opts.ScheduleTimeout = defaultOptions().ScheduleTimeout
	}
	if opts.FailureObservationWindow <= 0 {
		opts.FailureObservationWindow = defaultOptions().FailureObservationWindow
	}
	return &StateMachine{opts: opts}
}

// StateMachine 是 PodGroup 状态机的纯逻辑实现。
type StateMachine struct {
	opts Options
}

// Observe 依据组输入计算应迁移到的状态。
//
// 判定优先级：
//  1. 全部成员终态 -> Succeeded / Failed
//  2. 失败终态判定（T3 观察窗口）
//  3. 超时回退
//  4. released-generation 闭环放行
//  5. 正常阶段推进
func (m *StateMachine) Observe(v GroupView) Decision {
	now := v.Now
	if now.IsZero() {
		now = time.Now()
	}

	// 1. 组内全部成员终态（成功或失败）
	if v.Members.Total > 0 && v.Members.Total == v.Members.Success+v.Members.Failed {
		if v.Members.Failed > 0 {
			// 有 Failed 且无存活成员：直接 Failed（无观察窗口必要，无新成员可来）
			return Decision{NextPhase: PhaseFailed, Reason: "all-members-terminal-with-failure"}
		}
		return Decision{NextPhase: PhaseSucceeded, Reason: "all-members-succeeded"}
	}

	// 2. 失败终态判定（S3/T3）：存在 Failed 终态且持续 ≥ T 且期间无新成员创建
	if v.Members.Failed > 0 {
		if m.failureExceededWindow(v) {
			return Decision{NextPhase: PhaseFailed, Reason: "failed-pod-stable-beyond-window", Action: ActionReset}
		}
		// 仍在观察窗口内，可能是 Job backoff 重试中的瞬态失败，不判 Failed
	}

	// 3. 超时回退：PreScheduling/Scheduling 超过 scheduleTimeout
	if (v.Phase == PhasePreScheduling || v.Phase == PhaseScheduling) && !v.CreationTime.IsZero() {
		if now.Sub(v.CreationTime) > m.opts.ScheduleTimeout {
			return Decision{
				NextPhase:             PhasePending,
				Reason:                "schedule-timeout",
				Action:                ActionReset,
				UpdateScheduledByGroup: true,
				ScheduledByGroup:      0,
			}
		}
	}

	// 4. released-generation 闭环：调度器已放行（Running）
	if v.Phase == PhaseScheduling && v.ScheduledByGroup >= v.MinMember {
		return Decision{NextPhase: PhaseRunning, Reason: "released-generation-closed-loop"}
	}

	// 5. 正常推进
	switch v.Phase {
	case PhasePending:
		// 首个成员进入调度队列 -> PreScheduling
		if v.Members.Total > 0 {
			return Decision{NextPhase: PhasePreScheduling, Reason: "first-member-queued"}
		}
		return Decision{NextPhase: PhasePending, Reason: "no-member"}
	case PhasePreScheduling:
		// Permit 开始等待 -> Scheduling（由调度器状态或成员到达推进）
		return Decision{NextPhase: PhasePreScheduling, Reason: "awaiting-precheck"}
	case PhaseScheduling:
		return Decision{NextPhase: PhaseScheduling, Reason: "awaiting-members"}
	case PhaseRunning:
		return Decision{NextPhase: PhaseRunning, Reason: "running"}
	case PhaseFailed:
		// Failed -> Pending：出现同组新成员（t3，Pod 事件驱动）
		if v.Members.Pending > 0 || v.Members.Total > 0 {
			return Decision{NextPhase: PhasePending, Reason: "new-member-after-failed", Action: ActionReset}
		}
		return Decision{NextPhase: PhaseFailed, Reason: "failed"}
	case PhaseSucceeded:
		return Decision{NextPhase: PhaseSucceeded, Reason: "succeeded"}
	default:
		return Decision{NextPhase: v.Phase, Reason: "no-change"}
	}
}

// failureExceededWindow 判定 Failed 终态是否已持续超过观察窗口 T 且无新成员创建。
// 依据：存在 Failed Pod 且最后放行时间（或组创建时间）距 now 超过 T。
func (m *StateMachine) failureExceededWindow(v GroupView) bool {
	base := v.LastScheduleTime
	if base == nil {
		if v.CreationTime.IsZero() {
			return false // 无基准时间，暂不判定
		}
		bt := v.CreationTime
		base = &bt
	}
	return time.Since(*base) > m.opts.FailureObservationWindow
}

// ShouldResetAfterRollback 判断组是否需回退清零（S4）：离开 Running 到非 Running。
func ShouldResetAfterRollback(from, to Phase) bool {
	return from == PhaseRunning && to != PhaseRunning && to != PhaseSucceeded
}
