// Package gang 实现 Gang 调度的核心状态机与 All-or-Nothing 语义。
//
// 该包与 scheduler-plugins 框架解耦，封装可独立单测的纯逻辑：
//   - GroupState：组级状态缓存（active/waiting 计数、放行判定、超时定时器）
//   - Permit 原子放行算法（§7.3.1，含 R1/S4/T4/T5 评审修订）
//
// 核心正确性约束（来自 docs/DESIGN.md 与 REVIEW）：
//   - R1：放行判断在 AddWaitingPod 之后（waiting 含当前成员），
//         第 minMember 个成员加入即触发放行（off-by-one 修复）。
//   - N1：Permit 返回 Wait 后不再占用 batch 名额（active/waiting 双状态）。
//   - S4：组离开 Running（超时/失败/重置）时 ScheduledByGroup 置 0，
//         重新排队必须重新凑齐 minMember 才放行。
//   - T4/t7：Permit"已放行"分支带 phase == Running 防御。
//   - T5：区分孤儿 Pod（Wait 限时 Reject）与无组单 Pod（Success）。
package gang

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
)

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

// GroupSpec 是 Gang 调度所需的组级只读输入。
type GroupSpec struct {
	// MinMember 组内最少成功调度成员数。
	MinMember int32
	// MaxSchedulingBatch 组内同时处于"调度 cycle 进行中"的最大成员数（§8.4）。
	MaxSchedulingBatch int32
	// ScheduleTimeout 从组首次进入等待到放行的最大时长。
	ScheduleTimeout time.Duration
	// CreationTimestamp 组创建时间（QueueSort 先到先服务，§8.4）。
	CreationTimestamp time.Time
}

// CreationTime 返回组创建时间。
func (s GroupSpec) CreationTime() time.Time {
	return s.CreationTimestamp
}

// WaitingPod 抽象 Permit 挂起等待的成员（适配层实现）。
type WaitingPod interface {
	// ID 返回成员 Pod 的唯一标识。
	ID() string
	// Allow 放行该成员。
	Allow()
	// Reject 拒绝该成员并记录原因。
	Reject(reason string)
}

// 调度结论，供快速失败路径（§7.3.1 N3）上报。
type MemberOutcome string

const (
	OutcomeWaiting   MemberOutcome = "waiting"
	OutcomeRejected  MemberOutcome = "rejected"
	OutcomeFailed    MemberOutcome = "failed"
	OutcomeScheduled MemberOutcome = "scheduled"
)

// GroupState 是组级状态缓存（§7.3.1）。非并发安全，由调用方加锁保护。
type GroupState struct {
	// Name 组名。
	Name string
	// Namespace 组命名空间。
	Namespace string
	// Key 组唯一 key。
	Key types.NamespacedName

	// Spec 组期望。
	Spec GroupSpec

	// Phase 当前 phase（来自 CRD status 缓存）。
	Phase Phase
	// ScheduledByGroup 已通过 Permit 原子放行的成员数。
	ScheduledByGroup int32

	// Active 正在执行 PreFilter~Permit 提交的成员数（计入 batch，§8.4）。
	Active int
	// Waiting 已 Permit Wait、等待组级批准的成员。
	Waiting []WaitingPod
	// WaitingByPod 成员 ID -> waiting 句柄（去重用，§14）。
	WaitingByPod map[string]WaitingPod

	// MemberOutcomes 记录各成员调度结论（快速失败路径，§7.3.1 R11）。
	MemberOutcomes map[string]MemberOutcome

	// ReleasedGeneration 最近一次整组放行的 generation（CAS 单调递增）。
	ReleasedGeneration int64

	// timer 组级超时定时器（R6：以首次进入等待为基准，后续不重置）。
	timer timerIface
	// timerDone 定时器已触发标志。
	timerDone bool

	// precheckOK 本组成员是否已通过 GangPrecheck（由预检模块置位）。
	PrecheckOK bool
}

// NewGroupState 构造一个组状态。
func NewGroupState(namespace, name string, spec GroupSpec) *GroupState {
	if spec.MaxSchedulingBatch <= 0 {
		spec.MaxSchedulingBatch = 4 // 默认（§6.1）
	}
	// maxSchedulingBatch = min(spec.maxSchedulingBatch, minMember)（§8.4）
	if spec.MaxSchedulingBatch > spec.MinMember && spec.MinMember > 0 {
		spec.MaxSchedulingBatch = spec.MinMember
	}
	if spec.ScheduleTimeout <= 0 {
		spec.ScheduleTimeout = 600 * time.Second // 默认（§6.1）
	}
	return &GroupState{
		Name:           name,
		Namespace:      namespace,
		Key:            types.NamespacedName{Namespace: namespace, Name: name},
		Spec:           spec,
		WaitingByPod:   map[string]WaitingPod{},
		MemberOutcomes: map[string]MemberOutcome{},
	}
}

// MemberCount 返回当前已知成员数（active + waiting）。
func (s *GroupState) MemberCount() int {
	return s.Active + len(s.Waiting)
}

// CanEnterBatch 判断是否允许新成员进入调度 cycle（§8.4 N1）。
// 仅 active 计入 batch，waiting 不计入（N1 关键：minMember > batch 时仍能凑齐）。
func (s *GroupState) CanEnterBatch() bool {
	return s.Active < int(s.Spec.MaxSchedulingBatch)
}

// EnterBatch CAS 式尝试进入调度 cycle。成功返回 true 并计入 active；
// 失败（超限）返回 false（调用方返回 Wait 让出名额，s3/t2）。
func (s *GroupState) EnterBatch() bool {
	if !s.CanEnterBatch() {
		return false
	}
	s.Active++
	return true
}

// ExitBatch 成员完成调度决策（Permit 提交 / 快速失败）后释放 active 名额。
// active 成员进入 waiting 后由 Permit 内部转移，此处用于 PreFilter 被拒等场景。
func (s *GroupState) ExitBatch() {
	if s.Active > 0 {
		s.Active--
	}
}

// IsRunning 判断组是否已放行且处于 Running（T4/t7 防御）。
func (s *GroupState) IsRunning() bool {
	return s.Phase == PhaseRunning && s.ScheduledByGroup >= s.Spec.MinMember
}

// IsFailed 判断组是否已失败（调度器拒绝 Failed 组，S3）。
func (s *GroupState) IsFailed() bool {
	return s.Phase == PhaseFailed
}

// IsPrecheckScope 判断组是否处于需要 GangPrecheck 的作用域（§7.3.1 R2）：
// 仅未放行组（PreScheduling/Scheduling）需要整组预检；已放行组补位跳过。
func (s *GroupState) IsPrecheckScope() bool {
	return s.Phase == PhasePreScheduling || s.Phase == PhaseScheduling
}

// HasReleased 判断组是否已放行（补位成员容错路径，T4 防御：必须 Running）。
func (s *GroupState) HasReleased() bool {
	return s.IsRunning()
}

// ResetAfterRollback 在组离开 Running（超时/失败/重置）时调用（§9.1 S4）。
// 将 ScheduledByGroup 置 0，保证回退重排后必须重新凑齐 minMember。
// ReleasedGeneration 保持单调递增（不在此处重置，CAS 保证）。
func (s *GroupState) ResetAfterRollback() {
	s.ScheduledByGroup = 0
	s.PrecheckOK = false
	s.clearTimer()
	// 清空 waiting（旧批次成员已 Reject）
	s.Waiting = nil
	s.WaitingByPod = map[string]WaitingPod{}
}

// clearTimer 停止并清理定时器。
func (s *GroupState) clearTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.timerDone = false
}

// TimerFired 返回组级超时定时器是否已触发（§7.3.1）。
func (s *GroupState) TimerFired() bool {
	return s.timerDone
}
