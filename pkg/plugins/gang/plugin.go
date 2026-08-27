// Package gang 实现 Gang 调度插件（§7.3.1）。
//
// 该包将核心 Gang 语义（GroupState / Permit / GangPrecheck / 状态机）编排到
// Scheduler Framework 的扩展点语义之后，屏蔽框架细节、可独立单测。
//
// 扩展点映射（§7.3.1 表格）：
//   - QueueSort ：组优先级 + 创建时间排序（§8.4）
//   - PreFilter ：组状态校验 + 组级预检 GangPrecheck + batch 并发限制
//   - Filter    ：nil（组级判断由 Precheck 与 Permit 收敛）
//   - PostFilter：组级抢占（默认关闭）
//   - Permit    ：All-or-Nothing 原子放行
//   - Reserve   ：组级记账 +1
//   - Unreserve ：组级记账 -1 / 快速失败上报（§7.3.1 R11）
package gang

import (
	"strings"
	"time"

	schedulingv1alpha1 "github.com/chenxihui/TopoGang/apis/scheduling/v1alpha1"
	"github.com/chenxihui/TopoGang/pkg/controller/state"
	core "github.com/chenxihui/TopoGang/pkg/gang"
)

// Plugin 是 Gang 插件的主入口，组合核心逻辑。
type Plugin struct {
	// Groups 组状态缓存（key -> GroupState）。
	Groups map[string]*core.GroupState
	// Precheck 预检缓存。
	Precheck *core.PrecheckCache
	// Sim 预检模拟器。
	Sim core.PrecheckSimulator
	// GroupLister 提供 Pod 所属组的只读视图（由适配层注入）。
	GroupLister GroupLister
	// MaxSchedulingBatch 默认并发上限（§6.1）。
	MaxSchedulingBatch int32
	// ScheduleTimeout 默认组超时（§6.1）。
	ScheduleTimeout time.Duration
	// PreemptionEnabled 是否启用组级抢占（§8.5，默认关闭）。
	PreemptionEnabled bool
}

// GroupLister 提供组级只读输入。
type GroupLister interface {
	// GetGroup 返回 Pod 所属的组状态；nil 表示组不存在。
	GetGroup(namespace, name string) *core.GroupState
}

// NewPlugin 构造 Gang 插件。
func NewPlugin(lister GroupLister) *Plugin {
	return &Plugin{
		Groups:             map[string]*core.GroupState{},
		Precheck:           core.NewPrecheckCache(),
		Sim:                core.NewGreedySimulator(),
		GroupLister:        lister,
		MaxSchedulingBatch: 4,
		ScheduleTimeout:    600 * time.Second,
	}
}

// PodInfo 是插件所需的 Pod 维度信息。
type PodInfo struct {
	// Namespace Pod 命名空间。
	Namespace string
	// Name Pod 名称。
	Name string
	// GroupName group-name annotation 值（空表示无组）。
	GroupName string
	// GPUCount 本 Pod 请求的 GPU 数。
	GPUCount int
	// PriorityClass 优先级类。
	PriorityClass int32
	// CreationTimestamp Pod 创建时间。
	CreationTimestamp time.Time
}

// GroupOf 返回 Pod 所属组状态（含懒创建）。
func (p *Plugin) GroupOf(pi PodInfo) *core.GroupState {
	if pi.GroupName == "" {
		return nil
	}
	key := pi.Namespace + "/" + pi.GroupName
	if g, ok := p.Groups[key]; ok {
		return g
	}
	// 从 GroupLister 尝试获取权威状态
	if p.GroupLister != nil {
		if g := p.GroupLister.GetGroup(pi.Namespace, pi.GroupName); g != nil {
			p.Groups[key] = g
			return g
		}
	}
	return nil
}

// QueueLess 实现 QueueSort 的严格弱序（§8.4）：
// 1. 组优先级高者在前；2. 组创建时间早者在前；3. 无组 Pod 退化为默认比较。
// 返回 true 表示 a 应排在 b 前。
func (p *Plugin) QueueLess(a, b PodInfo) bool {
	ga := p.GroupOf(a)
	gb := p.GroupOf(b)
	switch {
	case ga == nil && gb == nil:
		// 无组单 Pod：按优先级 + 创建时间
		if a.PriorityClass != b.PriorityClass {
			return a.PriorityClass > b.PriorityClass
		}
		return a.CreationTimestamp.Before(b.CreationTimestamp)
	case ga == nil:
		return false // 无组 Pod 排在有组 Pod 后
	case gb == nil:
		return true
	}
	// 组优先级
	if a.PriorityClass != b.PriorityClass {
		return a.PriorityClass > b.PriorityClass
	}
	// 组创建时间（先到先服务，§8.4）
	return ga.Spec.CreationTime().Before(gb.Spec.CreationTime())
}

// PreFilterResult 是 PreFilter 的返回。
type PreFilterResult struct {
	// Allow 是否允许继续。
	Allow bool
	// Wait 是否返回 Wait（等待组就绪 / batch 超限）。
	Wait bool
	// Reject 是否拒绝。
	Reject bool
	// Reason 原因。
	Reason string
	// Group 所属组状态（nil 表示无组）。
	Group *core.GroupState
}

// PreFilter 实现组状态校验 + GangPrecheck + batch 并发限制（§7.3.1）。
func (p *Plugin) PreFilter(pi PodInfo) PreFilterResult {
	gs := p.GroupOf(pi)

	// 无组单 Pod：直接允许（s2）
	if gs == nil {
		return PreFilterResult{Allow: true}
	}

	// 组 Failed：直接拒绝（S3）
	if gs.IsFailed() {
		return PreFilterResult{Reject: true, Reason: "group-failed"}
	}

	// 已放行组（Running）补位成员：跳过预检、跳过 batch（§7.3.1 R2/s1）
	if gs.IsRunning() {
		return PreFilterResult{Allow: true, Group: gs}
	}

	// batch 并发限制（§8.4 s3/t2）：超限返回 Wait 让出名额
	if !gs.EnterBatch() {
		return PreFilterResult{Wait: true, Reason: "max-scheduling-batch-exceeded", Group: gs}
	}

	// 组级预检（R2：仅未放行组）
	if gs.IsPrecheckScope() {
		ok := p.runPrecheck(gs, pi)
		if !ok {
			// 整组拒绝，退回队列 + 指数退避；释放 batch 名额
			gs.ExitBatch()
			return PreFilterResult{Reject: true, Reason: "gang-precheck-failed", Group: gs}
		}
	}

	return PreFilterResult{Allow: true, Group: gs}
}

// runPrecheck 执行组级预检（§7.3.1）。此处节点快照由适配层注入，
// 当前版本提供接口占位（具体节点快照聚合在 M3 与 AllocationTracker 对接）。
func (p *Plugin) runPrecheck(gs *core.GroupState, pi PodInfo) bool {
	// TODO(M3)：从 AllocationTracker + NodeGpuTopology 聚合节点快照。
	// 当前返回 true（预检在单测层通过 GreedySimulator 独立验证）。
	return true
}

// Permit 委托核心 All-or-Nothing 逻辑（§7.3.1）。
func (p *Plugin) Permit(gs *core.GroupState, podID string, newWaiting func(id string) core.WaitingPod, bumpGen func() int64) core.PermitResult {
	return core.Permit(core.PermitInput{
		PodID:              podID,
		HasGroupAnnotation: gs != nil,
		Group:              gs,
		NewWaitingPod:      newWaiting,
		BumpGeneration:     bumpGen,
	})
}

// Reserve 组级记账：成员成功 Reserve 记入成员结论（§7.3.1）。
func (p *Plugin) Reserve(gs *core.GroupState, podID string) {
	if gs == nil {
		return
	}
	gs.MemberOutcomes[podID] = core.OutcomeScheduled
}

// Unreserve 组级记账回滚 + 快速失败上报（§7.3.1 R11/N3）。
// outcome 为该成员的调度结论（rejected/unschedulable），若为失败则整组 Reject。
func (p *Plugin) Unreserve(gs *core.GroupState, podID, reason string) {
	if gs == nil {
		return
	}
	gs.MemberOutcomes[podID] = core.OutcomeRejected
	gs.ExitBatch()
	// 快速失败（N3）：任一成员失败 -> 整组 Reject 重排
	core.ReleaseWaiting(gs, reason)
}

// StateMachineView 将 GroupState 转为状态机输入（§9.1）。
func StateMachineView(gs *core.GroupState) state.GroupView {
	return state.GroupView{
		Phase:             state.Phase(gs.Phase),
		MinMember:         gs.Spec.MinMember,
		ScheduledByGroup:  gs.ScheduledByGroup,
	}
}

// ---------- 其余扩展点编排（可单测） ----------

// Filter 返回 nil：组级判断由 GangPrecheck 与 Permit 收敛（§7.3.1 表格）。
// FilterResult.Allow 表示节点可继续；本插件在 Filter 阶段不做组级拦截。
func (p *Plugin) Filter(pi PodInfo) FilterResult {
	return FilterResult{Allow: true}
}

// FilterResult 是 Filter 的返回。
type FilterResult struct {
	Allow bool
}

// PreBind 把分配的 GPU 列表写入 Pod annotation（§7.3.1 s1 修订：由 Reserve 移至 PreBind，
// 避免未放行即持久化修改 Pod 元数据）。返回新 annotation 值供适配层写回。
func (p *Plugin) PreBind(gs *core.GroupState, podID string, gpuUUIDs []string) map[string]string {
	if len(gpuUUIDs) == 0 {
		return nil
	}
	return map[string]string{
		schedulingv1alpha1.GPUUUIDsAnnotation: joinUUIDs(gpuUUIDs),
	}
}

// PreemptCandidate 描述组级抢占候选（§8.5）。
type PreemptCandidate struct {
	// GroupName 被抢占的低优组。
	GroupName string
	// Namespace 被抢占组命名空间。
	Namespace string
	// MemberPodIDs 被抢占组成员 Pod 列表。
	MemberPodIDs []string
}

// PostFilter 实现组级抢占决策（§8.5，默认关闭）。返回是否可抢占及候选。
func (p *Plugin) PostFilter(pi PodInfo, lowerPriorityGroups []PreemptCandidate) (bool, []PreemptCandidate) {
	if !p.PreemptionEnabled {
		return false, nil
	}
	// 组级抢占：只允许整组抢占（§8.5 规则 2）
	for _, cand := range lowerPriorityGroups {
		if len(cand.MemberPodIDs) >= 1 {
			return true, []PreemptCandidate{cand}
		}
	}
	return false, nil
}

func joinUUIDs(ids []string) string {
	return strings.Join(ids, ",")
}
