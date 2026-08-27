package gang

import "sort"

// Preemptor 实现组级抢占（§8.5）。
//
// 默认关闭（Gang 任务抢占易破坏组完整性，§8.5）。开启时（
// PodGroup.spec.preemptionPolicy=PreemptLowerPriority）：
//  1. 筛选候选：占用 GPU 且属于更低优先级 PodGroup 的 Pod。
//  2. 组级抢占：只允许整组抢占（该低优组所有 Pod 一起驱逐），避免"部分驱逐导致低优组碎片"。
//  3. 通过 preemptionVictims 返回驱逐列表，调度框架执行驱逐后重试。
type Preemptor struct {
	// Enabled 是否启用组级抢占（默认关闭）。
	Enabled bool
}

// NewPreemptor 构造抢占器。
func NewPreemptor() *Preemptor {
	return &Preemptor{Enabled: false}
}

// Candidate 是一个可被抢占的低优先级组。
type Candidate struct {
	// Namespace 组命名空间。
	Namespace string
	// Name 组名。
	Name string
	// Priority 该组优先级（低于抢占发起组才可被抢）。
	Priority int32
	// GPUPods 该组占用 GPU 的成员 Pod（整组驱逐目标）。
	GPUPods []string
	// TotalPods 该组全部成员 Pod（整组一致性）。
	TotalPods int
}

// FindVictim 依据抢占发起者的优先级筛选可被整组抢占的低优组。
//
// 规则（§8.5）：
//  1. 仅抢占优先级严格低于发起者的组。
//  2. 仅整组抢占：候选组所有 GPU 占用 Pod 一起驱逐（victims=GPUPods）。
//  3. 返回排序后的候选（低优组、按组名确定性排序），由调度框架驱逐后重试。
func (p *Preemptor) FindVictim(requesterPriority int32, candidates []Candidate) ([]Candidate, bool) {
	if !p.Enabled {
		return nil, false
	}
	var victims []Candidate
	for _, c := range candidates {
		if c.Priority >= requesterPriority {
			continue // 仅抢更低优先级（§8.5 规则 1）
		}
		// 整组抢占：仅当该组有 GPU 占用 Pod 才可作为受害者
		if len(c.GPUPods) > 0 {
			victims = append(victims, c)
		}
	}
	if len(victims) == 0 {
		return nil, false
	}
	// 确定性排序：优先级低者优先，再按组名
	sort.Slice(victims, func(i, j int) bool {
		if victims[i].Priority != victims[j].Priority {
			return victims[i].Priority < victims[j].Priority
		}
		if victims[i].Namespace != victims[j].Namespace {
			return victims[i].Namespace < victims[j].Namespace
		}
		return victims[i].Name < victims[j].Name
	})
	return victims, true
}

// PreemptVictims 返回应被驱逐的 GPU Pod 列表（整组，§8.5 规则 2）。
func PreemptVictims(victims []Candidate) []string {
	var out []string
	for _, v := range victims {
		out = append(out, v.GPUPods...)
	}
	return out
}

// GroupPreemptionDecision 是一次组级抢占的完整决策（供日志/审计）。
type GroupPreemptionDecision struct {
	// VictimGroups 被抢占的低优组。
	VictimGroups []string
	// EvictedPods 被驱逐的 GPU Pod 列表（整组）。
	EvictedPods []string
	// WholeGroup 是否整组抢占（§8.5 规则 2，恒为 true）。
	WholeGroup bool
}
