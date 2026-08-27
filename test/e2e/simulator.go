// Package e2e 实现 §10.3 全场景用例的调度模拟器。
//
// 将 pkg/gang（Permit/GangPrecheck）、pkg/allocator（GPU 账本）、
// pkg/controller/state（PodGroup 状态机）与 pkg/plugins/topo（Filter/Score）
// 编排成一个端到端调度模拟器，在无集群环境下回归设计 §10.3 的关键场景。
package e2e

import (
	"time"

	"github.com/chenxihui/TopoGang/pkg/allocator"
	"github.com/chenxihui/TopoGang/pkg/controller/state"
	core "github.com/chenxihui/TopoGang/pkg/gang"
	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// Cluster 是一个模拟集群：N 节点 GPU + AllocationTracker。
type Cluster struct {
	// Tracker GPU 分配账本。
	Tracker *allocator.AllocationTracker
	// Nodes 节点拓扑视图。
	Nodes map[string]NodeView
	// Domains 每节点域列表。
	Domains map[string][]topo.Domain
}

// NodeView 是模拟节点的拓扑视图。
type NodeView struct {
	// TotalGPUs 节点 GPU 总数。
	TotalGPUs int
	// FreeGPUs 空闲 GPU 数。
	FreeGPUs int
	// Domains 域列表。
	Domains []topo.Domain
	// Healthy 是否健康（心跳）。
	Healthy bool
}

// NewCluster 构造模拟集群，每节点 8 卡（2 域 × 4）。
func NewCluster(nodes int) *Cluster {
	tr := allocator.NewAllocationTracker()
	c := &Cluster{
		Tracker: tr,
		Nodes:   map[string]NodeView{},
		Domains: map[string][]topo.Domain{},
	}
	for i := 0; i < nodes; i++ {
		name := "node-" + int2str(i)
		gpus := make([]string, 0, 8)
		dom := map[string]string{}
		for j := 0; j < 8; j++ {
			gid := "GPU-" + int2str(i) + "-" + int2str(j)
			gpus = append(gpus, gid)
			if j < 4 {
				dom[gid] = "d1"
			} else {
				dom[gid] = "d2"
			}
		}
		tr.AddNode(allocator.NodeGPUInfo{NodeName: name, GPUs: gpus, GpuDomain: dom, InManagedDomain: true})
		c.Nodes[name] = NodeView{TotalGPUs: 8, FreeGPUs: 8, Domains: []topo.Domain{
			{ID: "d1", GPUIndexes: []int{0, 1, 2, 3}},
			{ID: "d2", GPUIndexes: []int{4, 5, 6, 7}},
		}, Healthy: true}
		c.Domains[name] = c.Nodes[name].Domains
	}
	return c
}

// FreeCount 返回节点域空闲数。
func (c *Cluster) FreeCount(node, domain string) int {
	return c.Tracker.FreeCount(node, domain)
}

// AllocateOnNode 在某节点选择并记账 count 张 GPU（模拟 Reserve + SelectGPUs）。
// 返回是否成功。
func (c *Cluster) AllocateOnNode(node string, podUID string, count int) bool {
	selected, err := c.Tracker.SelectGPUs(node, count, c.Domains[node], nil, topo.DefaultDomainScoreParams())
	if err != nil {
		return false
	}
	for _, gpu := range selected {
		if err := c.Tracker.Allocate(node, gpu, podUID); err != nil {
			return false
		}
	}
	// 更新视图空闲数
	nv := c.Nodes[node]
	nv.FreeGPUs -= count
	c.Nodes[node] = nv
	return true
}

// GroupSim 是模拟中的一个 Gang 组。
type GroupSim struct {
	// State 组状态（Gang 核心）。
	State *core.GroupState
	// Members 成员 Pod 调度结论。
	Members map[string]bool
	// AllocatedNode 成员分配的节点。
	AllocatedNode map[string]string
	// PodStateController PodGroup 状态机。
	StateMachine *state.StateMachine
	// scheduledCount 已成功 Reserve 的成员数（GangPrecheck 整组模拟基准）。
	scheduledCount int
}

// NewGroupSim 构造组模拟。
func NewGroupSim(namespace, name string, minMember int32) *GroupSim {
	return &GroupSim{
		State: core.NewGroupState(namespace, name, core.GroupSpec{
			MinMember:         minMember,
			MaxSchedulingBatch: 4,
			ScheduleTimeout:    10 * time.Second,
		}),
		Members:        map[string]bool{},
		AllocatedNode:  map[string]string{},
		StateMachine:   state.New(state.Options{ScheduleTimeout: 10 * time.Second}),
	}
}

// ScheduleMember 模拟一个成员 Pod 的调度 cycle：
// 尝试在集群放置，成功则记账 + Permit，返回是否进入 waiting 或放行。
func (g *GroupSim) ScheduleMember(cl *Cluster, podID string, gpuCount int) (permitDecision core.Decision, ok bool) {
	if !g.State.EnterBatch() {
		return core.DecisionWait, false // batch 超限
	}
	defer g.State.ExitBatch()

	// GangPrecheck（§7.3.1）：整组可调度性模拟——模拟放置全部未调度成员
	// （含当前成员）。若整组无法全部放置，拒绝当前成员、整组不进 Reserve。
	remaining := int(g.State.Spec.MinMember) - g.scheduledCount
	if !g.canPlaceAll(cl, gpuCount, remaining) {
		return core.DecisionReject, false
	}

	// 实际选择节点放置
	node := g.chooseNode(cl, gpuCount)
	if node == "" {
		return core.DecisionReject, false
	}
	if !cl.AllocateOnNode(node, podID, gpuCount) {
		return core.DecisionReject, false
	}
	g.AllocatedNode[podID] = node
	g.scheduledCount++

	// Permit
	res := core.Permit(core.PermitInput{
		PodID:              podID,
		HasGroupAnnotation: true,
		Group:              g.State,
		NewWaitingPod: func(id string) core.WaitingPod {
			allowed := new(bool)
			g.Members[id] = false
			return &simWaitingPod{id: id, allowed: allowed, mark: func(b bool) { g.Members[id] = b }}
		},
		BumpGeneration: func() int64 {
			g.State.ReleasedGeneration++
			return g.State.ReleasedGeneration
		},
	})
	// waiting 成员已记账，不再占用 batch（N1 由 Permit 内部处理）
	return res.Decision, true
}

// canPlaceAll 整组可放置性（GangPrecheck §7.3.1 贪心模拟）：
// 模拟将 count 个（各请求 gpuCount 卡）成员贪心放置到所有健康节点。
// 全部放置成功才返回 true；否则整组拒绝。
func (g *GroupSim) canPlaceAll(cl *Cluster, gpuCount, count int) bool {
	// 复制各节点空闲数（不修改真实状态）
	freeByNode := map[string]int{}
	for name, nv := range cl.Nodes {
		if !nv.Healthy {
			continue
		}
		freeByNode[name] = nv.FreeGPUs
	}
	placed := 0
	for placed < count {
		// 贪心：找能放下 gpuCount 的节点
		found := false
		for name, free := range freeByNode {
			if free >= gpuCount {
				freeByNode[name] = free - gpuCount
				placed++
				found = true
				break
			}
		}
		if !found {
			// 无法放置所有剩余成员
			return false
		}
	}
	return true
}

// chooseNode 选择一个可放置的节点。
func (g *GroupSim) chooseNode(cl *Cluster, gpuCount int) string {
	for name, nv := range cl.Nodes {
		if !nv.Healthy {
			continue
		}
		if nv.FreeGPUs >= gpuCount {
			return name
		}
	}
	return ""
}

// AdvanceState 依据状态机推进 PodGroup phase（§9.1）。
func (g *GroupSim) AdvanceState() state.Decision {
	view := state.GroupView{
		Phase:            state.Phase(g.State.Phase),
		MinMember:        g.State.Spec.MinMember,
		ScheduledByGroup: g.State.ScheduledByGroup,
		Now:              time.Now(),
		CreationTime:     time.Now().Add(-time.Second),
	}
	return g.StateMachine.Observe(view)
}

// simWaitingPod 实现 core.WaitingPod。
type simWaitingPod struct {
	id      string
	allowed *bool
	mark    func(bool)
}

func (s *simWaitingPod) ID() string { return s.id }
func (s *simWaitingPod) Allow() {
	*s.allowed = true
	if s.mark != nil {
		s.mark(true)
	}
}
func (s *simWaitingPod) Reject(string) {}

func int2str(v int) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}
