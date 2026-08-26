package gang

import (
	"testing"
	"time"

	core "github.com/chenxihui/TopoGang/pkg/gang"
)

// staticLister 返回固定组集合的 GroupLister。
type staticLister struct {
	groups map[string]*core.GroupState
}

func (s *staticLister) GetGroup(namespace, name string) *core.GroupState {
	return s.groups[namespace+"/"+name]
}

func TestQueueSort_PriorityThenCreation(t *testing.T) {
	now := time.Now()
	lister := &staticLister{groups: map[string]*core.GroupState{}}
	p := NewPlugin(lister)

	// 组 g1：高优先级、后创建；组 g2：低优先级、先创建
	p.Groups["ns/g1"] = core.NewGroupState("ns", "g1", core.GroupSpec{
		MinMember:         2,
		MaxSchedulingBatch: 2,
		ScheduleTimeout:    time.Minute,
		CreationTimestamp:  now.Add(-time.Minute),
	})
	p.Groups["ns/g2"] = core.NewGroupState("ns", "g2", core.GroupSpec{
		MinMember:         2,
		MaxSchedulingBatch: 2,
		ScheduleTimeout:    time.Minute,
		CreationTimestamp:  now.Add(-2 * time.Minute),
	})

	a := PodInfo{Namespace: "ns", Name: "a", GroupName: "g1", PriorityClass: 100, CreationTimestamp: now}
	b := PodInfo{Namespace: "ns", Name: "b", GroupName: "g2", PriorityClass: 50, CreationTimestamp: now}

	// 高优先级组 g1 应排在 g2 前（优先级优先于创建时间，§8.4 规则 1 优先）
	if !p.QueueLess(a, b) {
		t.Fatal("expected higher-priority group g1 to sort before g2")
	}
	if p.QueueLess(b, a) {
		t.Fatal("expected g2 not to sort before g1")
	}
}

func TestQueueSort_SamePriorityFIFO(t *testing.T) {
	now := time.Now()
	p := NewPlugin(&staticLister{groups: map[string]*core.GroupState{}})
	p.Groups["ns/g1"] = core.NewGroupState("ns", "g1", core.GroupSpec{MinMember: 2, MaxSchedulingBatch: 2, ScheduleTimeout: time.Minute, CreationTimestamp: now.Add(-time.Minute)})
	p.Groups["ns/g2"] = core.NewGroupState("ns", "g2", core.GroupSpec{MinMember: 2, MaxSchedulingBatch: 2, ScheduleTimeout: time.Minute, CreationTimestamp: now.Add(-2 * time.Minute)})

	a := PodInfo{Namespace: "ns", Name: "a", GroupName: "g1", PriorityClass: 100, CreationTimestamp: now} // 后创建
	b := PodInfo{Namespace: "ns", Name: "b", GroupName: "g2", PriorityClass: 100, CreationTimestamp: now} // 先创建

	// 同优先级，先创建（g2）应在先
	if !p.QueueLess(b, a) {
		t.Fatal("expected earlier-created g2 to sort before g1")
	}
}

func TestPreFilter_GroupFailedRejects(t *testing.T) {
	p := NewPlugin(&staticLister{groups: map[string]*core.GroupState{}})
	gs := core.NewGroupState("ns", "g1", core.GroupSpec{MinMember: 2, ScheduleTimeout: time.Minute})
	gs.Phase = core.PhaseFailed
	p.Groups["ns/g1"] = gs

	r := p.PreFilter(PodInfo{Namespace: "ns", Name: "p", GroupName: "g1"})
	if !r.Reject || r.Reason != "group-failed" {
		t.Fatalf("expected reject on failed group, got %+v", r)
	}
}

func TestPreFilter_SinglePodAllowed(t *testing.T) {
	p := NewPlugin(&staticLister{})
	r := p.PreFilter(PodInfo{Namespace: "ns", Name: "solo", GroupName: ""})
	if !r.Allow {
		t.Fatalf("expected single pod allowed, got %+v", r)
	}
}

func TestPreFilter_RunningGroupSkipsBatch(t *testing.T) {
	p := NewPlugin(&staticLister{groups: map[string]*core.GroupState{}})
	gs := core.NewGroupState("ns", "g1", core.GroupSpec{MinMember: 2, MaxSchedulingBatch: 1, ScheduleTimeout: time.Minute})
	gs.ScheduledByGroup = 2
	gs.Phase = core.PhaseRunning
	p.Groups["ns/g1"] = gs

	// Running 组补位成员：跳过 batch（batch=1 但已满也应允许补位，R2）
	r := p.PreFilter(PodInfo{Namespace: "ns", Name: "patch", GroupName: "g1"})
	if !r.Allow {
		t.Fatalf("expected Running group patch member allowed, got %+v", r)
	}
	if gs.Active != 0 {
		t.Fatalf("expected no batch consumed for Running group, got active=%d", gs.Active)
	}
}

func TestPreFilter_BatchLimit(t *testing.T) {
	p := NewPlugin(&staticLister{groups: map[string]*core.GroupState{}})
	gs := core.NewGroupState("ns", "g1", core.GroupSpec{MinMember: 8, MaxSchedulingBatch: 2, ScheduleTimeout: time.Minute})
	gs.Phase = core.PhaseScheduling
	p.Groups["ns/g1"] = gs

	// 填满 batch=2
	if r := p.PreFilter(PodInfo{Namespace: "ns", Name: "a", GroupName: "g1"}); !r.Allow {
		t.Fatalf("expected allow, got %+v", r)
	}
	if r := p.PreFilter(PodInfo{Namespace: "ns", Name: "b", GroupName: "g1"}); !r.Allow {
		t.Fatalf("expected allow, got %+v", r)
	}
	// 第 3 个超限 -> Wait（§8.4 s3/t2）
	r := p.PreFilter(PodInfo{Namespace: "ns", Name: "c", GroupName: "g1"})
	if !r.Wait {
		t.Fatalf("expected Wait on batch overflow, got %+v", r)
	}
}
