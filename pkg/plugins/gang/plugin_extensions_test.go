package gang

import (
	"testing"

	schedulingv1alpha1 "github.com/chenxihui/TopoGang/apis/scheduling/v1alpha1"
	core "github.com/chenxihui/TopoGang/pkg/gang"
)

// Filter 返回 nil/Allow（§7.3.1：组级判断由 Precheck 与 Permit 收敛）。
func TestFilter_AlwaysAllow(t *testing.T) {
	p := NewPlugin(&staticLister{})
	r := p.Filter(PodInfo{})
	if !r.Allow {
		t.Fatal("Filter should always allow at Gang level")
	}
}

// PreBind 写 gpu-uuids annotation（s1：由 Reserve 移至 PreBind）。
func TestPreBind_WritesGPUUUIDs(t *testing.T) {
	p := NewPlugin(&staticLister{})
	ann := p.PreBind(nil, "pod-1", []string{"GPU-1", "GPU-2"})
	if ann == nil {
		t.Fatal("expected annotations")
	}
	if v, ok := ann[schedulingv1alpha1.GPUUUIDsAnnotation]; !ok {
		t.Fatalf("expected GPUUUIDsAnnotation, got %v", ann)
	} else if v != "GPU-1,GPU-2" {
		t.Fatalf("expected 'GPU-1,GPU-2', got %q", v)
	}
}

// PreBind 空列表不写 annotation。
func TestPreBind_Empty(t *testing.T) {
	p := NewPlugin(&staticLister{})
	if ann := p.PreBind(nil, "pod-1", nil); ann != nil {
		t.Fatalf("expected nil annotations for empty GPUs, got %v", ann)
	}
}

// PostFilter 默认关闭：不抢占。
func TestPostFilter_DisabledByDefault(t *testing.T) {
	p := NewPlugin(&staticLister{})
	ok, cands := p.PostFilter(PodInfo{}, []PreemptCandidate{
		{GroupName: "low", MemberPodIDs: []string{"p1"}},
	})
	if ok || len(cands) != 0 {
		t.Fatalf("expected preemption disabled by default, ok=%v cands=%v", ok, cands)
	}
}

// PostFilter 开启时：整组抢占（§8.5 规则 2）。
func TestPostFilter_Enabled_WholeGroupPreemption(t *testing.T) {
	p := NewPlugin(&staticLister{})
	p.PreemptionEnabled = true
	ok, cands := p.PostFilter(PodInfo{}, []PreemptCandidate{
		{GroupName: "low", Namespace: "ns", MemberPodIDs: []string{"p1", "p2"}},
	})
	if !ok || len(cands) != 1 {
		t.Fatalf("expected preemption of whole low-priority group, ok=%v cands=%v", ok, cands)
	}
	if cands[0].GroupName != "low" {
		t.Fatalf("expected victim group 'low', got %q", cands[0].GroupName)
	}
	if len(cands[0].MemberPodIDs) != 2 {
		t.Fatalf("expected whole-group victims (2 pods), got %v", cands[0].MemberPodIDs)
	}
}

// StateMachineView 正确构造状态机输入。
func TestStateMachineView(t *testing.T) {
	gs := core.NewGroupState("ns", "g1", core.GroupSpec{MinMember: 3})
	gs.Phase = core.PhaseScheduling
	gs.ScheduledByGroup = 1
	v := StateMachineView(gs)
	if v.MinMember != 3 || v.ScheduledByGroup != 1 {
		t.Fatalf("unexpected view: %+v", v)
	}
}
