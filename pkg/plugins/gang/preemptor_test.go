package gang

import "testing"

func TestPreemptor_DisabledByDefault(t *testing.T) {
	p := NewPreemptor()
	_, ok := p.FindVictim(100, []Candidate{
		{Namespace: "ns", Name: "low", Priority: 50, GPUPods: []string{"p1"}},
	})
	if ok {
		t.Fatal("preemption should be disabled by default (S3: Gang 抢占易破坏组完整性)")
	}
}

// §8.5 规则 1：仅抢占优先级严格低于发起者的组。
func TestPreemptor_OnlyLowerPriority(t *testing.T) {
	p := NewPreemptor()
	p.Enabled = true
	// 高优组不应被抢
	victims, ok := p.FindVictim(100, []Candidate{
		{Namespace: "ns", Name: "higher", Priority: 200, GPUPods: []string{"p1"}},
		{Namespace: "ns", Name: "equal", Priority: 100, GPUPods: []string{"p2"}},
		{Namespace: "ns", Name: "lower", Priority: 50, GPUPods: []string{"p3"}},
	})
	if !ok || len(victims) != 1 {
		t.Fatalf("expected only lower-priority group preemptable, got %+v", victims)
	}
	if victims[0].Name != "lower" {
		t.Fatalf("expected victim group 'lower', got %q", victims[0].Name)
	}
}

// §8.5 规则 2：整组抢占——受害者返回低优组全部 GPU Pod。
func TestPreemptor_WholeGroupPreemption(t *testing.T) {
	p := NewPreemptor()
	p.Enabled = true
	victims, ok := p.FindVictim(100, []Candidate{
		{Namespace: "ns", Name: "low", Priority: 50, GPUPods: []string{"low-0", "low-1", "low-2"}, TotalPods: 4},
	})
	if !ok {
		t.Fatal("expected preemption candidates")
	}
	evicted := PreemptVictims(victims)
	if len(evicted) != 3 {
		t.Fatalf("expected whole-group eviction (3 GPU pods), got %v", evicted)
	}
}

// 无低优 GPU 占用组时不可抢占。
func TestPreemptor_NoVictim(t *testing.T) {
	p := NewPreemptor()
	p.Enabled = true
	_, ok := p.FindVictim(100, []Candidate{
		{Namespace: "ns", Name: "no-gpu-pods", Priority: 50, GPUPods: nil},
	})
	if ok {
		t.Fatal("expected no victim when group has no GPU pods")
	}
}

// 确定性排序：优先级低者先，同优先级按组名。
func TestPreemptor_DeterministicOrder(t *testing.T) {
	p := NewPreemptor()
	p.Enabled = true
	victims, _ := p.FindVictim(100, []Candidate{
		{Namespace: "ns", Name: "b", Priority: 50, GPUPods: []string{"p1"}},
		{Namespace: "ns", Name: "a", Priority: 50, GPUPods: []string{"p2"}},
		{Namespace: "ns", Name: "c", Priority: 30, GPUPods: []string{"p3"}},
	})
	if len(victims) != 3 {
		t.Fatalf("expected 3 victims, got %d", len(victims))
	}
	// 优先级最低的 c 在最前
	if victims[0].Name != "c" {
		t.Fatalf("expected lowest-priority 'c' first, got %q", victims[0].Name)
	}
	// 同优先级 a 在 b 前
	if victims[1].Name != "a" || victims[2].Name != "b" {
		t.Fatalf("expected a before b, got %v", victims[1:])
	}
}
