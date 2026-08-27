package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRecorder_Registration(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg)
	if r == nil {
		t.Fatal("recorder should not be nil")
	}
	// 指标应能正常注册（无重复 panic）
	r.RecordSchedulingCycle(0.01)
	r.RecordGangQueue(1.5)
	r.SetWaitingPods("g1", "ns", 4)
	r.IncPreempted(2)
	r.IncTimeout()
	r.IncDrift()
	r.SetDerived(0.9, 0.05, 0.1)
}

// t4 口径：scheduling_cycle 按每次调度 attempt 记录（Permit 等待不计入）。
func TestRecorder_CycleMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg)
	for i := 0; i < 5; i++ {
		r.RecordSchedulingCycle(0.02) // 5 次 attempt，每次 20ms
	}
	// 通过 gather 检查 count=5
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}
	found := false
	for _, m := range metrics {
		if m.GetName() == "topogang_scheduling_cycle_seconds" {
			found = true
			if len(m.GetMetric()) == 0 {
				t.Fatal("expected metric samples")
			}
			h := m.GetMetric()[0].GetHistogram()
			if h.GetSampleCount() != 5 {
				t.Fatalf("expected 5 samples, got %d", h.GetSampleCount())
			}
		}
	}
	if !found {
		t.Fatal("scheduling_cycle metric not found")
	}
}

func TestDerive(t *testing.T) {
	nodes := []NodeFragState{
		{TotalGPUs: 4, FreeGPUs: 4, FreeByDomain: map[string]int{"d1": 4}},
		{TotalGPUs: 4, FreeGPUs: 3, FreeByDomain: map[string]int{"d1": 1, "d2": 2}},
	}
	snap := Derive(8, 10, nodes) // 8/10 同域
	if snap.AffinityHitRate != 0.8 {
		t.Fatalf("expected hit rate 0.8, got %f", snap.AffinityHitRate)
	}
	if snap.CrossDomainRatio < 0.19 || snap.CrossDomainRatio > 0.21 {
		t.Fatalf("expected cross-domain ~0.2, got %f", snap.CrossDomainRatio)
	}
	// 碎片：d1 有 1 空闲（<2），其余 4+2=6 非碎片。总空闲=1+1+2=4？节点2 d1=1,d2=2。
	// 空闲总数=4+1+2=7，碎片=1（节点2 d1 的 1 卡）。fragment=1/7≈0.143
	if snap.FragmentRate < 0.1 || snap.FragmentRate > 0.2 {
		t.Fatalf("expected fragment rate ~0.143, got %f", snap.FragmentRate)
	}
}

func TestDerive_ZeroScheduled(t *testing.T) {
	snap := Derive(0, 0, nil)
	if snap.AffinityHitRate != 0 {
		t.Fatal("expected 0 hit rate when nothing scheduled")
	}
}

func TestDerive_NoFree(t *testing.T) {
	nodes := []NodeFragState{
		{TotalGPUs: 4, FreeGPUs: 0, FreeByDomain: map[string]int{"d1": 0}},
	}
	snap := Derive(1, 1, nodes)
	if snap.FragmentRate != 0 {
		t.Fatalf("expected 0 fragment rate (no free), got %f", snap.FragmentRate)
	}
}
