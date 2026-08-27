package topo

import (
	"testing"

	"github.com/chenxihui/TopoGang/pkg/allocator"
	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

func node8() NodeView {
	return NodeView{
		Name:       "node-a",
		TotalGPUs:  8,
		FreeGPUs:   8,
		Domains:    []topo.Domain{{ID: "d1", GPUIndexes: []int{0, 1, 2, 3}}, {ID: "d2", GPUIndexes: []int{4, 5, 6, 7}}},
		Health:     HealthHealthy,
		InManagedDomain: true,
	}
}

func TestFilter_EnoughGPUs(t *testing.T) {
	p := NewTopoPlugin(nil)
	r := p.Filter(PodGPURequest{Count: 4, Policy: PolicyNone}, node8())
	if !r.Allow {
		t.Fatalf("expected allow, got %+v", r)
	}
}

func TestFilter_InsufficientGPUs(t *testing.T) {
	p := NewTopoPlugin(nil)
	n := node8()
	n.FreeGPUs = 2
	r := p.Filter(PodGPURequest{Count: 4, Policy: PolicyNone}, n)
	if r.Allow {
		t.Fatal("expected reject on insufficient GPU count")
	}
}

// 强制 nvlink：单域无法容纳时拒绝（§7.3.2）。
func TestFilter_ForcedNvlink_NoSingleDomain(t *testing.T) {
	p := NewTopoPlugin(nil)
	// 每域最多 2 空闲，请求 4 -> 无单域可容纳
	free := func(node, domain string) int {
		if domain == "d1" || domain == "d2" {
			return 2
		}
		return 0
	}
	p.Free = free
	n := node8()
	r := p.Filter(PodGPURequest{Count: 4, Policy: PolicyNvlink}, n)
	if r.Allow {
		t.Fatal("expected reject: no single domain can hold 4 GPUs")
	}
}

// 强制 nvlink：单域可容纳时通过。
func TestFilter_ForcedNvlink_SingleDomainOK(t *testing.T) {
	p := NewTopoPlugin(nil)
	free := func(node, domain string) int {
		if domain == "d1" {
			return 4
		}
		return 1
	}
	p.Free = free
	r := p.Filter(PodGPURequest{Count: 4, Policy: PolicyNvlink}, node8())
	if !r.Allow {
		t.Fatalf("expected allow, got %+v", r)
	}
}

// T2：心跳过期节点完全停止分配（Filter 不返回该节点）。
func TestFilter_HeartbeatStaleRejected(t *testing.T) {
	p := NewTopoPlugin(nil)
	n := node8()
	n.Health = HealthHeartbeatStale
	r := p.Filter(PodGPURequest{Count: 1, Policy: PolicyNone}, n)
	if r.Allow {
		t.Fatal("expected reject on heartbeat-stale node (T2)")
	}
}

// s9：数据缺失节点仅数量过滤、不选卡（Allow 但 DataMissing=true）。
func TestFilter_DataMissing_QuantityOnly(t *testing.T) {
	p := NewTopoPlugin(nil)
	n := node8()
	n.Health = HealthDataMissing
	r := p.Filter(PodGPURequest{Count: 2, Policy: PolicyNone}, n)
	if !r.Allow || !r.DataMissing {
		t.Fatalf("expected allow with DataMissing=true, got %+v", r)
	}
}

// Score：健康节点 topoAffinity=1.0（单域可容纳）分数高于不健康节点。
func TestScore_HealthyHigherThanUnhealthy(t *testing.T) {
	p := NewTopoPlugin(nil)
	req := PodGPURequest{Count: 4, Policy: PolicyNone, SiblingGPUs: map[string]map[string]bool{}}
	healthy := p.Score(req, node8())
	unhealthy := node8()
	unhealthy.Health = HealthDataMissing
	unhealthyScore := p.Score(req, unhealthy)
	if healthy <= unhealthyScore {
		t.Fatalf("expected healthy node score > unhealthy, got %f vs %f", healthy, unhealthyScore)
	}
}

// Score：空闲越多的节点 Balance 越高。
func TestScore_Balance(t *testing.T) {
	p := NewTopoPlugin(nil)
	req := PodGPURequest{Count: 1, Policy: PolicyNone, SiblingGPUs: map[string]map[string]bool{}}
	full := node8()
	full.FreeGPUs = 8
	fullScore := p.Score(req, full)
	partial := node8()
	partial.FreeGPUs = 2
	partialScore := p.Score(req, partial)
	if fullScore <= partialScore {
		t.Fatalf("expected more-free node higher balance score, got %f vs %f", fullScore, partialScore)
	}
}

// GangAffinity：同节点兄弟亲和提升分数（§8.2）。
func TestScore_GangAffinity(t *testing.T) {
	p := NewTopoPlugin(nil)
	withSibling := PodGPURequest{Count: 2, Policy: PolicyNone,
		SiblingGPUs: map[string]map[string]bool{"node-a": {"GPU0": true}}}
	noSibling := PodGPURequest{Count: 2, Policy: PolicyNone, SiblingGPUs: map[string]map[string]bool{}}
	s1 := p.Score(withSibling, node8())
	s2 := p.Score(noSibling, node8())
	if s1 <= s2 {
		t.Fatalf("expected sibling-affinity node score higher, got %f vs %f", s1, s2)
	}
}

// 与 AllocationTracker 集成：Filter 用 FreeCount，SelectGPUs 用 best-fit。
func TestFilter_WithAllocator(t *testing.T) {
	tr := allocator.NewAllocationTracker()
	tr.AddNode(allocator.NodeGPUInfo{
		NodeName: "node-a",
		GPUs:     []string{"GPU0", "GPU1", "GPU2", "GPU3"},
		GpuDomain: map[string]string{"GPU0": "d1", "GPU1": "d1", "GPU2": "d1", "GPU3": "d1"},
		InManagedDomain: true,
	})
	p := NewTopoPlugin(tr)
	n := NodeView{
		Name: "node-a", TotalGPUs: 4, FreeGPUs: 4,
		Domains: []topo.Domain{{ID: "d1", GPUIndexes: []int{0, 1, 2, 3}}},
		Health:  HealthHealthy, InManagedDomain: true,
	}
	// 占 2 张
	_ = tr.Allocate("node-a", "GPU0", "p1")
	_ = tr.Allocate("node-a", "GPU1", "p1")
	n.FreeGPUs = 2
	r := p.Filter(PodGPURequest{Count: 2, Policy: PolicyNvlink}, n)
	if !r.Allow {
		t.Fatalf("expected allow (2 free in single domain), got %+v", r)
	}
	r = p.Filter(PodGPURequest{Count: 3, Policy: PolicyNvlink}, n)
	if r.Allow {
		t.Fatal("expected reject: only 2 free in domain, request 3")
	}
}
