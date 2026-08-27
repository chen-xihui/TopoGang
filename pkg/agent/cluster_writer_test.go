package agent

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	topologyv1alpha1 "github.com/chenxihui/TopoGang/apis/topology/v1alpha1"
	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

func clusterScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = topologyv1alpha1.AddToScheme(s)
	return s
}

// 构造 8 卡双域拓扑。
func testTopo8(node string) *topo.GpuTopology {
	g := &topo.GpuTopology{NodeName: node}
	for i := 0; i < 8; i++ {
		g.GPUs = append(g.GPUs, &topo.Gpu{Index: i, ID: "GPU" + string(rune('0'+i))})
	}
	for a := 0; a < 4; a++ {
		for b := a + 1; b < 4; b++ {
			g.Links = append(g.Links, &topo.Link{A: a, B: b, LinkType: topo.LinkNVLink, Bandwidth: 600})
		}
	}
	for a := 4; a < 8; a++ {
		for b := a + 1; b < 8; b++ {
			g.Links = append(g.Links, &topo.Link{A: a, B: b, LinkType: topo.LinkNVLink, Bandwidth: 600})
		}
	}
	g.Domains = []topo.Domain{
		{ID: "nvlink-1", GPUIndexes: []int{0, 1, 2, 3}},
		{ID: "nvlink-2", GPUIndexes: []int{4, 5, 6, 7}},
	}
	return g
}

func TestClusterWriter_WriteCreate(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(clusterScheme()).
		WithStatusSubresource(&topologyv1alpha1.NodeGpuTopology{}).Build()
	w := NewClusterWriter(cl)
	ctx := context.Background()

	written, err := w.Write(ctx, "node-a", testTopo8("node-a"), 1)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if !written {
		t.Fatal("expected write to create")
	}

	var obj topologyv1alpha1.NodeGpuTopology
	if err := cl.Get(ctx, types.NamespacedName{Name: "node-node-a"}, &obj); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(obj.Spec.Gpus) != 8 {
		t.Fatalf("expected 8 gpus, got %d", len(obj.Spec.Gpus))
	}
	if len(obj.Spec.Domains) != 2 {
		t.Fatalf("expected 2 domains, got %d", len(obj.Spec.Domains))
	}
	if obj.Status.Healthy == nil || !*obj.Status.Healthy {
		t.Fatal("expected healthy status")
	}
}

func TestClusterWriter_WriteUpdate(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(clusterScheme()).
		WithStatusSubresource(&topologyv1alpha1.NodeGpuTopology{}).Build()
	w := NewClusterWriter(cl)
	ctx := context.Background()

	_, _ = w.Write(ctx, "node-a", testTopo8("node-a"), 1)
	written, err := w.Write(ctx, "node-a", testTopo8("node-a"), 2)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if !written {
		t.Fatal("expected write to update on new generation")
	}
	var obj topologyv1alpha1.NodeGpuTopology
	_ = cl.Get(ctx, types.NamespacedName{Name: "node-node-a"}, &obj)
	if obj.Spec.Generation != 2 {
		t.Fatalf("expected generation 2, got %d", obj.Spec.Generation)
	}
}

func TestClusterWriter_GetView(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(clusterScheme()).
		WithStatusSubresource(&topologyv1alpha1.NodeGpuTopology{}).Build()
	w := NewClusterWriter(cl)
	ctx := context.Background()

	_, _ = w.Write(ctx, "node-a", testTopo8("node-a"), 3)
	view, err := w.GetView(ctx, types.NamespacedName{Name: "node-a"})
	if err != nil {
		t.Fatalf("getview failed: %v", err)
	}
	if view.Generation != 3 || !view.Healthy {
		t.Fatalf("unexpected view: %+v", view)
	}
}

// §7.3.3 校正路径：回填 allocatedTo。
func TestClusterWriter_BackfillAllocation(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(clusterScheme()).
		WithStatusSubresource(&topologyv1alpha1.NodeGpuTopology{}).Build()
	w := NewClusterWriter(cl)
	ctx := context.Background()

	_, _ = w.Write(ctx, "node-a", testTopo8("node-a"), 1)
	if err := w.BackfillAllocation(ctx, "node-a", "GPU0", "pod-uid-1", "pod-1", "ns"); err != nil {
		t.Fatalf("backfill failed: %v", err)
	}
	var obj topologyv1alpha1.NodeGpuTopology
	_ = cl.Get(ctx, types.NamespacedName{Name: "node-node-a"}, &obj)
	found := false
	for _, g := range obj.Spec.Gpus {
		if g.ID == "GPU0" && g.AllocatedTo != nil && g.AllocatedTo.PodUID == "pod-uid-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected GPU0 allocatedTo backfilled to pod-uid-1")
	}
}
