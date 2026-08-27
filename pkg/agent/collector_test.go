package agent

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

func TestCollector_DiscoverAndWrite(t *testing.T) {
	wr := NewInMemoryWriter()
	col := NewCollector(CollectorOptions{
		NodeName:         "node-a",
		DiscoverInterval: time.Hour, // 不自动触发，手动调用
		DomainStrategy:   topo.DomainClique,
		Source:           NewMockSource(Mock8TwoDomains, "node-a"),
		Writer:           wr,
	})

	// 手动执行一次
	col.discoverAndWrite(context.Background())

	view, err := wr.GetView(context.Background(), types.NamespacedName{Name: "node-a"})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !view.Healthy {
		t.Fatal("expected healthy after successful discover")
	}
	if view.Generation != 1 {
		t.Fatalf("expected generation 1, got %d", view.Generation)
	}

	topoData := wr.Topology("node-a")
	if topoData == nil {
		t.Fatal("expected topology written")
	}
	if len(topoData.Domains) != 2 {
		t.Fatalf("expected 2 domains attached, got %+v", topoData.Domains)
	}
}

func TestCollector_WriteOnce(t *testing.T) {
	// 相同 generation 重复写不视为新写入
	wr := NewInMemoryWriter()
	col := NewCollector(CollectorOptions{
		NodeName:         "node-a",
		DiscoverInterval: time.Hour,
		Source:           NewMockSource(Mock4SingleDomain, "node-a"),
		Writer:           wr,
	})
	col.discoverAndWrite(context.Background())
	col.discoverAndWrite(context.Background()) // gen 递增 => 应更新到 2
	view, _ := wr.GetView(context.Background(), types.NamespacedName{Name: "node-a"})
	if view.Generation != 2 {
		t.Fatalf("expected generation 2, got %d", view.Generation)
	}
}
