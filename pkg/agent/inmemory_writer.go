package agent

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/types"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// memoryStore 保存每个节点的拓扑快照，供 InMemoryWriter 使用。
type memoryStore struct {
	mu          sync.RWMutex
	topology    map[string]*topo.GpuTopology
	generation  map[string]int64
	lastWritten map[string]bool
}

// InMemoryWriter 是 Writer 的内存实现，用于无集群环境调试、单元测试与 mock 模式
// （§7.1 mock 源 / §10.2 拓扑模拟器）。
type InMemoryWriter struct {
	store *memoryStore
}

// NewInMemoryWriter 构造内存 Writer。
func NewInMemoryWriter() *InMemoryWriter {
	return &InMemoryWriter{store: &memoryStore{
		topology:    map[string]*topo.GpuTopology{},
		generation:  map[string]int64{},
		lastWritten: map[string]bool{},
	}}
}

// Write 覆盖内存快照并返回是否实际更新（generation 变化）。
func (w *InMemoryWriter) Write(_ context.Context, nodeName string, topology *topo.GpuTopology, generation int64) (bool, error) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	prevGen := w.store.generation[nodeName]
	// 拓扑为 nil（采集失败）时只更新健康标记相关状态，不清空已有数据
	if topology != nil {
		w.store.topology[nodeName] = cloneTopology(topology)
		w.store.generation[nodeName] = generation
		w.store.lastWritten[nodeName] = true
	}
	return generation > prevGen, nil
}

// GetView 返回某节点的轻量视图。
func (w *InMemoryWriter) GetView(_ context.Context, name types.NamespacedName) (*GpuTopologyView, error) {
	w.store.mu.RLock()
	defer w.store.mu.RUnlock()
	node := name.Name
	topoData, ok := w.store.topology[node]
	if !ok {
		return &GpuTopologyView{Generation: 0, Healthy: false}, nil
	}
	alloc := map[string]string{}
	for _, gp := range topoData.GPUs {
		alloc[gp.ID] = "" // 内存模式无分配信息
	}
	return &GpuTopologyView{
		Generation:  w.store.generation[node],
		Healthy:     true,
		Allocations: alloc,
	}, nil
}

// Topology 返回某节点的完整拓扑快照（测试用）。
func (w *InMemoryWriter) Topology(node string) *topo.GpuTopology {
	w.store.mu.RLock()
	defer w.store.mu.RUnlock()
	return cloneTopology(w.store.topology[node])
}

func cloneTopology(g *topo.GpuTopology) *topo.GpuTopology {
	if g == nil {
		return nil
	}
	cp := &topo.GpuTopology{
		NodeName: g.NodeName,
		GPUs:     make([]*topo.Gpu, 0, len(g.GPUs)),
		Links:    make([]*topo.Link, 0, len(g.Links)),
		Domains:  make([]topo.Domain, 0, len(g.Domains)),
	}
	for _, gp := range g.GPUs {
		g2 := *gp
		cp.GPUs = append(cp.GPUs, &g2)
	}
	for _, l := range g.Links {
		l2 := *l
		cp.Links = append(cp.Links, &l2)
	}
	for _, d := range g.Domains {
		d2 := d
		d2.GPUIndexes = append([]int(nil), d.GPUIndexes...)
		cp.Domains = append(cp.Domains, d2)
	}
	return cp
}
