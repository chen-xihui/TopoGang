// Package agent 实现 topo-agent 的采集链路（§7.1）：
// Source 接口（可插拔数据源）+ nvidia-smi / mock 实现 + NodeGpuTopology Writer。
package agent

import (
	"context"

	"k8s.io/apimachinery/pkg/types"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// Source 采集原始拓扑数据（不同实现：nvidia-smi / dcgmi / mock，§7.1）。
type Source interface {
	// Discover 返回 GPU 间的拓扑矩阵与设备信息。
	Discover(ctx context.Context) (*topo.GpuTopology, error)
}

// Writer 将 GpuTopology 写入 NodeGpuTopology CRD（§7.1）。
type Writer interface {
	// Write 以给定节点名写（创建或更新）NodeGpuTopology。
	// generation 由调用方维护；返回值表示本次写入是否实际发生（CAS 语义）。
	Write(ctx context.Context, nodeName string, topology *topo.GpuTopology, generation int64) (bool, error)
	// Get 读取某节点的 NodeGpuTopology 当前状态（供对账）。
	Get(ctx context.Context, name types.NamespacedName) (*GpuTopologyView, error)
}

// GpuTopologyView 是 Writer 返回的轻量视图（避免把 API 对象暴露给 Source 层）。
type GpuTopologyView struct {
	// Generation 当前 generation。
	Generation int64
	// Healthy 是否健康。
	Healthy bool
	// Allocations gpuID -> podUID 的分配映射（对账用）。
	Allocations map[string]string
}
