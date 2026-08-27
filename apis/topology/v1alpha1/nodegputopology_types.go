// Package v1alpha1 定义 NodeGpuTopology 的 API 类型。
//
// API Group: topology.topogang.io
// Version:   v1alpha1
//
// NodeGpuTopology 由 topo-agent 周期采集写入，描述单个 GPU 节点的物理互联拓扑，
// 供调度器做拓扑感知决策（NVLink 域划分、跨域惩罚打分等）。
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LinkType 表示两 GPU 之间的互联链路类型（§6.2 / §7.1）。
type LinkType string

const (
	// LinkNVLink NVLink（NV1/NV2/NV3 统一，权重按带宽）。
	LinkNVLink LinkType = "NVLink"
	// LinkNVSwitch 经 NVSwitch 互联。
	LinkNVSwitch LinkType = "NVSwitch"
	// LinkPIX 同 PCIe Switch。
	LinkPIX LinkType = "PIX"
	// LinkPHB 同 Root Complex 不同 Switch。
	LinkPHB LinkType = "PHB"
	// LinkSYS 跨主机总线（NUMA 间）。
	LinkSYS LinkType = "SYS"
)

// SourceType 表示拓扑数据来源。
type SourceType string

const (
	// SourceNvidiaSMI 来自 nvidia-smi topo -m。
	SourceNvidiaSMI SourceType = "nvidia-smi"
	// SourceDCGM 来自 dcgmi topo -g。
	SourceDCGM SourceType = "dcgmi"
	// SourceMock 模拟数据源（无 GPU 环境调试用）。
	SourceMock SourceType = "mock"
)

// GpuAllocation 记录某 GPU 当前被分配的 Pod（仅对账用，非调度器记账写入源，§7.3.3）。
type GpuAllocation struct {
	// podUID 占用 Pod 的 UID。
	// +optional
	PodUID string `json:"podUID,omitempty"`
	// podName 占用 Pod 名称。
	// +optional
	PodName string `json:"podName,omitempty"`
	// namespace 占用 Pod 命名空间。
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// GpuPeer 表示某 GPU 与对端 GPU 的一条链路。
type GpuPeer struct {
	// gpuId 对端 GPU ID。
	GpuID string `json:"gpuId"`
	// linkType 链路类型。
	LinkType LinkType `json:"linkType"`
	// linkSpeed 链路带宽（GB/s，双向）。
	// +optional
	LinkSpeed float64 `json:"linkSpeed,omitempty"`
}

// Gpu 描述节点上一张 GPU（§6.2）。
type Gpu struct {
	// id 物理 GPU UUID / PCI 地址。
	ID string `json:"id"`
	// index 节点内 GPU 索引。
	Index int32 `json:"index"`
	// model GPU 型号。
	// +optional
	Model string `json:"model,omitempty"`
	// nvlinkDomain 所属 NVLink 域 ID（与 spec.domains 交叉校验，s6 修订）。
	// +optional
	NvlinkDomain string `json:"nvlinkDomain,omitempty"`
	// numaNode 预留：NUMA 节点（v2 纳入打分）。
	// +optional
	NumaNode int32 `json:"numaNode,omitempty"`
	// allocatedTo 由 topo-agent 观测 Pod annotation 回填，仅对账用。
	// +optional
	AllocatedTo *GpuAllocation `json:"allocatedTo,omitempty"`
	// peers 与该 GPU 互联的对端（只列有链路/对端）。
	// +optional
	Peers []GpuPeer `json:"peers,omitempty"`
}

// Domain 描述一个 NVLink 域（§6.2，权威聚合视图，s6 修订）。
type Domain struct {
	// id 域 ID。
	ID string `json:"id"`
	// gpuIndexes 域内 GPU 索引集合。
	GpuIndexes []int32 `json:"gpuIndexes"`
	// intraBandwidth 域内链路带宽（GB/s）。
	// +optional
	IntraBandwidth float64 `json:"intraBandwidth,omitempty"`
}

// NodeGpuTopologySpec 定义节点 GPU 拓扑的期望数据（§6.2）。
type NodeGpuTopologySpec struct {
	// nodeName 所属节点名。
	NodeName string `json:"nodeName"`
	// generation 递增版本，供调度器判断缓存是否过期（§7.3.1 预检缓存 key）。
	Generation int64 `json:"generation"`
	// source 数据来源。
	// +optional
	Source SourceType `json:"source,omitempty"`
	// gpus GPU 列表。
	Gpus []Gpu `json:"gpus"`
	// domains NVLink 域权威聚合视图（§6.2 / §8.1）。
	Domains []Domain `json:"domains"`
}

// NodeGpuTopologyStatus 定义节点拓扑的健康与观测状态（§7.1 T2 修订）。
type NodeGpuTopologyStatus struct {
	// observedGeneration 已观测的 spec 版本。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// lastHeartbeat 最近一次采集心跳。
	// +optional
	LastHeartbeat *metav1.Time `json:"lastHeartbeat,omitempty"`
	// healthy 节点拓扑是否健康。false 时分两级处置：
	//   心跳过期 -> 完全停止新分配（Filter 不返回该节点）；
	//   心跳正常但数据缺失 -> 仅按数量过滤、不选卡（§7.1 / §9.3）。
	// +optional
	Healthy *bool `json:"healthy,omitempty"`
	// error 最近一次采集错误信息。
	// +optional
	Error string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeGpuTopology 描述单个 GPU 节点的物理拓扑（§6.2）。
type NodeGpuTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodeGpuTopologySpec   `json:"spec,omitempty"`
	Status NodeGpuTopologyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeGpuTopologyList 是 NodeGpuTopology 的列表。
type NodeGpuTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeGpuTopology `json:"items"`
}

// 标签键常量（§6.2）。
const (
	// NodeNameLabel 标记 NodeGpuTopology 所属节点。
	NodeNameLabel = "topology.topogang.io/node"
)
