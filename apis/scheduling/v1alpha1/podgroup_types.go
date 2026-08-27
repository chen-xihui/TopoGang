// Package v1alpha1 定义 PodGroup 的 API 类型。
//
// API Group: scheduling.topogang.io
// Version:   v1alpha1
//
// PodGroup 是 TopoGang 的 Gang 调度核心 CRD，声明一组"必须整组一起调度"的
// 训练 Pod（如 8 Worker DDP / DeepSpeed）。调度器保证 `minMember` 全部满足才
// 原子放行（All-or-Nothing）。
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodGroupPhase 表示 PodGroup 的当前调度阶段。
type PodGroupPhase string

const (
	// PodGroupPending 组已创建，尚未开始调度（或超时回退后重新排队）。
	PodGroupPending PodGroupPhase = "Pending"
	// PodGroupPreScheduling 首个成员已进入调度队列，等待组级预检。
	PodGroupPreScheduling PodGroupPhase = "PreScheduling"
	// PodGroupScheduling Permit 开始等待成员凑齐。
	PodGroupScheduling PodGroupPhase = "Scheduling"
	// PodGroupRunning 组内 minMember 已原子放行，任务运行中。
	PodGroupRunning PodGroupPhase = "Running"
	// PodGroupSucceeded 组内全部成员成功退出。
	PodGroupSucceeded PodGroupPhase = "Succeeded"
	// PodGroupFailed 组内存在终态 Failed Pod 且持续超过观察窗口、无新成员创建。
	PodGroupFailed PodGroupPhase = "Failed"
	// PodGroupUnknown 控制器失联（leader 变更且 status 长时间无更新），仅告警。
	PodGroupUnknown PodGroupPhase = "Unknown"
)

// GpuDomainPolicy 表示拓扑需求声明（§6.1）。
type GpuDomainPolicy string

const (
	// GpuDomainNone 尽力而为（默认）：能放同域就同域，否则跨域。
	GpuDomainNone GpuDomainPolicy = "none"
	// GpuDomainNvlink 强制：必须存在单个 NVLink 域能容纳全部 GPU（M3 实现）。
	GpuDomainNvlink GpuDomainPolicy = "nvlink"
	// GpuDomainPCIe 预留（本期不实现）。
	GpuDomainPCIe GpuDomainPolicy = "pcie"
)

// PodGroupConditionType 表示 PodGroup 的条件类型。
type PodGroupConditionType string

const (
	// PodGroupScheduled 组是否已成功调度（原子放行）。
	PodGroupScheduled PodGroupConditionType = "Scheduled"
)

// TopologyPolicy 声明组对 GPU 拓扑的需求（§6.1 / §3.2）。
type TopologyPolicy struct {
	// gpuDomain 声明域策略：none | nvlink | pcie（本期实现 nvlink）。
	// +optional
	GpuDomain GpuDomainPolicy `json:"gpuDomain,omitempty"`
}

// PodGroupSpec 定义 PodGroup 的期望状态（§6.1）。
type PodGroupSpec struct {
	// minMember 组内最少成功调度成员数，调度器保证"minMember 全部满足才放行"。
	// 该值必须为 0 以上；为 0 表示非严格 Gang（预留）。
	MinMember int32 `json:"minMember"`

	// scheduleTimeoutSeconds 从组开始排队到整组调度的最大等待时间（秒），
	// 超时整组回退重排。默认 600。
	// +optional
	ScheduleTimeoutSeconds *int32 `json:"scheduleTimeoutSeconds,omitempty"`

	// maxSchedulingBatch 组内同时进入"调度 cycle 进行中（PreFilter~Permit 提交）"
	// 的最大成员数；Permit 返回 Wait 后不计入名额（§8.4，N1）。默认 4。
	// +optional
	MaxSchedulingBatch *int32 `json:"maxSchedulingBatch,omitempty"`

	// queue 队列名（预留多队列策略，本期为 FIFO）。
	// +optional
	Queue string `json:"queue,omitempty"`

	// priorityClassName 用于跨组抢占排序的优先级类。
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// topologyPolicy 可选的拓扑需求声明（§6.1）。
	// +optional
	TopologyPolicy *TopologyPolicy `json:"topologyPolicy,omitempty"`
}

// PodGroupCondition 描述 PodGroup 的一个状态条件。
type PodGroupCondition struct {
	Type               PodGroupConditionType `json:"type"`
	Status             corev1.ConditionStatus `json:"status"`
	Reason             string                `json:"reason,omitempty"`
	Message            string                `json:"message,omitempty"`
	LastTransitionTime metav1.Time           `json:"lastTransitionTime,omitempty"`
}

// PodGroupStatus 定义 PodGroup 的观测状态（§6.1）。
type PodGroupStatus struct {
	// phase 当前阶段。
	// +optional
	Phase PodGroupPhase `json:"phase,omitempty"`

	// scheduled 已调度成员数。
	// +optional
	Scheduled int32 `json:"scheduled,omitempty"`

	// scheduledByGroup 已通过 Permit 原子放行的成员数（§9.1 released-generation 闭环）。
	// +optional
	ScheduledByGroup int32 `json:"scheduledByGroup,omitempty"`

	// running 运行中成员数。
	// +optional
	Running int32 `json:"running,omitempty"`

	// succeeded 成功成员数。
	// +optional
	Succeeded int32 `json:"succeeded,omitempty"`

	// failed 失败成员数。
	// +optional
	Failed int32 `json:"failed,omitempty"`

	// conditions 状态条件。
	// +optional
	Conditions []PodGroupCondition `json:"conditions,omitempty"`

	// lastScheduleTime 最近一次整组放行时间（供观察窗口判定，§9.1）。
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// observedGeneration 已观测的 spec 版本。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroup 是一组需要原子调度的 Pod 的声明（§6.1）。
type PodGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PodGroupSpec   `json:"spec,omitempty"`
	Status PodGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroupList 是 PodGroup 的列表。
type PodGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PodGroup `json:"items"`
}

// 注解键常量（§6.1 / §7.3.1，命名统一，s7 修订）。
const (
	// GroupNameAnnotation 标记 Pod 所属的 PodGroup（调度器据此识别组成员）。
	GroupNameAnnotation = "scheduling.topogang.io/group-name"
	// GroupIndexAnnotation 组内 Pod 序号，用于 QueueSort 确定性排序。
	GroupIndexAnnotation = "topogang.io/group-index"
	// GPUUUIDsAnnotation 调度器 PreBind 阶段写入：本 Pod 被分配的 GPU 列表。
	GPUUUIDsAnnotation = "topogang.io/gpu-uuids"
	// ReleasedGenerationAnnotation 调度器"整组放行事件"序号（CAS 递增），
	// Controller watch 后回写 status.scheduledByGroup（§9.1）。
	ReleasedGenerationAnnotation = "scheduling.topogang.io/released-generation"
	// ResetAnnotation 用户显式重置组（Failed -> Pending）。
	ResetAnnotation = "scheduling.topogang.io/reset"
)
