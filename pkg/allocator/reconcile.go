package allocator

// DriftHandler 处理 AllocationTracker 与 Agent 观测的分配漂移（§7.3.3 N2）。
//
// 漂移分两类处置：
//   - 记账超前（tracker 有记录、Agent 观测空闲）：以调度器事件为准，核对后清理记账。
//   - 物理占用超前（Agent 观测到 GPU 被 Pod 占用、tracker 认为空闲）：物理真相为准，
//     将该 GPU 标记 locked（安全阀），防超卖；不篡改记账。
type DriftHandler interface {
	// Release 释放 tracker 中某 GPU 的记账（记账超前时核对清理）。
	Release(node, gpuID, podUID string) bool
	// LockGPU 封锁某 GPU（物理占用超前时，安全阀）。
	LockGPU(node, gpuID string)
	// IsAllocated 判断 tracker 是否认为该 GPU 已分配。
	IsAllocated(node, gpuID string) bool
	// Owner 返回 tracker 中该 GPU 的属主。
	Owner(node, gpuID string) string
}

// AgentAllocation 是 agent 观测到的物理占用。
type AgentAllocation struct {
	// Node 节点名。
	Node string
	// GPU gpuID。
	GPU string
	// OccupiedByPod 观测到的占用 Pod UID（空表示空闲）。
	OccupiedByPod string
}

// DriftAction 是一次对账处置的动作。
type DriftAction struct {
	// Node 节点。
	Node string
	// GPU gpuID。
	GPU string
	// Type 处置类型：cleanup（记账超前清理）或 lock（物理占用超前封锁）。
	Type string
	// Detail 说明。
	Detail string
}

const (
	// DriftTypeCleanup 记账超前：tracker 有记录但物理空闲，核对后清理。
	DriftTypeCleanup = "tracker-ahead-cleanup"
	// DriftTypeLock 物理占用超前：agent 观测占用但 tracker 空闲，locked 安全阀。
	DriftTypeLock = "agent-ahead-lock"
)

// ReconcileDrifts 对单节点执行一次对账，返回处置动作（§7.3.3 N2）。
func ReconcileDrifts(handler DriftHandler, observed []AgentAllocation) []DriftAction {
	var actions []DriftAction

	// 物理观测到的 GPU 集合
	observedGPU := map[string]AgentAllocation{}
	for _, o := range observed {
		observedGPU[o.Node+"|"+o.GPU] = o
	}

	// 类型①：tracker 记账超前（tracker 有、agent 空闲）-> 清理
	for _, o := range observed {
		if o.OccupiedByPod == "" && handler.IsAllocated(o.Node, o.GPU) {
			owner := handler.Owner(o.Node, o.GPU)
			handler.Release(o.Node, o.GPU, owner)
			actions = append(actions, DriftAction{
				Node: o.Node, GPU: o.GPU, Type: DriftTypeCleanup,
				Detail: "tracker records allocation but agent observes free",
			})
		}
	}

	// 类型②：物理占用超前（agent 观测占用、tracker 空闲）-> locked
	// 遍历所有 agent 观测的占用
	for _, o := range observed {
		if o.OccupiedByPod != "" && !handler.IsAllocated(o.Node, o.GPU) {
			handler.LockGPU(o.Node, o.GPU)
			actions = append(actions, DriftAction{
				Node: o.Node, GPU: o.GPU, Type: DriftTypeLock,
				Detail: "agent observes physical occupancy not tracked by scheduler",
			})
		}
	}
	return actions
}
