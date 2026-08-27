package metrics

// DerivedSnapshot 是一次指标推导的结果（§11.2 命中率/碎片率/跨域比）。
type DerivedSnapshot struct {
	// AffinityHitRate 拓扑命中率：同域调度占比 [0,1]。
	AffinityHitRate float64
	// FragmentRate 节点 GPU 碎片率：空闲但不可整域分配占比 [0,1]。
	FragmentRate float64
	// CrossDomainRatio 跨域调度占比 [0,1]。
	CrossDomainRatio float64
}

// NodeFragState 描述一个节点的碎片统计（推导输入）。
type NodeFragState struct {
	// TotalGPUs 节点 GPU 总数。
	TotalGPUs int
	// FreeGPUs 空闲 GPU 总数。
	FreeGPUs int
	// FreeByDomain 各域空闲 GPU 数（键为域 ID）。
	FreeByDomain map[string]int
}

// Derive 计算聚合指标（§11.2）。
//
//   - AffinityHitRate：同域调度占比。由调用方以"同域调度数/总调度数"传入
//     sameDomainCount / totalScheduled（若 totalScheduled=0 返回 0）。
//   - FragmentRate：碎片率 = 各节点"空闲但无法整域分配"的 GPU 数 / 空闲总数。
//     无法整域分配 = 该域空闲数 < 典型请求数（这里以域容量 1/2 为界，简化）。
//   - CrossDomainRatio = 1 - AffinityHitRate。
func Derive(sameDomainCount, totalScheduled int, nodes []NodeFragState) DerivedSnapshot {
	snap := DerivedSnapshot{}
	if totalScheduled > 0 {
		snap.AffinityHitRate = float64(sameDomainCount) / float64(totalScheduled)
	}
	snap.CrossDomainRatio = 1 - snap.AffinityHitRate

	// 碎片率：空闲但不可整域分配（§11.2）
	// 判定：空闲但所在域无法整域满足典型 2 卡请求的 GPU 视为碎片。
	totalFree := 0
	fragmented := 0
	for _, n := range nodes {
		for _, free := range n.FreeByDomain {
			totalFree += free
			if free > 0 && free < 2 {
				fragmented += free
			}
		}
	}
	if totalFree > 0 {
		snap.FragmentRate = float64(fragmented) / float64(totalFree)
	}
	return snap
}
