package topo

import (
	"sort"
)

// FillCandidate 描述 best-fit 决策中的一个候选域及填充信息。
type FillCandidate struct {
	// Domain 域信息。
	Domain Domain
	// FreeGPUs 域内空闲 GPU（已排序）。
	FreeGPUs []int
	// SiblingCount 兄弟 Pod 已占用（位于该域内）的 GPU 数。
	SiblingCount int
	// TotalCapacity 域容量。
	TotalCapacity int
	// CrossDomainRatio 该域的跨域边占比（用于 γ 内聚惩罚）。
	CrossDomainRatio float64
}

// BestFitDomain 返回候选域中最优的一个（依据 domainScore 软打分）。
// 调用方负责传入已通过硬约束（空闲数 ≥ count、无 locked）的候选域。
//
// 这是 Score 与 SelectGPUs 的共享决策函数（§8.1 M2/R4 修订）：打分评估的最优域
// 即 SelectGPUs 实际落地的域，保证"打分选 A 域、实际选 B 域"的二义不发生。
func BestFitDomain(candidates []FillCandidate, params DomainScoreParams) *FillCandidate {
	if len(candidates) == 0 {
		return nil
	}
	best := -1
	bestScore := -1e9
	for i, c := range candidates {
		cd := CandidateDomain{
			Domain:        c.Domain,
			FreeGPUs:      c.FreeGPUs,
			SiblingCount:  c.SiblingCount,
			TotalCapacity: c.TotalCapacity,
		}
		score := params.Evaluate(cd)
		// 内聚度惩罚：γ·跨域边占比
		score -= params.Gamma * c.CrossDomainRatio
		if score > bestScore {
			bestScore = score
			best = i
		} else if score == bestScore && best != -1 {
			// 同分时选空闲 GPU 更多的域（装箱平衡），再选索引更小的（确定性）
			if len(candidates[i].FreeGPUs) > len(candidates[best].FreeGPUs) {
				best = i
			} else if len(candidates[i].FreeGPUs) == len(candidates[best].FreeGPUs) {
				if candidates[i].Domain.ID < candidates[best].Domain.ID {
					best = i
				}
			}
		}
	}
	if best == -1 {
		return nil
	}
	return &candidates[best]
}

// FillCandidatesFromTopo 依据 GpuTopology 与空闲 GPU 索引集合构造候选域，
// 并计算每域的兄弟亲和与跨域边占比。
//
//	domains   : NVLink 域列表（来自 FindNvlinkDomains）。
//	freeByID  : 空闲 GPU 集合（map[gpuIndex]bool）。
//	siblingIDs: 兄弟 Pod 已占用 GPU 索引集合（map[gpuIndex]bool）。
func FillCandidatesFromTopo(g *GpuTopology, domains []Domain, freeByID map[int]bool, siblingIDs map[int]bool) []FillCandidate {
	candidates := make([]FillCandidate, 0, len(domains))
	for _, d := range domains {
		var free []int
		siblingCount := 0
		for _, gi := range d.GPUIndexes {
			if freeByID[gi] {
				free = append(free, gi)
			}
			if siblingIDs[gi] {
				siblingCount++
			}
		}
		if len(free) == 0 {
			continue
		}
		sort.Ints(free)
		candidates = append(candidates, FillCandidate{
			Domain:           d,
			FreeGPUs:         free,
			SiblingCount:     siblingCount,
			TotalCapacity:    len(d.GPUIndexes),
			CrossDomainRatio: CrossDomainRatio(len(d.GPUIndexes), len(g.GPUs)),
		})
	}
	return candidates
}

// SelectGPUsFromDomain 从最优域中取 count 张 GPU；域内空闲不足时返回最大可取的集合
// 与是否满足（供调用方决定是否退化为跨域组合）。
func SelectGPUsFromDomain(best *FillCandidate, count int) ([]int, bool) {
	if best == nil {
		return nil, false
	}
	if len(best.FreeGPUs) < count {
		return best.FreeGPUs, false
	}
	return best.FreeGPUs[:count], true
}
