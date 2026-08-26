package topo

import (
	"testing"
)

func TestBestFitDomain_FavorsSiblingDomain(t *testing.T) {
	// 两个候选域，域A 有 1 个兄弟 GPU，域B 无兄弟；容量富余相近时 A 应胜出
	g := buildTestTopo() // 8 卡双域
	domains := FindNvlinkDomains(g, DomainClique)

	freeAll := map[int]bool{}
	for i := 0; i < 8; i++ {
		freeAll[i] = true
	}
	siblingInDomainA := map[int]bool{0: true} // 兄弟占域1 的 GPU0

	cands := FillCandidatesFromTopo(g, domains, freeAll, siblingInDomainA)
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(cands))
	}

	params := DefaultDomainScoreParams()
	best := BestFitDomain(cands, params)
	if best == nil {
		t.Fatal("expected a best-fit domain")
	}
	// 兄弟在域1（GPU 0-3）
	if best.Domain.GPUIndexes[0] != 0 {
		t.Fatalf("expected best domain to contain sibling GPU 0, got %+v", best.Domain)
	}
}

func TestBestFitDomain_FavorsMoreCapacity(t *testing.T) {
	// 容量富余：一个域较满、一个域较空，倾向选择较空域（装箱平衡）
	g := buildTestTopo()
	domains := FindNvlinkDomains(g, DomainClique)

	freeByID := map[int]bool{
		0: true, 1: true, 2: true, // 域1 仅剩 3 卡
		4: true, 5: true, 6: true, 7: true, // 域2 剩 4 卡
	}
	cands := FillCandidatesFromTopo(g, domains, freeByID, nil)
	params := DefaultDomainScoreParams()
	best := BestFitDomain(cands, params)
	if best == nil {
		t.Fatal("expected a best-fit domain")
	}
	if best.Domain.GPUIndexes[0] != 4 {
		t.Fatalf("expected best domain to be domain 2 (GPU4-7), got %+v", best.Domain)
	}
}

func TestBestFitDomain_EmptyCandidates(t *testing.T) {
	params := DefaultDomainScoreParams()
	if got := BestFitDomain(nil, params); got != nil {
		t.Fatalf("expected nil for empty candidates, got %+v", got)
	}
}

func TestSelectGPUsFromDomain_Sufficient(t *testing.T) {
	best := &FillCandidate{FreeGPUs: []int{0, 1, 2, 3}}
	gpus, ok := SelectGPUsFromDomain(best, 4)
	if !ok || len(gpus) != 4 {
		t.Fatalf("expected 4 GPUs, got %v ok=%v", gpus, ok)
	}
}

func TestSelectGPUsFromDomain_Insufficient(t *testing.T) {
	best := &FillCandidate{FreeGPUs: []int{0, 1, 2}}
	gpus, ok := SelectGPUsFromDomain(best, 4)
	if ok {
		t.Fatalf("expected insufficient, got ok=true")
	}
	if len(gpus) != 3 {
		t.Fatalf("expected 3 available GPUs returned, got %v", gpus)
	}
}

func TestDomainScoreParams_Evaluate(t *testing.T) {
	p := DefaultDomainScoreParams()
	// 空闲满额的域，容量富余度 1.0
	full := CandidateDomain{FreeGPUs: []int{0, 1, 2, 3}, TotalCapacity: 4}
	partial := CandidateDomain{FreeGPUs: []int{0}, TotalCapacity: 4}
	if p.Evaluate(full) <= p.Evaluate(partial) {
		t.Fatalf("full-capacity domain should score higher than partial")
	}
}
