package topo

import (
	"reflect"
	"testing"
)

// buildTestTopo 构造一个 8 卡双域（4+4 NVLink，域间 PIX）的拓扑。
func buildTestTopo() *GpuTopology {
	g := &GpuTopology{NodeName: "node-a"}
	for i := 0; i < 8; i++ {
		g.GPUs = append(g.GPUs, &Gpu{Index: i, ID: "GPU" + string(rune('0'+i))})
	}
	complete := func(a, b int, lt LinkType) {
		g.Links = append(g.Links, &Link{A: a, B: b, LinkType: lt, Bandwidth: LinkBandwidth(lt)})
	}
	// 域1: 0-3 全互联 NVLink
	for a := 0; a < 4; a++ {
		for b := a + 1; b < 4; b++ {
			complete(a, b, LinkNVLink)
		}
	}
	// 域2: 4-7 全互联 NVLink
	for a := 4; a < 8; a++ {
		for b := a + 1; b < 8; b++ {
			complete(a, b, LinkNVLink)
		}
	}
	// 域间 PIX
	for a := 0; a < 4; a++ {
		for b := 4; b < 8; b++ {
			complete(a, b, LinkPIX)
		}
	}
	return g
}

func TestFindNvlinkDomains_Clique_TwoDomains(t *testing.T) {
	g := buildTestTopo()
	domains := FindNvlinkDomains(g, DomainClique)

	// 期望两个大小为 4 的域
	if len(domains) != 2 {
		t.Fatalf("expected 2 domains, got %d: %+v", len(domains), domains)
	}
	got := make([][]int, 0, 2)
	for _, d := range domains {
		got = append(got, d.GPUIndexes)
	}
	expect := [][]int{{0, 1, 2, 3}, {4, 5, 6, 7}}
	if !reflect.DeepEqual(got, expect) {
		t.Fatalf("expected domains %v, got %v", expect, got)
	}
}

func TestFindNvlinkDomains_Connected_TwoDomains(t *testing.T) {
	g := buildTestTopo()
	domains := FindNvlinkDomains(g, DomainConnected)
	if len(domains) != 2 {
		t.Fatalf("expected 2 domains, got %d: %+v", len(domains), domains)
	}
}

func TestFindNvlinkDomains_SingleDomain(t *testing.T) {
	g := &GpuTopology{}
	for i := 0; i < 4; i++ {
		g.GPUs = append(g.GPUs, &Gpu{Index: i, ID: "GPU" + string(rune('0'+i))})
	}
	for a := 0; a < 4; a++ {
		for b := a + 1; b < 4; b++ {
			g.Links = append(g.Links, &Link{A: a, B: b, LinkType: LinkNVLink, Bandwidth: 600})
		}
	}
	domains := FindNvlinkDomains(g, DomainClique)
	if len(domains) != 1 || len(domains[0].GPUIndexes) != 4 {
		t.Fatalf("expected 1 domain of 4, got %+v", domains)
	}
}

func TestFindNvlinkDomains_OverlapCliques(t *testing.T) {
	// GPU 0-3 全互联，GPU 4 与 0,1,2 互联 => 极大团 {0,1,2,3} 与 {0,1,2,4}
	g := &GpuTopology{}
	for i := 0; i < 5; i++ {
		g.GPUs = append(g.GPUs, &Gpu{Index: i, ID: "GPU" + string(rune('0'+i))})
	}
	link := func(a, b int) {
		g.Links = append(g.Links, &Link{A: a, B: b, LinkType: LinkNVLink, Bandwidth: 600})
	}
	link(0, 1)
	link(0, 2)
	link(0, 3)
	link(1, 2)
	link(1, 3)
	link(2, 3)
	link(0, 4)
	link(1, 4)
	link(2, 4)

	domains := FindNvlinkDomains(g, DomainClique)
	// 应有大小为 4 的极大团 {0,1,2,3} 与 {0,1,2,4}，且排序后 3 在 4 前
	if len(domains) < 2 {
		t.Fatalf("expected overlapping cliques, got %+v", domains)
	}
	first := domains[0]
	if len(first.GPUIndexes) != 4 {
		t.Fatalf("expected largest clique of size 4, got %+v", first)
	}
	if !reflect.DeepEqual(first.GPUIndexes, []int{0, 1, 2, 3}) {
		t.Fatalf("expected largest clique {0,1,2,3}, got %v", first.GPUIndexes)
	}
}

func TestCrossDomainRatio(t *testing.T) {
	// 8 卡中单域 4 卡：域内边 6，全网边 28，跨域边 22，比值 22/28≈0.786
	if got := CrossDomainRatio(4, 8); got < 0.78 || got > 0.79 {
		t.Fatalf("expected ~0.786, got %f", got)
	}
	// 全互联单域：无跨域
	if got := CrossDomainRatio(8, 8); got != 0 {
		t.Fatalf("expected 0 for full mesh, got %f", got)
	}
}

func TestDefaultDomainScoreParams(t *testing.T) {
	p := DefaultDomainScoreParams()
	if p.Alpha != 0.5 || p.Beta != 0.3 || p.Gamma != 0.2 {
		t.Fatalf("unexpected default params: %+v", p)
	}
}

func TestNormalizedLinkCost(t *testing.T) {
	if c := NormalizedLinkCost(LinkNVLink); c >= NormalizedLinkCost(LinkPIX) {
		t.Fatalf("NVLink cost should be lower than PIX")
	}
	if c := NormalizedLinkCost(LinkSYS); c <= NormalizedLinkCost(LinkPIX) {
		t.Fatalf("SYS cost should be higher than PIX")
	}
	if c := NormalizedLinkCost(LinkNVLink); c > 1.0 || c < 0 {
		t.Fatalf("cost out of range: %f", c)
	}
}
