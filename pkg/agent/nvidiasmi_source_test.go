package agent

import (
	"context"
	"strings"
	"testing"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

const sampleTopoOutput = `       GPU0   GPU1   GPU2   GPU3   GPU4   GPU5   GPU6   GPU7
GPU0    X     NV1    NV1    NV1    SYS    SYS    SYS    SYS
GPU1    NV1    X     NV1    NV1    SYS    SYS    SYS    SYS
GPU2    NV1    NV1     X     NV1    SYS    SYS    SYS    SYS
GPU3    NV1    NV1    NV1     X     SYS    SYS    SYS    SYS
GPU4    SYS    SYS    SYS    SYS     X     NV1    NV1    NV1
GPU5    SYS    SYS    SYS    SYS    NV1     X     NV1    NV1
GPU6    SYS    SYS    SYS    SYS    NV1    NV1     X     NV1
GPU7    SYS    SYS    SYS    SYS    NV1    NV1    NV1     X`

func TestNvidiaSmiSource_Discover(t *testing.T) {
	src := NewNvidiaSmiSource()
	// 注入假 exec：返回样例输出
	src.Exec = func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "nvidia-smi" {
			t.Fatalf("unexpected command %q", name)
		}
		if strings.Join(args, " ") != "topo -m" {
			t.Fatalf("unexpected args %v", args)
		}
		return []byte(sampleTopoOutput), nil
	}

	g, err := src.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}
	if len(g.GPUs) != 8 {
		t.Fatalf("expected 8 GPUs, got %d", len(g.GPUs))
	}
	// 校验域划分：期望两个 4 卡域
	domains := topo.FindNvlinkDomains(g, topo.DomainClique)
	if len(domains) != 2 {
		t.Fatalf("expected 2 domains, got %d: %+v", len(domains), domains)
	}
	// 校验链路数：NVLink 域内 (4*3/2)*2=12，域间 SYS 4*4=16，共 28
	if len(g.Links) != 28 {
		t.Fatalf("expected 28 links, got %d", len(g.Links))
	}
	// 校验 GPU0-GPU3 为 NVLink，GPU0-GPU4 为 SYS
	if lt := g.LinkTypeOf(0, 1); lt != topo.LinkNVLink {
		t.Fatalf("expected NVLink between 0-1, got %s", lt)
	}
	if lt := g.LinkTypeOf(0, 4); lt != topo.LinkSYS {
		t.Fatalf("expected SYS between 0-4, got %s", lt)
	}
}

func TestParseLinkType(t *testing.T) {
	cases := map[string]topo.LinkType{
		"X":       "",
		"NV1":     topo.LinkNVLink,
		"NV3":     topo.LinkNVLink,
		"PIX":     topo.LinkPIX,
		"PHB":     topo.LinkPHB,
		"SYS":     topo.LinkSYS,
		"NVSWITCH": topo.LinkNVSwitch,
		"UNKNOWN": "",
	}
	for in, want := range cases {
		if got := parseLinkType(in); got != want {
			t.Errorf("parseLinkType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMockSource_VariousSpecs(t *testing.T) {
	check := func(spec MockTopologySpec, wantGPUs, wantDomains int) {
		src := NewMockSource(spec, "node-mock")
		g, err := src.Discover(context.Background())
		if err != nil {
			t.Fatalf("spec %v Discover failed: %v", spec, err)
		}
		if len(g.GPUs) != wantGPUs {
			t.Fatalf("spec %v: want %d GPUs, got %d", spec, wantGPUs, len(g.GPUs))
		}
		domains := topo.FindNvlinkDomains(g, topo.DomainClique)
		if len(domains) != wantDomains {
			t.Fatalf("spec %v: want %d domains, got %d (%+v)", spec, wantDomains, len(domains), domains)
		}
	}

	check(Mock8SingleDomain, 8, 1)
	check(Mock8TwoDomains, 8, 2)
	check(Mock4SingleDomain, 4, 1)
	check(MockNoNVLink, 4, 0)
}

func TestMockSource_Overlap(t *testing.T) {
	src := NewMockSource(MockOverlapCliques, "node-mock")
	g, err := src.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}
	domains := topo.FindNvlinkDomains(g, topo.DomainClique)
	if len(domains) < 2 {
		t.Fatalf("expected overlapping cliques, got %+v", domains)
	}
}
