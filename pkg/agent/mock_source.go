package agent

import (
	"context"
	"fmt"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// MockTopologySpec 定义 mock 源的拓扑形态（§10.2 拓扑模拟器）。
type MockTopologySpec int

const (
	// Mock8SingleDomain 8 卡全互联（NVSwitch）单域。
	Mock8SingleDomain MockTopologySpec = iota
	// Mock8TwoDomains 8 卡双域（4+4）。
	Mock8TwoDomains
	// Mock4SingleDomain 4 卡全互联单域。
	Mock4SingleDomain
	// MockOverlapCliques 部分互联（极大团重叠）。
	MockOverlapCliques
	// MockNoNVLink 无 NVLink（全 PIX/SYS）。
	MockNoNVLink
)

// MockSource 提供可复现的拓扑数据，用于无 GPU 环境调试与单元/集成测试（§7.1/§10.2）。
type MockSource struct {
	// Spec 拓扑形态。
	Spec MockTopologySpec
	// NodeName 模拟节点名。
	NodeName string
}

// NewMockSource 构造指定形态的 mock 源。
func NewMockSource(spec MockTopologySpec, nodeName string) *MockSource {
	return &MockSource{Spec: spec, NodeName: nodeName}
}

// Discover 依据 Spec 返回拓扑图。
func (m *MockSource) Discover(_ context.Context) (*topo.GpuTopology, error) {
	g := &topo.GpuTopology{NodeName: m.NodeName}
	switch m.Spec {
	case Mock8SingleDomain:
		buildComplete(g, 8, topo.LinkNVLink)
	case Mock8TwoDomains:
		buildComplete(g, 4, topo.LinkNVLink)
		buildComplete(g, 4, topo.LinkNVLink) // 第二域从索引 4 开始
		// 域间 PIX
		for a := 0; a < 4; a++ {
			for b := 4; b < 8; b++ {
				g.Links = append(g.Links, &topo.Link{A: a, B: b, LinkType: topo.LinkPIX, Bandwidth: topo.LinkBandwidth(topo.LinkPIX)})
			}
		}
	case Mock4SingleDomain:
		buildComplete(g, 4, topo.LinkNVLink)
	case MockOverlapCliques:
		// GPU 0-3 全互联，GPU 4 与 0,1,2 互联 => 极大团 {0,1,2,3} 与 {0,1,2,4}
		buildComplete(g, 4, topo.LinkNVLink)
		g.GPUs = append(g.GPUs, &topo.Gpu{Index: 4, ID: "GPU4"})
		for _, b := range []int{0, 1, 2} {
			g.Links = append(g.Links, &topo.Link{A: 4, B: b, LinkType: topo.LinkNVLink, Bandwidth: topo.LinkBandwidth(topo.LinkNVLink)})
		}
		for _, b := range []int{0, 1, 2} {
			g.Links = append(g.Links, &topo.Link{A: 3, B: b, LinkType: topo.LinkNVLink, Bandwidth: topo.LinkBandwidth(topo.LinkNVLink)})
		}
		for a := 0; a < 4; a++ {
			g.Links = append(g.Links, &topo.Link{A: 4, B: a, LinkType: topo.LinkPIX, Bandwidth: topo.LinkBandwidth(topo.LinkPIX)})
		}
	case MockNoNVLink:
		buildComplete(g, 4, topo.LinkPIX)
	default:
		return nil, fmt.Errorf("unknown mock spec %d", m.Spec)
	}
	return g, nil
}

// buildComplete 在现有 GPU 基础上追加 count 张全互联 GPU。
func buildComplete(g *topo.GpuTopology, count int, lt topo.LinkType) {
	start := len(g.GPUs)
	for i := start; i < start+count; i++ {
		g.GPUs = append(g.GPUs, &topo.Gpu{Index: i, ID: fmt.Sprintf("GPU%d", i)})
	}
	for a := start; a < start+count; a++ {
		for b := a + 1; b < start+count; b++ {
			g.Links = append(g.Links, &topo.Link{A: a, B: b, LinkType: lt, Bandwidth: topo.LinkBandwidth(lt)})
		}
	}
}
