// topo-agent 入口：周期采集节点 GPU 拓扑并写入 NodeGpuTopology（§7.1）。
//
// 用法：
//
//	topo-agent --node-name=node-a --source=mock --mock-spec=8-2
//	topo-agent --node-name=node-a --source=nvidia-smi
//	topo-agent --node-name=node-a --source=mock --mock-spec=8-2 --writer=memory
//
// 默认 source=mock、writer=memory，便于无 GPU 环境验证采集→域划分→写入链路。
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"github.com/chenxihui/TopoGang/pkg/agent"
	"github.com/chenxihui/TopoGang/pkg/topo"
)

func main() {
	var (
		nodeName      = flag.String("node-name", "", "this node name (required)")
		sourceType    = flag.String("source", "mock", "data source: mock | nvidia-smi")
		mockSpec      = flag.String("mock-spec", "8-2", "mock topology spec: 8-1 | 8-2 | 4-1 | overlap | none")
		writerType    = flag.String("writer", "memory", "writer: memory (in-cluster CRD writer is TBD)")
		interval      = flag.Duration("interval", 60*time.Second, "discover interval")
		strategy      = flag.String("domain-strategy", "clique", "nvlink domain strategy: clique | connected")
	)
	klog.InitFlags(nil)
	flag.Parse()

	if *nodeName == "" {
		klog.Fatalf("--node-name is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	src, err := buildSource(*sourceType, *mockSpec)
	if err != nil {
		klog.Fatalf("build source: %v", err)
	}

	var wr agent.Writer
	switch *writerType {
	case "memory":
		wr = agent.NewInMemoryWriter()
	default:
		klog.Fatalf("unknown writer %q", *writerType)
	}

	ds := topo.DomainClique
	if *strategy == "connected" {
		ds = topo.DomainConnected
	}

	collector := agent.NewCollector(agent.CollectorOptions{
		NodeName:         *nodeName,
		DiscoverInterval: *interval,
		DomainStrategy:   ds,
		Source:           src,
		Writer:           wr,
	})
	collector.Run(ctx)
}

func buildSource(sourceType, mockSpec string) (agent.Source, error) {
	switch sourceType {
	case "mock":
		spec, err := parseMockSpec(mockSpec)
		if err != nil {
			return nil, err
		}
		return agent.NewMockSource(spec, ""), nil
	case "nvidia-smi":
		return agent.NewNvidiaSmiSource(), nil
	default:
		return nil, os.ErrProcessDone // unreachable: replaced below
	}
}

func parseMockSpec(s string) (agent.MockTopologySpec, error) {
	switch s {
	case "8-1":
		return agent.Mock8SingleDomain, nil
	case "8-2":
		return agent.Mock8TwoDomains, nil
	case "4-1":
		return agent.Mock4SingleDomain, nil
	case "overlap":
		return agent.MockOverlapCliques, nil
	case "none":
		return agent.MockNoNVLink, nil
	default:
		return agent.Mock8TwoDomains, nil // 默认 8 卡双域
	}
}
