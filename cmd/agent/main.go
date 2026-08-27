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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/chenxihui/TopoGang/apis"
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

	ds := topo.DomainClique
	if *strategy == "connected" {
		ds = topo.DomainConnected
	}

	switch *writerType {
	case "memory":
		wr := agent.NewInMemoryWriter()
		runCollector(ctx, *nodeName, src, *interval, ds, wr)
	case "cluster":
		// 真实集群模式：controller-runtime 对接 NodeGpuTopology CRD + Pod 对账回填
		runClusterAgent(ctx, *nodeName, src, *interval, ds)
	default:
		klog.Fatalf("unknown writer %q", *writerType)
	}
}

// runCollector 以内存 Writer 运行采集（无集群调试模式）。
func runCollector(ctx context.Context, nodeName string, src agent.Source, interval time.Duration, ds topo.DomainStrategy, wr agent.Writer) {
	collector := agent.NewCollector(agent.CollectorOptions{
		NodeName:         nodeName,
		DiscoverInterval: interval,
		DomainStrategy:   ds,
		Source:           src,
		Writer:           wr,
	})
	collector.Run(ctx)
}

// runClusterAgent 启动集群模式 agent：ClusterWriter + PodReconciler（§7.1 对账闭环）。
func runClusterAgent(ctx context.Context, nodeName string, src agent.Source, interval time.Duration, ds topo.DomainStrategy) {
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: agentScheme()})
	if err != nil {
		klog.Fatalf("unable to start agent manager: %v", err)
	}
	wr := agent.NewClusterWriter(mgr.GetClient())

	// topo-agent 采集器（周期采集 -> 写 CRD）
	collector := agent.NewCollector(agent.CollectorOptions{
		NodeName:         nodeName,
		DiscoverInterval: interval,
		DomainStrategy:   ds,
		Source:           src,
		Writer:           wr,
	})
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		go collector.Run(ctx)
		return nil
	})); err != nil {
		klog.Fatalf("unable to add collector: %v", err)
	}

	// Pod 对账回填（§7.3.3 校正路径）
	pr := &agent.PodReconciler{Client: mgr.GetClient(), Writer: wr, NodeName: nodeName}
	if err := pr.SetupWithManager(mgr); err != nil {
		klog.Fatalf("unable to setup pod reconciler: %v", err)
	}

	klog.Infof("starting topo-agent (cluster mode) on node %s", nodeName)
	if err := mgr.Start(ctx); err != nil {
		klog.Fatalf("agent manager failed: %v", err)
	}
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
		return nil, nil
	}
}

// agentScheme 返回 agent 所需的 scheme（含 NodeGpuTopology + Pod）。
func agentScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	if err := apis.AddToScheme(s); err != nil {
		klog.Fatalf("unable to add topogang APIs: %v", err)
	}
	return s
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
