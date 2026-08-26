package agent

import (
	"context"
	"time"

	"k8s.io/klog/v2"

	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// CollectorOptions 配置采集器。
type CollectorOptions struct {
	// NodeName 本节点名。
	NodeName string
	// DiscoverInterval 全量采集周期（默认 60s）。
	DiscoverInterval time.Duration
	// DomainStrategy NVLink 域划分策略（默认 DomainClique）。
	DomainStrategy topo.DomainStrategy
	// Source 数据源（nvidia-smi / mock）。
	Source Source
	// Writer CRD 写入器。
	Writer Writer
}

// Collector 周期采集拓扑并写入 NodeGpuTopology（§7.1）。
type Collector struct {
	opts    CollectorOptions
	gen     int64
	lastErr error
}

// NewCollector 构造采集器。
func NewCollector(opts CollectorOptions) *Collector {
	if opts.DiscoverInterval <= 0 {
		opts.DiscoverInterval = 60 * time.Second
	}
	return &Collector{opts: opts}
}

// Run 阻塞式运行采集循环，直到 ctx 取消。
func (c *Collector) Run(ctx context.Context) {
	klog.Infof("topo-agent collector starting on node %s (interval=%s)", c.opts.NodeName, c.opts.DiscoverInterval)
	ticker := time.NewTicker(c.opts.DiscoverInterval)
	defer ticker.Stop()

	// 启动即采集一次
	c.discoverAndWrite(ctx)
	for {
		select {
		case <-ctx.Done():
			klog.Infof("topo-agent collector stopped")
			return
		case <-ticker.C:
			c.discoverAndWrite(ctx)
		}
	}
}

// discoverAndWrite 执行一次采集并写入 CRD。
func (c *Collector) discoverAndWrite(ctx context.Context) {
	topoData, err := c.opts.Source.Discover(ctx)
	if err != nil {
		// 采集失败：保留旧数据，健康标记由 Writer 处理（§7.1 T2 心跳正常但数据缺失）
		klog.Errorf("discover failed: %v", err)
		c.lastErr = err
		c.markError(ctx)
		return
	}
	c.lastErr = nil
	c.gen++

	// 计算 NVLink 域并回填到拓扑结构（供 Writer 写 domains）
	if err := attachDomains(topoData, c.opts.DomainStrategy); err != nil {
		klog.Errorf("domain computation failed: %v", err)
		c.markError(ctx)
		return
	}

	written, err := c.opts.Writer.Write(ctx, c.opts.NodeName, topoData, c.gen)
	if err != nil {
		klog.Errorf("write NodeGpuTopology failed: %v", err)
		c.markError(ctx)
		return
	}
	if written {
		klog.V(2).Infof("NodeGpuTopology written gen=%d gpus=%d domains=%d", c.gen, len(topoData.GPUs), len(topoData.Domains))
	}
}

// markError 将错误写入 status.error（Writer 侧处理健康标记）。
func (c *Collector) markError(ctx context.Context) {
	_, _ = c.opts.Writer.Write(ctx, c.opts.NodeName, nil, c.gen)
}

// attachDomains 计算 NVLink 域并挂载到 GpuTopology.Domains（内部字段）。
func attachDomains(g *topo.GpuTopology, strategy topo.DomainStrategy) error {
	if g == nil {
		return nil
	}
	g.Domains = topo.FindNvlinkDomains(g, strategy)
	return nil
}
