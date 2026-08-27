// topogang-scheduler 入口（§5.2 / §7.3）。
//
// 以独立 kube-scheduler 二进制的插件扩展方式运行，schedulerName=topogang-scheduler。
//
// 部署：
//   - 真实集群集成需链接 kube-scheduler（scheduler-plugins 框架），注册 Gang 插件。
//   - 本项目提供 `config/scheduler/scheduler-config.yaml`（§7.3 独立 profile）与
//     `pkg/plugins/gang`（Gang 扩展点纯逻辑编排，可单测）。
//   - 真实 framework 注册（QueueSort/PreFilter/Permit/PostFilter/Reserve 映射）见
//     docs/DESIGN.md §7.3.1 表格，在集群环境链接 k8s.io/kubernetes 时接入。
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"k8s.io/klog/v2"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config/scheduler/scheduler-config.yaml", "path to scheduler configuration")
	klog.InitFlags(nil)
	flag.Parse()

	if err := validateConfig(configPath); err != nil {
		fmt.Fprintf(os.Stderr, "scheduler config invalid: %v\n", err)
		os.Exit(1)
	}
	klog.Infof("topogang-scheduler config validated: %s", configPath)
	klog.Infof("NOTE: production launch requires linking kube-scheduler with Gang plugin registry (see docs/DESIGN.md §7.3)")
}

// validateConfig 做基本的 YAML 存在性与 profile 校验。
func validateConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	// 简单校验：应包含 schedulerName 与 topogang 相关插件
	if !strings.Contains(string(data), "topogang-scheduler") {
		return fmt.Errorf("config missing schedulerName 'topogang-scheduler'")
	}
	if !strings.Contains(string(data), "GangQueueSort") {
		return fmt.Errorf("config missing GangQueueSort plugin")
	}
	return nil
}
