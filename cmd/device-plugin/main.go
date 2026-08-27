// topo-gpu-plugin 入口（§7.4）：GPU 资源上报与分配生效（device plugin，仅执行不决策）。
//
// 设计原则（C1/N7）："决策在调度器、执行在插件"。调度器 PreBind 写 gpu-uuids
// annotation，插件 Allocate 时读取并以 kubelet device manager checkpoint 为物理基准校验。
//
// 用法：
//
//	topo-gpu-plugin --device-socket=/var/lib/kubelet/device-plugins
//	topo-gpu-plugin --device-socket=/var/lib/kubelet/device-plugins --mock
//
// 无 GPU 环境可用 --mock 上报模拟设备。
package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/klog/v2"

	"github.com/chenxihui/TopoGang/pkg/deviceplugin"
)

func main() {
	var (
		deviceSocket string
		mock         bool
		mockCount    int
	)
	flag.StringVar(&deviceSocket, "device-socket", "/var/lib/kubelet/device-plugins", "kubelet device plugins directory")
	flag.BoolVar(&mock, "mock", true, "use mock device list (no GPU env)")
	flag.IntVar(&mockCount, "mock-count", 8, "number of mock GPUs")
	klog.InitFlags(nil)
	flag.Parse()

	// 收集设备（真实环境由 nvidia-smi 枚举；mock 用模拟设备）
	var deviceIDs []string
	if mock {
		deviceIDs = make([]string, mockCount)
		for i := range deviceIDs {
			deviceIDs[i] = "GPU-MOCK-" + string(rune('A'+i))
		}
	} else {
		klog.Fatal("real device enumeration not implemented; use --mock or extend via nvidia-smi")
	}

	// checkpoint 基准（N7）：mock 模式全部空闲
	ckpt := &mockCheckpoint{ids: deviceIDs}

	plugin := deviceplugin.NewPlugin(deviceIDs, nil, ckpt)

	// 注册到 kubelet
	kubeletSocket := deviceSocket + "/kubelet.sock"
	if err := plugin.RegisterWithKubelet(kubeletSocket, deviceplugin.ResourceName); err != nil {
		klog.Warningf("register with kubelet failed (continue serving): %v", err)
	}

	// 启动 gRPC 服务
	ctx, cancel := signal.NotifyContext(nil, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_ = ctx
	klog.Infof("starting topo-gpu-plugin (resource=%s, %d devices)", deviceplugin.ResourceName, len(deviceIDs))
	if err := plugin.Serve(deviceSocket); err != nil {
		klog.Fatalf("serve device plugin: %v", err)
	}
}

// mockCheckpoint 返回全部空闲的物理 GPU 基准（N7）。
type mockCheckpoint struct {
	ids []string
}

func (m *mockCheckpoint) GPUs() ([]deviceplugin.PhysicalGPU, error) {
	out := make([]deviceplugin.PhysicalGPU, 0, len(m.ids))
	for _, id := range m.ids {
		out = append(out, deviceplugin.PhysicalGPU{ID: id, AllocatedBy: ""})
	}
	return out, nil
}
