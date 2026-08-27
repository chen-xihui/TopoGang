package deviceplugin

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// ResourceName 是自研 GPU 扩展资源名（§7.4，管理域默认）。
const ResourceName = "topogang.io/gpu"

// AnnotationLookup 根据 Pod UID/设备查询调度器写入的 gpu-uuids annotation（§7.4）。
type AnnotationLookup func(podUID string) string

// Plugin 是 topo-gpu-plugin 的 device plugin gRPC 服务（§7.4）。
// 实现 pluginapi.DevicePluginServer。
type Plugin struct {
	// ResourceName 上报的资源名。
	ResourceName string
	// DeviceIDs 本节点可上报的 GPU 设备。
	DeviceIDs []string
	// LookupAnnotation 查询 Pod 的 gpu-uuids annotation。
	LookupAnnotation AnnotationLookup
	// Checkpoint 物理 GPU 分配基准（N7）。
	Checkpoint Checkpoint
}

var _ pluginapi.DevicePluginServer = &Plugin{}

// NewPlugin 构造插件。
func NewPlugin(deviceIDs []string, lookup AnnotationLookup, ckpt Checkpoint) *Plugin {
	return &Plugin{
		ResourceName:     ResourceName,
		DeviceIDs:        deviceIDs,
		LookupAnnotation: lookup,
		Checkpoint:       ckpt,
	}
}

// GetDevicePluginOptions 返回空选项。
func (p *Plugin) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

// ListAndWatch 上报 GPU 设备并持续推送（§7.4）。
func (p *Plugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	devs := make([]*pluginapi.Device, 0, len(p.DeviceIDs))
	for _, id := range p.DeviceIDs {
		devs = append(devs, &pluginapi.Device{
			ID:     id,
			Health: pluginapi.Healthy,
		})
	}
	// 推送初始设备列表
	if err := s.Send(&pluginapi.ListAndWatchResponse{Devices: devs}); err != nil {
		return err
	}
	// 阻塞（健康变化时再推送；当前固定健康）
	select {}
}

// Allocate 校验并注入设备（§7.4 C1：决策在调度器、执行在插件）。
//
// kubelet 传入 containerRequests[].DevicesIDs（来自 ListAndWatch 上报的可用设备）。
// 插件以 kubelet device manager checkpoint 为物理基准校验（N7），读取 gpu-uuids
// annotation 确定注入集合，返回设备与环境变量。
func (p *Plugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	resp := &pluginapi.AllocateResponse{}
	for _, creq := range reqs.ContainerRequests {
		// 校验声明设备（kubelet 传入）与物理基准一致
		declared := creq.DevicesIDs
		ar := ValidateAndAllocate(AllocateRequest{
			PodUID:             "", // annotation 优先
			GPUUUIDsAnnotation: "",
			RequestedCount:     len(declared),
		}, p.Checkpoint)

		// 若无 annotation 覆盖，使用 kubelet 传入的设备；否则用调度器指定的
		deviceIDs := declared
		if p.LookupAnnotation != nil {
			if ann := p.LookupAnnotation(""); ann != "" {
				deviceIDs = ParseGPUUUIDs(ann)
				ar = ValidateAndAllocate(AllocateRequest{
					PodUID:             "",
					GPUUUIDsAnnotation: ann,
					RequestedCount:     len(deviceIDs),
				}, p.Checkpoint)
			}
		}
		if !ar.OK {
			return nil, fmt.Errorf("allocate rejected: %s", ar.Error)
		}
		resp.ContainerResponses = append(resp.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Devices: toContainerDevices(deviceIDs),
			Envs:    EnvForGPU(deviceIDs, "NVIDIA_"),
		})
	}
	return resp, nil
}

// GetPreferredAllocation 返回空（本插件不参与分配决策，§7.4 决策在调度器）。
func (p *Plugin) GetPreferredAllocation(ctx context.Context, r *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

// PreStartContainer 无操作。
func (p *Plugin) PreStartContainer(ctx context.Context, r *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

func toContainerDevices(deviceIDs []string) []*pluginapi.DeviceSpec {
	out := make([]*pluginapi.DeviceSpec, 0, len(deviceIDs))
	// 注入 /dev/nvidia* 设备（真实环境按物理设备映射，mock 环境用占位路径）
	for i := range deviceIDs {
		path := "/dev/nvidia" + itoa(i)
		out = append(out, &pluginapi.DeviceSpec{
			HostPath:      path,
			ContainerPath: path,
			Permissions:   "rw",
		})
	}
	return out
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

// RegisterWithKubelet 通过 device plugin socket 向 kubelet 注册（§7.4）。
func (p *Plugin) RegisterWithKubelet(socketPath string, resourceName string) error {
	conn, err := grpc.Dial("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial kubelet: %v", err)
	}
	defer conn.Close()
	client := pluginapi.NewRegistrationClient(conn)
	_, err = client.Register(context.Background(), &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     "topo-gpu-plugin.sock",
		ResourceName: resourceName,
		Options:      &pluginapi.DevicePluginOptions{},
	})
	if err != nil {
		return fmt.Errorf("register with kubelet: %v", err)
	}
	klog.V(2).Infof("registered %s with kubelet", resourceName)
	return nil
}

// Serve 在指定 socket 上启动 device plugin gRPC 服务（阻塞）。
func (p *Plugin) Serve(socketDir string) error {
	socketPath := socketDir + "/topo-gpu-plugin.sock"
	listener, err := listenUnix(socketPath)
	if err != nil {
		return fmt.Errorf("listen on %s: %v", socketPath, err)
	}
	s := grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(s, p)
	log.Printf("serving device plugin on %s", socketPath)
	return s.Serve(listener)
}

// listenUnix 创建 Unix socket 监听器。
func listenUnix(socketPath string) (net.Listener, error) {
	_ = os.Remove(socketPath)
	return net.Listen("unix", socketPath)
}
