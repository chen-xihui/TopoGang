// Package deviceplugin 实现 topo-gpu-plugin 的分配逻辑（§7.4）。
//
// 原则（C1/N7）："决策在调度器、执行在插件"。调度器在 PreBind 把 GPU 列表写入
// Pod annotation `topogang.io/gpu-uuids`；device plugin 在 Allocate 时**读取**该
// annotation，以 kubelet device manager checkpoint 为物理基准校验目标 GPU 存在且
// 未被其他 Pod 分配（annotation 可篡改，不能作为信任源，N7），校验通过后返回
// 设备注入配置（deviceIDs/envs），**只执行不决策**；校验失败拒绝 Allocate 并告警。
package deviceplugin

import (
	"fmt"
	"strings"
)

// AllocateRequest 是 device plugin Allocate 的输入。
type AllocateRequest struct {
	// PodUID 请求 Pod 的 UID。
	PodUID string
	// GPUUUIDsAnnotation 调度器 PreBind 写入的 gpu-uuids annotation 值（逗号分隔）。
	GPUUUIDsAnnotation string
	// RequestedCount 该 Pod 请求的 GPU 数量。
	RequestedCount int
}

// PhysicalGPU 是 kubelet device manager checkpoint 中一张物理 GPU 的状态（N7 基准）。
type PhysicalGPU struct {
	// ID 物理 GPU ID（PCI 地址 / UUID）。
	ID string
	// AllocatedBy 当前已分配给哪个 Pod UID（空表示空闲）。
	AllocatedBy string
}

// Checkpoint 提供物理 GPU 分配基准（N7）。
type Checkpoint interface {
	// GPUs 返回节点上物理 GPU 及其当前分配状态。
	GPUs() ([]PhysicalGPU, error)
}

// AllocateResult 是分配校验的结果。
type AllocateResult struct {
	// DeviceIDs 校验通过后注入的 deviceIDs（§7.4）。
	DeviceIDs []string
	// OK 是否校验通过。
	OK bool
	// Error 校验失败原因。
	Error string
}

// ParseGPUUUIDs 解析 gpu-uuids annotation 为 GPU ID 列表。
func ParseGPUUUIDs(annotation string) []string {
	if annotation == "" {
		return nil
	}
	parts := strings.Split(annotation, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ValidateAndAllocate 校验 gpu-uuids annotation 并返回可注入的设备。
//
// 校验规则（N7）：
//  1. 请求数一致性：annotation 声明的 GPU 数必须等于请求数。
//  2. 物理基准：声明的 GPU 必须存在于 kubelet checkpoint。
//  3. 冲突检测：声明的 GPU 不得已被其他 Pod 分配（annotation 可篡改，以 checkpoint 为准）。
func ValidateAndAllocate(req AllocateRequest, ckpt Checkpoint) AllocateResult {
	declared := ParseGPUUUIDs(req.GPUUUIDsAnnotation)
	if len(declared) != req.RequestedCount {
		return AllocateResult{
			OK:    false,
			Error: fmt.Sprintf("gpu-uuids count %d != requested %d", len(declared), req.RequestedCount),
		}
	}

	physGPUs, err := ckpt.GPUs()
	if err != nil {
		return AllocateResult{OK: false, Error: fmt.Sprintf("read checkpoint: %v", err)}
	}
	physMap := map[string]string{} // gpuID -> owner
	for _, g := range physGPUs {
		physMap[g.ID] = g.AllocatedBy
	}

	// 校验每个声明的 GPU 存在且未被他人占用
	for _, gpuID := range declared {
		owner, exists := physMap[gpuID]
		if !exists {
			return AllocateResult{OK: false, Error: fmt.Sprintf("declared GPU %s not found in checkpoint", gpuID)}
		}
		if owner != "" && owner != req.PodUID {
			return AllocateResult{
				OK:    false,
				Error: fmt.Sprintf("declared GPU %s already allocated to pod %s", gpuID, owner),
			}
		}
	}

	return AllocateResult{OK: true, DeviceIDs: declared}
}

// EnvForGPU 生成注入的环境变量（§7.4，供容器访问 GPU）。
func EnvForGPU(deviceIDs []string, prefix string) map[string]string {
	// 默认 NV 风格：NVIDIA_VISIBLE_DEVICES 用逗号分隔
	return map[string]string{
		prefix + "VISIBLE_DEVICES": strings.Join(deviceIDs, ","),
	}
}
