package deviceplugin

import (
	"testing"
)

// fakeCheckpoint 返回固定的物理 GPU 状态。
type fakeCheckpoint struct {
	gpus []PhysicalGPU
	err  error
}

func (f *fakeCheckpoint) GPUs() ([]PhysicalGPU, error) {
	return f.gpus, f.err
}

func TestParseGPUUUIDs(t *testing.T) {
	ids := ParseGPUUUIDs("GPU-1, GPU-2,GPU-3")
	if len(ids) != 3 || ids[0] != "GPU-1" || ids[1] != "GPU-2" || ids[2] != "GPU-3" {
		t.Fatalf("unexpected parse: %v", ids)
	}
	if ParseGPUUUIDs("") != nil {
		t.Fatal("expected nil for empty")
	}
}

// N7 正向：声明合法且空闲的 GPU 校验通过。
func TestValidateAndAllocate_OK(t *testing.T) {
	ckpt := &fakeCheckpoint{gpus: []PhysicalGPU{
		{ID: "GPU-1", AllocatedBy: ""},
		{ID: "GPU-2", AllocatedBy: ""},
	}}
	r := ValidateAndAllocate(AllocateRequest{
		PodUID:             "pod-1",
		GPUUUIDsAnnotation: "GPU-1,GPU-2",
		RequestedCount:     2,
	}, ckpt)
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if len(r.DeviceIDs) != 2 {
		t.Fatalf("expected 2 device IDs, got %v", r.DeviceIDs)
	}
}

// 请求数一致性：annotation 声明数与请求数不符拒绝。
func TestValidateAndAllocate_CountMismatch(t *testing.T) {
	ckpt := &fakeCheckpoint{gpus: []PhysicalGPU{
		{ID: "GPU-1", AllocatedBy: ""},
	}}
	r := ValidateAndAllocate(AllocateRequest{
		PodUID:             "pod-1",
		GPUUUIDsAnnotation: "GPU-1,GPU-2",
		RequestedCount:     1,
	}, ckpt)
	if r.OK {
		t.Fatal("expected reject on count mismatch")
	}
}

// N7 冲突检测：声明的 GPU 已被其他 Pod 分配 -> 拒绝（防伪造/篡改）。
func TestValidateAndAllocate_Conflict(t *testing.T) {
	ckpt := &fakeCheckpoint{gpus: []PhysicalGPU{
		{ID: "GPU-1", AllocatedBy: "other-pod"},
	}}
	r := ValidateAndAllocate(AllocateRequest{
		PodUID:             "pod-1",
		GPUUUIDsAnnotation: "GPU-1",
		RequestedCount:     1,
	}, ckpt)
	if r.OK {
		t.Fatal("expected reject on GPU already allocated to another pod (N7)")
	}
}

// 声明的 GPU 物理不存在 -> 拒绝。
func TestValidateAndAllocate_NotFound(t *testing.T) {
	ckpt := &fakeCheckpoint{gpus: []PhysicalGPU{
		{ID: "GPU-1", AllocatedBy: ""},
	}}
	r := ValidateAndAllocate(AllocateRequest{
		PodUID:             "pod-1",
		GPUUUIDsAnnotation: "GPU-99",
		RequestedCount:     1,
	}, ckpt)
	if r.OK {
		t.Fatal("expected reject on GPU not found in checkpoint")
	}
}

// 同一 Pod 重复声明自己已占的 GPU 允许（幂等）。
func TestValidateAndAllocate_SelfOwnedOK(t *testing.T) {
	ckpt := &fakeCheckpoint{gpus: []PhysicalGPU{
		{ID: "GPU-1", AllocatedBy: "pod-1"},
	}}
	r := ValidateAndAllocate(AllocateRequest{
		PodUID:             "pod-1",
		GPUUUIDsAnnotation: "GPU-1",
		RequestedCount:     1,
	}, ckpt)
	if !r.OK {
		t.Fatalf("expected OK for self-owned GPU, got %+v", r)
	}
}

func TestEnvForGPU(t *testing.T) {
	env := EnvForGPU([]string{"GPU-1", "GPU-2"}, "NVIDIA_")
	if env["NVIDIA_VISIBLE_DEVICES"] != "GPU-1,GPU-2" {
		t.Fatalf("unexpected env: %v", env)
	}
}
