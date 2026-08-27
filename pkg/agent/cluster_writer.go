package agent

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	topologyv1alpha1 "github.com/chenxihui/TopoGang/apis/topology/v1alpha1"
	topo "github.com/chenxihui/TopoGang/pkg/topo"
)

// ClusterWriter 是基于 controller-runtime 的 NodeGpuTopology Writer（§7.1）。
// 对接真实集群 CRD，并把采集到的拓扑写入 spec；对账回填由 PodReconciler 负责。
type ClusterWriter struct {
	client.Client
	// DefaultTTL 心跳过期阈值（§7.1，默认 60s×2）。
	DefaultTTL time.Duration
}

// NewClusterWriter 构造集群 Writer。
func NewClusterWriter(cl client.Client) *ClusterWriter {
	return &ClusterWriter{Client: cl, DefaultTTL: 120 * time.Second}
}

// Write 创建或更新 NodeGpuTopology（§7.1）。
// topology 为 nil 表示采集失败：保留旧数据、标记健康 false（T2 数据缺失）。
func (w *ClusterWriter) Write(ctx context.Context, nodeName string, topology *topo.GpuTopology, generation int64) (bool, error) {
	var existing topologyv1alpha1.NodeGpuTopology
	err := w.Get(ctx, types.NamespacedName{Name: topologyName(nodeName)}, &existing)
	notFound := errors.IsNotFound(err)
	if err != nil && !notFound {
		return false, err
	}

	now := metav1.Now()

	if topology == nil {
		// 采集失败：保留旧 spec，标记健康 false + 记录错误（T2 数据缺失）
		if notFound {
			return false, nil // 无旧数据可保留
		}
		existing.Status.Healthy = boolPtr(false)
		existing.Status.LastHeartbeat = &now
		if err := w.Status().Update(ctx, &existing); err != nil {
			return false, err
		}
		return false, nil
	}

	// 转换 GpuTopology -> NodeGpuTopology
	spec := toNodeGpuTopologySpec(nodeName, topology, generation)

	if notFound {
		obj := &topologyv1alpha1.NodeGpuTopology{
			ObjectMeta: metav1.ObjectMeta{
				Name:   topologyName(nodeName),
				Labels: map[string]string{topologyv1alpha1.NodeNameLabel: nodeName},
			},
			Spec: spec,
			Status: topologyv1alpha1.NodeGpuTopologyStatus{
				ObservedGeneration: generation,
				LastHeartbeat:      &now,
				Healthy:            boolPtr(true),
			},
		}
		if err := w.Create(ctx, obj); err != nil {
			return false, err
		}
		return true, nil
	}

	// 更新：保留既有 allocatedTo（对账回填由 PodReconciler 维护，不在此覆盖）
	spec = preserveAllocations(existing.Spec, spec)
	existing.Spec = spec
	existing.Status.ObservedGeneration = generation
	existing.Status.LastHeartbeat = &now
	existing.Status.Healthy = boolPtr(true)
	existing.Status.Error = ""
	if err := w.Update(ctx, &existing); err != nil {
		return false, err
	}
	return true, nil
}

// GetView 返回某节点 NodeGpuTopology 的轻量视图（§7.1 对账）。
func (w *ClusterWriter) GetView(ctx context.Context, name types.NamespacedName) (*GpuTopologyView, error) {
	var obj topologyv1alpha1.NodeGpuTopology
	if err := w.Get(ctx, types.NamespacedName{Name: topologyName(name.Name)}, &obj); err != nil {
		if errors.IsNotFound(err) {
			return &GpuTopologyView{Generation: 0, Healthy: false}, nil
		}
		return nil, err
	}
	alloc := map[string]string{}
	for _, g := range obj.Spec.Gpus {
		if g.AllocatedTo != nil {
			alloc[g.ID] = g.AllocatedTo.PodUID
		}
	}
	healthy := obj.Status.Healthy == nil || *obj.Status.Healthy
	return &GpuTopologyView{
		Generation:  obj.Spec.Generation,
		Healthy:     healthy,
		Allocations: alloc,
	}, nil
}

// ---------- 转换辅助 ----------

// topologyName 返回 NodeGpuTopology 对象名（§6.2 label 关联节点）。
func topologyName(nodeName string) string {
	return "node-" + sanitizeName(nodeName)
}

func sanitizeName(s string) string {
	// DNS-1123 兼容：替换非法字符
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+32)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

// toNodeGpuTopologySpec 把 GpuTopology 转为 NodeGpuTopologySpec（§6.2）。
func toNodeGpuTopologySpec(nodeName string, g *topo.GpuTopology, generation int64) topologyv1alpha1.NodeGpuTopologySpec {
	spec := topologyv1alpha1.NodeGpuTopologySpec{
		NodeName:   nodeName,
		Generation: generation,
		Source:     topologyv1alpha1.SourceNvidiaSMI,
		Gpus:       make([]topologyv1alpha1.Gpu, 0, len(g.GPUs)),
		Domains:    make([]topologyv1alpha1.Domain, 0, len(g.Domains)),
	}

	// gpuID -> nvlinkDomain 映射
	domainByGPU := map[int]string{}
	for _, d := range g.Domains {
		for _, idx := range d.GPUIndexes {
			domainByGPU[idx] = d.ID
		}
	}

	// 链路信息
	peerByGPU := map[int][]topologyv1alpha1.GpuPeer{}
	for _, l := range g.Links {
		peerByGPU[l.A] = append(peerByGPU[l.A], topologyv1alpha1.GpuPeer{
			GpuID:     gpuIDOf(g, l.B),
			LinkType:  topologyv1alpha1.LinkType(l.LinkType),
			LinkSpeed: l.Bandwidth,
		})
	}

	for _, gp := range g.GPUs {
		gpu := topologyv1alpha1.Gpu{
			ID:           gp.ID,
			Index:        int32(gp.Index),
			NvlinkDomain: domainByGPU[gp.Index],
			Peers:        peerByGPU[gp.Index],
		}
		spec.Gpus = append(spec.Gpus, gpu)
	}

	for _, d := range g.Domains {
		idxs := make([]int32, 0, len(d.GPUIndexes))
		for _, i := range d.GPUIndexes {
			idxs = append(idxs, int32(i))
		}
		spec.Domains = append(spec.Domains, topologyv1alpha1.Domain{
			ID:             d.ID,
			GpuIndexes:     idxs,
			IntraBandwidth: 600,
		})
	}
	return spec
}

// preserveAllocations 保留旧 spec 中的 allocatedTo（§7.1 对账，PodReconciler 维护）。
func preserveAllocations(old, new topologyv1alpha1.NodeGpuTopologySpec) topologyv1alpha1.NodeGpuTopologySpec {
	oldAlloc := map[string]*topologyv1alpha1.GpuAllocation{}
	for i := range old.Gpus {
		if old.Gpus[i].AllocatedTo != nil {
			oldAlloc[old.Gpus[i].ID] = old.Gpus[i].AllocatedTo
		}
	}
	for i := range new.Gpus {
		if a, ok := oldAlloc[new.Gpus[i].ID]; ok {
			new.Gpus[i].AllocatedTo = a
		}
	}
	return new
}

func gpuIDOf(g *topo.GpuTopology, index int) string {
	for _, gp := range g.GPUs {
		if gp.Index == index {
			return gp.ID
		}
	}
	return ""
}

func boolPtr(b bool) *bool { return &b }

var _ Writer = &ClusterWriter{}

// BackfillAllocation 对账回填：把某 GPU 的 allocatedTo 写入 NodeGpuTopology（§7.3.3 校正路径）。
// 由 PodReconciler 在监听 Pod annotation 后调用。
func (w *ClusterWriter) BackfillAllocation(ctx context.Context, nodeName, gpuID, podUID, podName, namespace string) error {
	var obj topologyv1alpha1.NodeGpuTopology
	if err := w.Get(ctx, types.NamespacedName{Name: topologyName(nodeName)}, &obj); err != nil {
		if errors.IsNotFound(err) {
			return nil // 拓扑尚未写入，跳过
		}
		return err
	}
	updated := false
	for i := range obj.Spec.Gpus {
		if obj.Spec.Gpus[i].ID == gpuID {
			if podUID == "" {
				obj.Spec.Gpus[i].AllocatedTo = nil
			} else {
				obj.Spec.Gpus[i].AllocatedTo = &topologyv1alpha1.GpuAllocation{
					PodUID:    podUID,
					PodName:   podName,
					Namespace: namespace,
				}
			}
			updated = true
			break
		}
	}
	if !updated {
		return nil
	}
	if err := w.Update(ctx, &obj); err != nil {
		return err
	}
	klog.V(2).Infof("backfilled allocation node=%s gpu=%s pod=%s", nodeName, gpuID, podUID)
	return nil
}
