package agent

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	schedulingv1alpha1 "github.com/chenxihui/TopoGang/apis/scheduling/v1alpha1"
)

// PodReconciler 监听 Pod 的 gpu-uuids annotation，对账回填 NodeGpuTopology 的
// allocatedTo（§7.3.3 校正路径 / §7.1）。allocatedTo 是对账基准与告警来源，
// 不是调度器记账的写入源。
type PodReconciler struct {
	client.Client
	// Writer 用于回填 allocatedTo。
	Writer *ClusterWriter
	// NodeName 本 agent 所在节点。
	NodeName string
	// GpuIDNode 解析某 GPU 归属节点（通常 agent 只处理本节点；注入以便测试）。
	GpuIDNode func(gpuID string) string
}

// SetupWithManager 注册 Pod watch。
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapPod)).
		Complete(r)
}

// mapPod 将携带 gpu-uuids annotation 且属于本节点的 Pod 入队。
func (r *PodReconciler) mapPod(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	if pod.Spec.NodeName != r.NodeName {
		return nil
	}
	if pod.Annotations == nil {
		return nil
	}
	if _, has := pod.Annotations[schedulingv1alpha1.GPUUUIDsAnnotation]; !has {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}}}
}

// Reconcile 对账回填 allocatedTo（§7.3.3）。
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		// Pod 已删除：清理对应 GPU 的 allocatedTo（由属主节点 agent 处理）
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if pod.Spec.NodeName != r.NodeName {
		return ctrl.Result{}, nil
	}
	if pod.Annotations == nil {
		return ctrl.Result{}, nil
	}
	gpuUUIDs := pod.Annotations[schedulingv1alpha1.GPUUUIDsAnnotation]
	if gpuUUIDs == "" {
		return ctrl.Result{}, nil
	}

	// 解析 GPU 列表并回填 allocatedTo
	for _, gpuID := range strings.Split(gpuUUIDs, ",") {
		gpuID = strings.TrimSpace(gpuID)
		if gpuID == "" {
			continue
		}
		if err := r.Writer.BackfillAllocation(ctx, r.NodeName, gpuID, string(pod.UID), pod.Name, pod.Namespace); err != nil {
			klog.Warningf("backfill allocation failed gpu=%s: %v", gpuID, err)
		}
	}
	return ctrl.Result{}, nil
}
