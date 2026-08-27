// Package controllers 实现 TopoGang 的 CRD 控制器。
package controllers

import (
	"context"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	schedulingv1alpha1 "github.com/chenxihui/TopoGang/apis/scheduling/v1alpha1"
	"github.com/chenxihui/TopoGang/pkg/controller/state"
)

// FinalizerName 保证组清理。
const FinalizerName = "scheduling.topogang.io/finalizer"

// PodGroupReconciler 负责 PodGroup 生命周期与状态机（§7.2 / §9.1）。
type PodGroupReconciler struct {
	client.Client
	// StateMachine PodGroup 状态机纯逻辑。
	StateMachine *state.StateMachine
}

// Options 控制器参数。
type Options struct {
	// ScheduleTimeout 组排队超时（默认 600s）。
	ScheduleTimeout time.Duration
	// FailureObservationWindow 失败观察窗口 T（默认 60s）。
	FailureObservationWindow time.Duration
}

// NewPodGroupReconciler 构造控制器。
func NewPodGroupReconciler(cl client.Client, opts Options) *PodGroupReconciler {
	if opts.ScheduleTimeout <= 0 {
		opts.ScheduleTimeout = 600 * time.Second
	}
	if opts.FailureObservationWindow <= 0 {
		opts.FailureObservationWindow = 60 * time.Second
	}
	return &PodGroupReconciler{
		Client:       cl,
		StateMachine: state.New(state.Options{ScheduleTimeout: opts.ScheduleTimeout, FailureObservationWindow: opts.FailureObservationWindow}),
	}
}

// Reconcile 处理 PodGroup 事件（§7.2）。
func (r *PodGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pg schedulingv1alpha1.PodGroup
	if err := r.Get(ctx, req.NamespacedName, &pg); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// finalizer：删除时解绑成员 Pod 的 group-name annotation（s4 修订）
	if !pg.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &pg)
	}

	// 确认 finalizer
	if !controllerutil.ContainsFinalizer(&pg, FinalizerName) {
		controllerutil.AddFinalizer(&pg, FinalizerName)
		if err := r.Update(ctx, &pg); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// 计算成员状态统计（§7.2 计数）
	members, err := r.listGroupPods(ctx, &pg)
	if err != nil {
		return ctrl.Result{}, err
	}
	counts := countMembers(members)

	// 读取 released-generation annotation，更新 scheduledByGroup 与 phase（闭环，§9.1）
	view := r.buildView(&pg, counts)

	// 状态机迁移
	dec := r.StateMachine.Observe(view)
	if schedulingv1alpha1.PodGroupPhase(dec.NextPhase) == pg.Status.Phase && !dec.UpdateScheduledByGroup {
		// 无变化
		return ctrl.Result{}, nil
	}

	// 应用决策
	if err := r.applyDecision(ctx, &pg, dec); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// buildView 构造状态机输入（§9.1）。
func (r *PodGroupReconciler) buildView(pg *schedulingv1alpha1.PodGroup, counts state.MemberCounts) state.GroupView {
	v := state.GroupView{
		Phase:             state.Phase(pg.Status.Phase),
		MinMember:         pg.Spec.MinMember,
		ScheduledByGroup:  pg.Status.ScheduledByGroup,
		Members:           counts,
		ReleasedGeneration: r.readReleasedGeneration(pg),
		Now:               time.Now(),
		CreationTime:      pg.CreationTimestamp.Time,
	}
	if pg.Status.LastScheduleTime != nil {
		ls := pg.Status.LastScheduleTime.Time
		v.LastScheduleTime = &ls
	}
	return v
}

// applyDecision 将状态机决策写回 CRD（§9.1）。
func (r *PodGroupReconciler) applyDecision(ctx context.Context, pg *schedulingv1alpha1.PodGroup, dec state.Decision) error {
	updated := pg.DeepCopy()

	if dec.UpdateScheduledByGroup {
		updated.Status.ScheduledByGroup = dec.ScheduledByGroup
	}
	if updated.Status.Phase != schedulingv1alpha1.PodGroupPhase(dec.NextPhase) {
		updated.Status.Phase = schedulingv1alpha1.PodGroupPhase(dec.NextPhase)
		klog.V(2).Infof("podgroup %s/%s phase %s -> %s (%s)", pg.Namespace, pg.Name, pg.Status.Phase, dec.NextPhase, dec.Reason)
		// 记录最后迁移时间（观察窗口基准）
		now := metav1.Now()
		updated.Status.LastScheduleTime = &now
	}

	switch dec.Action {
	case state.ActionReset:
		// 回退清零（S4）：组离开 Running，置 scheduledByGroup=0
		updated.Status.ScheduledByGroup = 0
	case state.ActionBumpReleasedGeneration:
		// 放行闭环：由调度器写入 released-generation，Controller 只消费，不在此写
	}

	if err := r.Status().Update(ctx, updated); err != nil {
		return err
	}
	return nil
}

// handleDeletion 处理 PodGroup 删除（s4 修订）：解绑成员 Pod 的 group-name annotation，
// 避免孤儿 Pod 在 Permit 永久 Wait。
func (r *PodGroupReconciler) handleDeletion(ctx context.Context, pg *schedulingv1alpha1.PodGroup) (ctrl.Result, error) {
	members, err := r.listGroupPods(ctx, pg)
	if err != nil {
		return ctrl.Result{}, err
	}
	for i := range members {
		if members[i].DeletionTimestamp.IsZero() {
			if err := r.unbindGroupAnnotation(ctx, &members[i]); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	// 移除 finalizer 完成删除
	if controllerutil.ContainsFinalizer(pg, FinalizerName) {
		controllerutil.RemoveFinalizer(pg, FinalizerName)
		if err := r.Update(ctx, pg); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// unbindGroupAnnotation 移除 Pod 的 group-name annotation（孤儿解绑，s4）。
func (r *PodGroupReconciler) unbindGroupAnnotation(ctx context.Context, pod *corev1.Pod) error {
	if pod.Annotations == nil {
		return nil
	}
	if _, ok := pod.Annotations[schedulingv1alpha1.GroupNameAnnotation]; !ok {
		return nil
	}
	podCopy := pod.DeepCopy()
	delete(podCopy.Annotations, schedulingv1alpha1.GroupNameAnnotation)
	return r.Update(ctx, podCopy)
}

// listGroupPods 列出组内成员 Pod（§7.2 计数）。
func (r *PodGroupReconciler) listGroupPods(ctx context.Context, pg *schedulingv1alpha1.PodGroup) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(pg.Namespace)); err != nil {
		return nil, err
	}
	var members []corev1.Pod
	for _, pod := range pods.Items {
		if pod.Annotations != nil && pod.Annotations[schedulingv1alpha1.GroupNameAnnotation] == pg.Name {
			members = append(members, pod)
		}
	}
	return members, nil
}

// countMembers 统计成员状态（§7.2 / §9.1）。
func countMembers(pods []corev1.Pod) state.MemberCounts {
	var c state.MemberCounts
	c.Total = int32(len(pods))
	for _, p := range pods {
		switch {
		case isPodSucceeded(p):
			c.Success++
		case isPodFailed(p):
			c.Failed++
		case p.Status.Phase == corev1.PodRunning:
			c.Running++
		default:
			c.Pending++
		}
	}
	return c
}

func isPodSucceeded(p corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded
}

func isPodFailed(p corev1.Pod) bool {
	// 终态 Failed：restartPolicy=Never 且 PodFailed（§9.1 T3）
	if p.Status.Phase == corev1.PodFailed && p.Spec.RestartPolicy == corev1.RestartPolicyNever {
		return true
	}
	return false
}

// readReleasedGeneration 读取 released-generation annotation（§9.1 闭环）。
func (r *PodGroupReconciler) readReleasedGeneration(pg *schedulingv1alpha1.PodGroup) int64 {
	if pg.Annotations == nil {
		return 0
	}
	v, err := strconv.ParseInt(pg.Annotations[schedulingv1alpha1.ReleasedGenerationAnnotation], 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// SetupWithManager 注册 controller 与 watch（PodGroup + 组内 Pod）。
func (r *PodGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&schedulingv1alpha1.PodGroup{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapPodToGroup)).
		Complete(r)
}

// mapPodToGroup 将 Pod 事件映射到所属 PodGroup（§7.2）。
func (r *PodGroupReconciler) mapPodToGroup(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod.Annotations == nil {
		return nil
	}
	groupName := pod.Annotations[schedulingv1alpha1.GroupNameAnnotation]
	if groupName == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: groupName},
	}}
}

var _ reconcile.Reconciler = &PodGroupReconciler{}
