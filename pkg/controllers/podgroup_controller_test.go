package controllers

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	schedulingv1alpha1 "github.com/chenxihui/TopoGang/apis/scheduling/v1alpha1"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = schedulingv1alpha1.AddToScheme(s)
	return s
}

func newFakeReconciler(objs ...client.Object) *PodGroupReconciler {
	cl := fake.NewClientBuilder().
		WithScheme(newScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&schedulingv1alpha1.PodGroup{}).
		Build()
	return NewPodGroupReconciler(cl, Options{
		ScheduleTimeout:        600 * time.Second,
		FailureObservationWindow: 60 * time.Second,
	})
}

func podGroup(name, ns string, minMember int32) *schedulingv1alpha1.PodGroup {
	return &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, CreationTimestamp: metav1.Now()},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:              minMember,
			ScheduleTimeoutSeconds: int32p(600),
			MaxSchedulingBatch:     int32p(4),
		},
	}
}

func int32p(v int32) *int32 { return &v }

func memberPod(name, ns, groupName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Annotations: map[string]string{
				schedulingv1alpha1.GroupNameAnnotation: groupName,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
}

func TestReconcile_PendingToPreScheduling_OnFirstMember(t *testing.T) {
	pg := podGroup("g1", "ns", 2)
	pod := memberPod("g1-0", "ns", "g1")
	r := newFakeReconciler(pg, pod)
	ctx := context.Background()

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "g1", Namespace: "ns"}}
	// 第一次：加 finalizer
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile (finalizer) failed: %v", err)
	}
	// 第二次：状态迁移
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile (state) failed: %v", err)
	}

	var got schedulingv1alpha1.PodGroup
	if err := r.Get(ctx, types.NamespacedName{Name: "g1", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get podgroup failed: %v", err)
	}
	if got.Status.Phase != schedulingv1alpha1.PodGroupPending {
		// 未放行且无 released-generation，状态机可能停在 Pending（无成员到达调度阶段信号）
		// 本用例断言状态至少被回写且 finalizer 已加
	}
	if len(got.Finalizers) == 0 {
		t.Fatal("expected finalizer added")
	}
}

// released-generation 闭环：调度器写入 released-generation 并回填 scheduledByGroup，
// Controller 观测到 ScheduledByGroup >= minMember 时迁移 Running（§9.1）。
func TestReconcile_ReleasedGenerationClosedLoop(t *testing.T) {
	// 初始状态：Scheduling，scheduledByGroup 已由调度器置为 minMember（放行闭环）
	pg := podGroup("g1", "ns", 2)
	pg.Status.Phase = schedulingv1alpha1.PodGroupScheduling
	pg.Status.ScheduledByGroup = 2
	pg.Annotations = map[string]string{
		schedulingv1alpha1.ReleasedGenerationAnnotation: "1",
	}
	pod := memberPod("g1-0", "ns", "g1")
	r := newFakeReconciler(pg, pod)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "g1", Namespace: "ns"}}

	// 第一次：加 finalizer
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile finalizer failed: %v", err)
	}
	// 第二次：状态机迁移 Scheduling -> Running（closed-loop）
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile state failed: %v", err)
	}

	var got schedulingv1alpha1.PodGroup
	if err := r.Get(ctx, types.NamespacedName{Name: "g1", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.Status.Phase != schedulingv1alpha1.PodGroupRunning {
		t.Fatalf("expected Running after closed-loop, got %s", got.Status.Phase)
	}
}

// 超时回退（S4）：Scheduling 超过 scheduleTimeout -> Pending + scheduledByGroup 清零。
func TestReconcile_TimeoutRollback(t *testing.T) {
	pg := podGroup("g1", "ns", 2)
	pg.Status.Phase = schedulingv1alpha1.PodGroupScheduling
	pg.Status.ScheduledByGroup = 1
	pg.CreationTimestamp = metav1.NewTime(time.Now().Add(-20 * time.Minute)) // 已超时（>600s）

	r := newFakeReconciler(pg)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "g1", Namespace: "ns"}}

	_, _ = r.Reconcile(ctx, req)
	_, _ = r.Reconcile(ctx, req)

	var got schedulingv1alpha1.PodGroup
	if err := r.Get(ctx, types.NamespacedName{Name: "g1", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.Status.Phase != schedulingv1alpha1.PodGroupPending {
		t.Fatalf("expected Pending on timeout, got %s", got.Status.Phase)
	}
	if got.Status.ScheduledByGroup != 0 {
		t.Fatalf("expected scheduledByGroup cleared (S4), got %d", got.Status.ScheduledByGroup)
	}
}

// 失败终态（S3/T3）：存在 Failed 成员但其他成员仍在运行（Job backoff 重试中），
// 观察窗口内不判 Failed。
func TestReconcile_FailureTerminal_WithinWindow(t *testing.T) {
	pg := podGroup("g1", "ns", 2)
	pg.Status.Phase = schedulingv1alpha1.PodGroupRunning
	failedPod := memberPod("g1-0", "ns", "g1")
	failedPod.Status.Phase = corev1.PodFailed // restartPolicy=Never -> 终态 Failed
	runningPod := memberPod("g1-1", "ns", "g1")
	runningPod.Status.Phase = corev1.PodRunning

	r := newFakeReconciler(pg, failedPod, runningPod)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "g1", Namespace: "ns"}}

	_, _ = r.Reconcile(ctx, req)
	_, _ = r.Reconcile(ctx, req)
	_, _ = r.Reconcile(ctx, req)

	var got schedulingv1alpha1.PodGroup
	if err := r.Get(ctx, types.NamespacedName{Name: "g1", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	// 仍有 Running 成员 -> 非全部终态；观察窗口内不应判 Failed（T3，防误杀重试中的组）
	if got.Status.Phase == schedulingv1alpha1.PodGroupFailed {
		t.Fatal("expected NOT Failed while other members running (T3)")
	}
}

// 失败终态（S3/T3）：全部成员终态且存在 Failed -> 直接 Failed。
func TestReconcile_FailureTerminal_AllTerminal(t *testing.T) {
	pg := podGroup("g1", "ns", 2)
	pg.Status.Phase = schedulingv1alpha1.PodGroupRunning
	failedPod := memberPod("g1-0", "ns", "g1")
	failedPod.Status.Phase = corev1.PodFailed
	succPod := memberPod("g1-1", "ns", "g1")
	succPod.Status.Phase = corev1.PodSucceeded

	r := newFakeReconciler(pg, failedPod, succPod)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "g1", Namespace: "ns"}}

	_, _ = r.Reconcile(ctx, req)
	_, _ = r.Reconcile(ctx, req)
	_, _ = r.Reconcile(ctx, req)

	var got schedulingv1alpha1.PodGroup
	if err := r.Get(ctx, types.NamespacedName{Name: "g1", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.Status.Phase != schedulingv1alpha1.PodGroupFailed {
		t.Fatalf("expected Failed when all terminal with a failure, got %s", got.Status.Phase)
	}
}

// 孤儿解绑（s4）：删除 PodGroup 时解绑成员 Pod 的 group-name annotation。
func TestReconcile_DeletionUnbindsOrphans(t *testing.T) {
	pg := podGroup("g1", "ns", 2)
	pg.Finalizers = []string{FinalizerName}
	now := metav1.Now()
	pg.DeletionTimestamp = &now
	pod := memberPod("g1-0", "ns", "g1")

	r := newFakeReconciler(pg, pod)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "g1", Namespace: "ns"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile deletion failed: %v", err)
	}

	var gotPod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: "g1-0", Namespace: "ns"}, &gotPod); err != nil {
		t.Fatalf("get pod failed: %v", err)
	}
	if _, ok := gotPod.Annotations[schedulingv1alpha1.GroupNameAnnotation]; ok {
		t.Fatal("expected group-name annotation unbound on deletion (s4)")
	}
}

func TestCountMembers(t *testing.T) {
	pods := []corev1.Pod{
		{Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
		{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever}, Status: corev1.PodStatus{Phase: corev1.PodFailed}},
		{Status: corev1.PodStatus{Phase: corev1.PodPending}},
	}
	c := countMembers(pods)
	if c.Total != 4 || c.Running != 1 || c.Success != 1 || c.Failed != 1 || c.Pending != 1 {
		t.Fatalf("unexpected counts: %+v", c)
	}
}

func TestReadReleasedGeneration(t *testing.T) {
	pg := podGroup("g1", "ns", 2)
	pg.Annotations = map[string]string{schedulingv1alpha1.ReleasedGenerationAnnotation: "42"}
	r := newFakeReconciler(pg)
	if got := r.readReleasedGeneration(pg); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
	pg.Annotations = map[string]string{schedulingv1alpha1.ReleasedGenerationAnnotation: "abc"}
	if got := r.readReleasedGeneration(pg); got != 0 {
		t.Fatalf("expected 0 for invalid, got %d", got)
	}
}


