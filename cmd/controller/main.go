// topogang-controller 入口：管理 PodGroup 生命周期与状态机（§7.2 / §9.1）。
//
// 用法：
//
//	topogang-controller --leader-elect
package main

import (
	"flag"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/chenxihui/TopoGang/apis"
	"github.com/chenxihui/TopoGang/pkg/controllers"
)

func main() {
	var (
		metricsAddr          string
		enableLeaderElection bool
		probeAddr            string
		scheduleTimeout      time.Duration
		failureWindow        time.Duration
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.DurationVar(&scheduleTimeout, "schedule-timeout", 600*time.Second, "PodGroup schedule timeout.")
	flag.DurationVar(&failureWindow, "failure-window", 60*time.Second, "PodGroup failure observation window T.")

	klog.InitFlags(nil)
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 newScheme(),
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "topogang-controller.scheduling.topogang.io",
	})
	if err != nil {
		klog.Fatalf("unable to start manager: %v", err)
	}

	reconciler := controllers.NewPodGroupReconciler(mgr.GetClient(), controllers.Options{
		ScheduleTimeout:        scheduleTimeout,
		FailureObservationWindow: failureWindow,
	})
	if err := reconciler.SetupWithManager(mgr); err != nil {
		klog.Fatalf("unable to create controller: %v", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		klog.Fatalf("unable to set up health check: %v", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		klog.Fatalf("unable to set up ready check: %v", err)
	}

	klog.Info("starting topogang-controller")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		klog.Fatalf("problem running manager: %v", err)
	}
}

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	if err := apis.AddToScheme(s); err != nil {
		klog.Fatalf("unable to add topogang APIs to scheme: %v", err)
	}
	return s
}
