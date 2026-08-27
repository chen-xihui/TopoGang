// Package metrics 实现 TopoGang 的可观测指标（§11.2）。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Recorder 封装全部指标（§11.2）。
type Recorder struct {
	// schedulingCycleSeconds 单 Pod 单次调度 attempt 的 cycle 耗时（PreFilter→Bind，
	// 不含 Permit 等待，s10/t4 修订：按每次调度 attempt 计）。
	SchedulingCycleSeconds prometheus.Histogram
	// gangQueueTimeSeconds 组排队时长（queue→放行，含 Permit 等待）。
	GangQueueTimeSeconds prometheus.Histogram
	// gangWaitingPods 处于 Permit 等待的 Pod 数（按组）。
	GangWaitingPods *prometheus.GaugeVec
	// affinityHitRate 拓扑命中率（同域调度占比，由推导层填充）。
	AffinityHitRate prometheus.Gauge
	// fragmentRate 节点 GPU 碎片率（空闲但不可整域分配）。
	FragmentRate prometheus.Gauge
	// agentHeartbeatStale 拓扑数据过期节点数（T2 两级，按 label 区分）。
	AgentHeartbeatStale *prometheus.GaugeVec
	// preemptedPods 被抢占 Pod 数。
	PreemptedPods prometheus.Counter
	// timeoutRetries 组超时回退次数。
	TimeoutRetries prometheus.Counter
	// crossDomainRatio 跨域调度占比。
	CrossDomainRatio prometheus.Gauge
	// allocationDriftEvents 分配漂移告警数。
	AllocationDriftEvents prometheus.Counter
	// visibilityWindowSeconds GPU 占用盲区窗口。
	VisibilityWindowSeconds prometheus.Gauge
}

// New 构造并注册全部指标。
func New(reg prometheus.Registerer) *Recorder {
	f := promauto.With(reg)
	return &Recorder{
		SchedulingCycleSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "topogang_scheduling_cycle_seconds",
			Help:    "Single-pod single scheduling attempt cycle duration (PreFilter->Bind, excluding Permit wait).",
			Buckets: prometheus.DefBuckets,
		}),
		GangQueueTimeSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "topogang_gang_queue_time_seconds",
			Help:    "Gang queue time (queued->released, including Permit wait).",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 20),
		}),
		GangWaitingPods: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "topogang_gang_waiting_pods",
			Help: "Pods waiting in Permit, by group.",
		}, []string{"group", "namespace"}),
		AffinityHitRate: f.NewGauge(prometheus.GaugeOpts{
			Name: "topogang_affinity_hit_rate",
			Help: "Topology affinity hit rate (same-domain scheduling ratio).",
		}),
		FragmentRate: f.NewGauge(prometheus.GaugeOpts{
			Name: "topogang_fragment_rate",
			Help: "Node GPU fragmentation rate (free but not domain-allocatable).",
		}),
		AgentHeartbeatStale: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "topogang_agent_heartbeat_stale",
			Help: "Nodes with stale/missing topology data (T2 two-tier by label).",
		}, []string{"tier"}),
		PreemptedPods: f.NewCounter(prometheus.CounterOpts{
			Name: "topogang_preempted_pods_total",
			Help: "Total preempted pods.",
		}),
		TimeoutRetries: f.NewCounter(prometheus.CounterOpts{
			Name: "topogang_timeout_retries_total",
			Help: "Total gang timeout rollbacks.",
		}),
		CrossDomainRatio: f.NewGauge(prometheus.GaugeOpts{
			Name: "topogang_cross_domain_ratio",
			Help: "Cross-domain scheduling ratio (inverse of affinity hit rate).",
		}),
		AllocationDriftEvents: f.NewCounter(prometheus.CounterOpts{
			Name: "topogang_allocation_drift_events_total",
			Help: "Total allocation drift events (agent vs scheduler view mismatch).",
		}),
		VisibilityWindowSeconds: f.NewGauge(prometheus.GaugeOpts{
			Name: "topogang_visibility_window_seconds",
			Help: "GPU occupancy blind window (agent reconcile period vs scheduler view).",
		}),
	}
}

// RecordSchedulingCycle 记录一次调度 cycle 耗时。
func (r *Recorder) RecordSchedulingCycle(seconds float64) {
	r.SchedulingCycleSeconds.Observe(seconds)
}

// RecordGangQueue 记录一次组放行排队时长。
func (r *Recorder) RecordGangQueue(seconds float64) {
	r.GangQueueTimeSeconds.Observe(seconds)
}

// SetWaitingPods 设置某组等待 Pod 数。
func (r *Recorder) SetWaitingPods(group, namespace string, count float64) {
	r.GangWaitingPods.WithLabelValues(group, namespace).Set(count)
}

// IncPreempted 记录一次抢占。
func (r *Recorder) IncPreempted(count float64) {
	r.PreemptedPods.Add(count)
}

// IncTimeout 记录一次超时回退。
func (r *Recorder) IncTimeout() {
	r.TimeoutRetries.Inc()
}

// IncDrift 记录一次漂移告警。
func (r *Recorder) IncDrift() {
	r.AllocationDriftEvents.Inc()
}

// SetDerived 更新推导类指标（命中率/碎片率/跨域比，由推导层按节点计算）。
func (r *Recorder) SetDerived(affinityHit, fragment, crossDomain float64) {
	r.AffinityHitRate.Set(affinityHit)
	r.FragmentRate.Set(fragment)
	r.CrossDomainRatio.Set(crossDomain)
}
