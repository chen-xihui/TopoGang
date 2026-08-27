M2 核心语义已开发完成，全部编译、vet、测试通过。

本轮完成内容（M2 Gang 调度核心）
新增 3 个包，7 个源文件 + 5 个测试文件：

模块	文件	设计章节
Gang 核心语义	pkg/gang/state.go、permit.go、precheck.go	§7.3.1 / §8.4
PodGroup 状态机	pkg/controller/state/statemachine.go	§9.1
Gang 插件编排	pkg/plugins/gang/plugin.go	§7.3.1
评审关键正确性项全部单测固化（REVIEW 明确要求）
评审项	语义	单测
R1	恰好 N 成员，第 N 个触发放行（off-by-one）	TestPermit_ExactlyNTriggersRelease
N1	minMember > batch 无死等（waiting 不占 batch）	TestPermit_MinMemberGreaterThanBatch
S4	超时回退清零，重新排队必须重新凑齐	TestPermit_TimeoutRollbackClearsScheduledByGroup
T4/t7	phase==Running 防御（缓存残留不误放）	TestPermit_NotRunningDoesNotRelease + 正向
T5	孤儿 Pod（Wait）vs 无组单 Pod（Success）	TestPermit_OrphanVsSinglePod
S3	Failed 组拒绝（防无限重排）	TestPermit_FailedGroupRejects
N3	快速失败整组 Reject	TestReleaseWaiting_FastFail
R6	定时器首次进入等待为基准	TestPermit_TimerStartsOnFirstWait
另有 S2（预检缓存 epoch 失效）、T3（失败观察窗口）、t3（Failed 后新成员重置 Pending）、R2（Running 补位跳过预检）、batch 并发限制等均覆盖。

设计要点
解耦：Gang 核心逻辑以纯逻辑包落地，不依赖 scheduler-plugins 框架，无集群可验证正确性；pkg/plugins/gang 提供框架语义的编排适配层。
防御性设计：T4 在 Permit 中"phase 非 Running 一律视为未放行"，计数从 0 起算，杜绝缓存残留导致回退组被误放。
下一步
M2 剩余落地项：① controller-runtime 的 PodGroup Controller 对接 CRD + released-generation annotation 闭环 + finalizer；② scheduler-plugins 插件适配与双调度器部署；③ envtest 组件测试。之后进入 M3（Topo 插件 + AllocationTracker + device plugin）。


M2 落地项（controller-runtime 版 PodGroup Controller）已完成，全部编译、vet、测试通过。

本轮完成内容（M2 Controller 落地）
新增/修改文件：

模块	文件	说明
PodGroup Controller	pkg/controllers/podgroup_controller.go	§7.2 Reconcile：finalizer / 孤儿解绑(s4) / released-generation 闭环 / 超时 / 失败终态
API 基建	apis/scheme.go、groupversion_info.go、zz_generated.deepcopy.go	scheme 注册 + DeepCopy + kubebuilder markers
CRD YAML	config/crd/bases/*.yaml + kustomization.yaml	controller-gen 生成 PodGroup / NodeGpuTopology
RBAC	config/rbac/role.yaml	§11.1 t1：含 pods/update + podgroups/update
controller 入口	cmd/controller/main.go	leader election + healthz + metrics
单测	pkg/controllers/podgroup_controller_test.go	fake client 验证闭环/超时/失败终态/孤儿解绑
Controller 覆盖的评审语义
released-generation 闭环：调度器写 annotation → Controller 观测 ScheduledByGroup ≥ minMember → 迁移 Running（§9.1）
超时回退 S4：Scheduling 超时 → Pending + scheduledByGroup 清零
失败终态 S3/T3：全部终态含失败 → Failed；有存活成员在观察窗口内 → 不误判（Job 重试保护）
孤儿解绑 s4：删除 PodGroup 时移除成员 group-name annotation
t3：Failed 后出现新成员 → 自动重置 Pending
工程完善
CRD 用 controller-gen 生成（make gen-crd），含 allowDangerousTypes（拓扑带宽 float 字段）
两个二进制均可构建：topo-agent.exe、topogang-controller.exe
DeepCopy 手动补齐（controller-gen object 生成器对嵌套类型支持不完整，已改为维护完整的 zz_generated.deepcopy.go）
当前 M2 完成度
✅ 核心语义（Permit/GangPrecheck/状态机）+ Controller Reconcile + CRD/RBAC + fake client 单测 ⬜ 剩余：scheduler-plugins 插件适配层、双调度器部署清单、envtest 组件测试、M2 压测

下一步
可推进：M2 尾期（scheduler-plugins 适配 + 双调度器部署），或直接进入 M3（Topo 插件 Filter/Score + AllocationTracker + device plugin，与 M1 的 best-fit 决策对接）。

本轮完成内容（M2 尾期）
新增/完善文件：

模块	文件	说明
Gang 插件扩展点补齐	pkg/plugins/gang/plugin.go	补充 Filter / PreBind / PostFilter，完整覆盖 §7.3.1 全部扩展点编排（可单测）
扩展点单测	pkg/plugins/gang/plugin_extensions_test.go	Filter 恒放行 / PreBind 写 gpu-uuids(s1) / PostFilter 组级抢占(§8.5)
scheduler 配置	config/scheduler/scheduler-config.yaml	§7.3 独立 profile（QueueSort/PreFilter/Filter/PostFilter/Score/Reserve/Permit/Bind + pluginConfig）
scheduler 入口	cmd/scheduler/main.go	配置校验 + 插件注册点骨架
双调度器部署清单	config/deploy/*.yaml	scheduler/controller/agent/webhook Deployment + namespace + SA
scheduler RBAC	config/rbac/scheduler-rbac.yaml	含 podgroups/update(released-generation) + pods/update(gpu-uuids)，t1
agent RBAC	config/rbac/agent-rbac.yaml	写 NodeGpuTopology
kustomization	config/deploy/kustomization.yaml	一键 kubectl apply -k
关键设计决策
真实 kube-scheduler framework 注册依赖 k8s.io/kubernetes（重量级，下载被跳过、无集群无法运行）。因此采用纯逻辑编排 + 部署清单交付：Gang 插件扩展点的完整逻辑在 pkg/plugins/gang/plugin.go（可单测），映射 kube-scheduler 接口的部分作为文档化的集群环境集成项（§7.3.1 表格）。
引入 scheduler-plugins 依赖后与 controller-runtime 的 client-go 版本冲突，已通过 go mod tidy 协调。
部署清单覆盖的 §11.1 约束
webhook（T1）：强制 GPU Pod schedulerName，未部署即视为不满足管理域条件
topo-agent（m3/s12）：hostPath 白名单 + readOnly rootfs + 最小 capability
双调度器共存：独立 schedulerName: topogang-scheduler + leader election
M2 完成度总结
✅ Gang 核心语义（Permit/GangPrecheck/状态机）+ 全部评审关键单测
✅ PodGroup Controller（released-generation 闭环/finalizer/孤儿解绑）
✅ CRD YAML + RBAC + 双调度器部署清单 + scheduler 配置
⬜ 集群环境待补：真实 kube-scheduler framework 注册、envtest、M2 压测（归入 M4 统一压测）
三个可执行文件均可构建：topo-agent.exe、topogang-controller.exe、topogang-scheduler.exe。

下一步
进入 M3（拓扑感知）：Topo 插件（Filter/Score）+ AllocationTracker + best-fit 决策落地（对接 M1 的 pkg/topo/bestfit.go）+ topo-gpu-plugin + 对账。要继续 M3 吗？

本轮完成内容（M3）
新增 3 个包 + 1 个入口：

模块	文件	设计章节
GPU AllocationTracker	pkg/allocator/allocator.go	§7.3.3
Topo 插件（Filter/Score）	pkg/plugins/topo/topo_plugin.go	§7.3.2 / §8.2
topo-gpu-plugin 分配逻辑	pkg/deviceplugin/allocator.go	§7.4
topo-gpu-plugin gRPC 服务	pkg/deviceplugin/plugin.go	§7.4
device plugin 入口	cmd/device-plugin/main.go	§5.2
AllocationTracker 覆盖的评审项
记账：Allocate/Release/FreeGPUs，以调度器 Reserve/Unreserve 为唯一写入源（M1）
epoch 单调递增：任何 Reserve/Release 递增 → GangPrecheck 预检缓存失效（S2）
locked 安全阀：物理占用超前时封锁 GPU，SelectGPUs/Allocate 排除（N2/T1）
管理域约束：心跳过期节点完全停止分配（T2）
共享 best-fit：SelectGPUs 复用 topo.BestFitDomain，保证"打分评估的域=实际落地的域"（§8.1 M2/R4）
Topo 插件
Filter：数量过滤 + 强制 nvlink 单域容量 + 拓扑健康三级（心跳过期拒绝/数据缺失仅数量过滤不选卡）
Score：W1·TopoAffinity + W2·GangAffinity + W3·Balance（§8.2，含兄弟亲和、跨节点聚拢 R5）
device plugin（§7.4）
上报 topogang.io/gpu 扩展资源
Allocate 读取 gpu-uuids annotation，以 kubelet checkpoint 为物理基准校验（N7），冲突检测防伪造
"决策在调度器、执行在插件"（C1），mock 模式无 GPU 可运行
单测覆盖（全部通过）
Allocator：记账/双分配冲突/locked 错误/SelectGPUs 单域装箱/兄弟亲和/心跳过期拒绝/NoFit/epoch 单调/域分桶
Topo 插件：数量过滤/强制 nvlink 拒绝与通过/健康分级/Score 健康 vs 不健康/Balance/GangAffinity/与 allocator 集成
Deviceplugin：gpu-uuids 解析/校验通过/计数不匹配/冲突检测(N7)/设备不存在/自占用幂等
当前进度
✅ M1 / M2 / M3 核心全部完成
四个可执行文件可构建：topo-agent、topogang-controller、topogang-scheduler、topo-gpu-plugin
M3 剩余项（真实对账闭环）
① topo-agent 对接真实集群 Writer 与 allocatedTo 对账；② device plugin 真实设备枚举；③ 对账漂移告警指标。

下一步
可推进：M3 剩余项（topo-agent 集群对账闭环）或直接进入 M4（组级抢占落地 + 性能基准 + 指标/日志 + NCCL 收益 demo）。要继续吗？