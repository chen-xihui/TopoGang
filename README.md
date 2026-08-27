# TopoGang

基于 Kubernetes 的 **GPU 拓扑感知 + Gang 调度器**，面向大模型分布式训练（PyTorch DDP / DeepSpeed / Megatron-LM / FSDP）的 K8s 资源调度。

设计文档见 [`docs/DESIGN.md`](docs/DESIGN.md)（v0.6，评审定稿）；评审记录见 [`docs/REVIEW.md`](docs/REVIEW.md)。

## 核心能力

| 能力 | 说明 |
|------|------|
| **GPU 拓扑感知调度** | 将训练任务的 GPU 尽量放入同一 NVLink 域（`NV1/NV2/NV3/PIX/PHB/SYS` 六级链路权重），降低 NCCL 跨域通信开销 |
| **Gang Scheduling** | `minMember` 原子放行 + 组级预检（GangPrecheck）+ 超时回滚，杜绝资源死锁 |

## 里程碑

| 阶段 | 状态 |
|------|------|
| **M1 地基**：CRD + topo-agent + 域划分/选团算法 | ✅ 已完成（核心逻辑 + 单测） |
| **M2 Gang 调度**：PodGroup Controller + Gang 插件 + 双调度器 | ✅ 已完成（Gang 语义 + Controller + 部署清单；真实 kube-scheduler framework 注册需集群环境） |
| **M3 拓扑感知**：Topo 插件 + AllocationTracker + device plugin | ✅ 已完成（AllocationTracker + Topo Filter/Score + device plugin + 对账闭环；真实设备枚举/envtest 需集群环境） |
| **M4 打磨**：抢占 / 性能基准 / 指标 / demo | ✅ **核心已完成**（组级抢占 + 性能基准 + 指标；NCCL 收益实测/面试 demo 需集群环境） |

## 当前代码结构

```
apis/scheduling/v1alpha1/       # PodGroup API 类型（§6.1）+ DeepCopy + scheme
apis/topology/v1alpha1/         # NodeGpuTopology API 类型（§6.2）+ DeepCopy + scheme
apis/scheme.go                  # 自定义 API 组 scheme 聚合
config/crd/                     # controller-gen 生成的 CRD YAML（§6）
config/rbac/                    # controller / scheduler / agent RBAC（含 pods/update，t1）
config/scheduler/               # scheduler-config.yaml（§7.3 独立 profile）
config/deploy/                  # 双调度器部署清单（scheduler/controller/agent/webhook，§11.1）
pkg/topo/                       # 拓扑图模型、NVLink 域划分（Bron–Kerbosch）、选团、best-fit 决策（§8.1）
pkg/allocator/                  # GPU AllocationTracker（§7.3.3：记账/epoch/locked 安全阀/SelectGPUs）
pkg/plugins/topo/               # 拓扑感知插件（§7.3.2：Filter/Score + 健康分级）
pkg/deviceplugin/               # topo-gpu-plugin（§7.4：gpu-uuids 校验 + checkpoint 基准 + gRPC 服务）
pkg/agent/                      # topo-agent：Source 接口 + nvidia-smi/mock 实现 + CRD Writer（§7.1）
pkg/gang/                       # Gang 核心语义：Permit All-or-Nothing / 组状态 / GangPrecheck / 预检缓存（§7.3.1）
pkg/controller/state/           # PodGroup 状态机（§9.1：phase 迁移 / 超时 / released-generation 闭环 / 失败终态）
pkg/controllers/                # PodGroup Controller Reconcile（controller-runtime，§7.2：finalizer/孤儿解绑/闭环）
pkg/plugins/gang/               # Gang 插件编排（QueueSort/PreFilter/Filter/PreBind/Permit/Reserve/Unreserve/PostFilter）
pkg/plugins/gang/preemptor.go   # 组级抢占（§8.5：整组抢占决策 + 低优受害者筛选）
pkg/metrics/                    # 可观测指标（§11.2：调度周期/排队时长/命中率/碎片率/漂移）
pkg/bench/                      # 性能基准（§10.3 用例 6/11/12：1000 Pod p99 / 批量放行 / 混跑）
test/e2e/                       # 端到端调度模拟器 + 全场景用例回归（§10.3 用例 1/2/8/9/10/14/16/17）
config/**/kustomization.yaml    # 部署清单（`kubectl kustomize config/deploy` 已渲染校验）
cmd/agent/                      # topo-agent 入口
cmd/controller/                 # topogang-controller 入口（leader election）
cmd/scheduler/                  # topogang-scheduler 入口（配置校验 + 插件注册点）
cmd/device-plugin/              # topo-gpu-plugin 入口（device plugin gRPC 服务，mock 可运行）
```

M2 的 Gang 核心语义以**可独立单测的纯逻辑包**落地（`pkg/gang` / `pkg/controller/state` / `pkg/plugins/gang`），不依赖集群即可验证评审关键正确性项（R1 off-by-one、N1 batch 计数、S4 回退清零、T4 phase 防御、T5 孤儿 Pod、S3 失败组拒绝、N3 快速失败）；PodGroup Controller 已对接 CRD 与 released-generation 闭环（fake client 单测验证）；双调度器部署清单已就绪（真实 kube-scheduler framework 注册需集群环境链接 `k8s.io/kubernetes`）。

## 快速开始

### 依赖

- Go 1.22+
- 无 GPU 环境即可通过 **mock 源** 验证调度地基逻辑

### 构建与测试

```bash
make build   # 编译所有二进制
make test    # 运行单元测试（域划分/选团/best-fit/采集解析）
```

### 运行 topo-agent（mock 模式）

```bash
make run-agent
# 等价于：
go run ./cmd/agent --node-name=node-a --source=mock --mock-spec=8-2 --interval=5s -v=2
```

支持的 mock 拓扑形态（§10.2）：

| mock-spec | 形态 |
|-----------|------|
| `8-1` | 8 卡单域（NVSwitch 全互联） |
| `8-2` | 8 卡双域（4+4，域间 PIX） |
| `4-1` | 4 卡单域 |
| `overlap` | 部分互联（极大团重叠，验证选团策略） |
| `none` | 无 NVLink（全 PIX） |

### 真机采集（有 GPU 环境）

```bash
go run ./cmd/agent --node-name=node-a --source=nvidia-smi
# 内部调用 `nvidia-smi topo -m` 解析物理互联矩阵
```

### 集群模式（真实集群，写 NodeGpuTopology CRD + 对账回填）

```bash
go run ./cmd/agent --node-name=node-a --source=nvidia-smi --writer=cluster
# 对接 NodeGpuTopology CRD，监听 Pod 的 gpu-uuids annotation 回填 allocatedTo（§7.3.3 校正路径）
```

对账漂移分类处置（§7.3.3 N2，`pkg/allocator/reconcile.go`）：
- **记账超前**（tracker 有记录、agent 观测空闲）→ 核对清理
- **物理占用超前**（agent 观测占用、tracker 空闲）→ 该 GPU 打 `locked` 安全阀，防超卖

## 关键算法（M1 交付，§8.1）

- **NVLink 域划分**：`pkg/topo.FindNvlinkDomains` 用 **Bron–Kerbosch 求极大团**（`DomainClique` 策略），可退化为连通分量（`DomainConnected`）。
- **选团策略**：硬约束（容量 / locked）与软打分（`domainScore = β·兄弟亲和 + α·容量富余 − γ·跨域惩罚`）分离。
- **best-fit 决策**：`pkg/topo.BestFitDomain` 是 **Score 与 SelectGPUs 共享**的最优域选择函数（§8.1 M2/R4），保证"打分评估的域 = 实际落地的域"。

## 里程碑达成（M1–M4 核心完成）

Gang 调度（All-or-Nothing + GangPrecheck + 状态机 + Controller）+ 拓扑感知（AllocationTracker + Topo Filter/Score + device plugin + 对账闭环）+ M4 打磨（组级抢占 + 性能基准 + 指标）。**13 个测试包全部通过**（含 test/e2e 全场景用例回归：用例 1/2/8/9/10/14/16/17），性能基准验证 1000 Pod 决策 p99 远低于 500ms 目标，部署清单 `kubectl kustomize config/deploy` 渲染校验通过。

**集群环境待补（真实集成验证）**：kube-scheduler framework 插件注册、device plugin 真实设备枚举、kind 全量 E2E、NCCL 同域 vs 跨域收益实测。

## 面试 demo 口径（简历亮点，§1.1）

- **调度内核**：Scheduler Framework 插件实现 8 个扩展点（QueueSort/PreFilter/Filter/PostFilter/Score/Reserve/Permit/PreBind）。
- **拓扑建模**：自研 GPU 拓扑图（NVLink 域 = Bron–Kerbosch 极大团 + 选团策略），六级链路权重。
- **All-or-Nothing**：GangPrecheck 组级预检 + Permit 原子放行 + 超时回滚三层机制；评审 off-by-one（R1）等关键正确性单测固化。
- **可移植性**：Source 接口解耦 nvidia-smi/DCGM/mock；Device plugin 只执行不决策。
- **生产级工程**：双调度器共存、leader election、对账漂移分类处置（locked 安全阀）、Prometheus 指标、性能基准。

---

*按 `docs/DESIGN.md` §13 里程碑推进；每模块实现后同步更新设计文档的"已实现"标注。*
