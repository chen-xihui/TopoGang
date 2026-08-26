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
| **M2 Gang 调度**：PodGroup Controller + Gang 插件 + 双调度器 | 🚧 **进行中（核心语义已完成，真实适配待续）** |
| M3 拓扑感知：Topo 插件 + AllocationTracker + device plugin | 未开始 |
| M4 打磨：抢占 / 性能基准 / 指标 / demo | 未开始 |

## 当前代码结构

```
apis/scheduling/v1alpha1/       # PodGroup API 类型（§6.1）
apis/topology/v1alpha1/         # NodeGpuTopology API 类型（§6.2）
pkg/topo/                       # 拓扑图模型、NVLink 域划分（Bron–Kerbosch）、选团、best-fit 决策（§8.1）
pkg/agent/                      # topo-agent：Source 接口 + nvidia-smi/mock 实现 + CRD Writer（§7.1）
pkg/gang/                       # Gang 核心语义：Permit All-or-Nothing / 组状态 / GangPrecheck / 预检缓存（§7.3.1）
pkg/controller/state/           # PodGroup 状态机（§9.1：phase 迁移 / 超时 / released-generation 闭环 / 失败终态）
pkg/plugins/gang/               # Gang 插件编排（QueueSort / PreFilter / Permit / Reserve / Unreserve）
cmd/agent/                      # topo-agent 入口
```

M2 的 Gang 核心语义以**可独立单测的纯逻辑包**落地（`pkg/gang` / `pkg/controller/state` / `pkg/plugins/gang`），不依赖集群即可验证评审关键正确性项（R1 off-by-one、N1 batch 计数、S4 回退清零、T4 phase 防御、T5 孤儿 Pod、S3 失败组拒绝、N3 快速失败）。

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

## 关键算法（M1 交付，§8.1）

- **NVLink 域划分**：`pkg/topo.FindNvlinkDomains` 用 **Bron–Kerbosch 求极大团**（`DomainClique` 策略），可退化为连通分量（`DomainConnected`）。
- **选团策略**：硬约束（容量 / locked）与软打分（`domainScore = β·兄弟亲和 + α·容量富余 − γ·跨域惩罚`）分离。
- **best-fit 决策**：`pkg/topo.BestFitDomain` 是 **Score 与 SelectGPUs 共享**的最优域选择函数（§8.1 M2/R4），保证"打分评估的域 = 实际落地的域"。

## 下一步（M2 续 / M3）

M2 核心语义（Permit / GangPrecheck / 状态机）已完成；剩余为落地项：① controller-runtime 的 PodGroup Controller 对接 CRD 与 released-generation annotation 闭环；② scheduler-plugins 插件适配层与双调度器部署。随后进入 M3（Topo 插件 + AllocationTracker + device plugin）。

---

*按 `docs/DESIGN.md` §13 里程碑推进；每模块实现后同步更新设计文档的"已实现"标注。*
