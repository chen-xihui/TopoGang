# TopoGang 评审记录

> 评审时间：2026-08-25
> 评审角色：资深 Kubernetes 调度 / GPU Infra 专家（外部评审）
> 被评审文档：`docs/DESIGN.md` v0.1 → v0.2
> 评审结论：**通过，但需修订**（已完成全部修订）

---

## 1. 总体结论

架构方向正确（Framework 插件 + CRD + Agent 解耦）、工程化程度高、可落地性强。评审发现 3 个关键问题（C1–C3）、5 个重大问题（M1–M5）、6 个一般问题（m1–m6），均已在本轮修订中修复，`DESIGN.md` 已升级至 v0.2。

---

## 2. 问题清单与修订状态

### 2.1 P0：关键问题（Critical，已全部修订）

| ID | 位置（v0.1） | 问题摘要 | 修订内容（v0.2 位置） | 状态 |
|----|--------------|----------|----------------------|------|
| C1 | §7.4 | GPU 分配时序颠倒：文档写 device plugin 在 `Allocate` 时把 GPU UUID 写入 annotation，但调度器 `Reserve` 阶段必须预先确定具体 GPU，否则拓扑感知与按卡记账无法落地 | §7.4 重写为"决策在调度器、执行在插件"：Reserve 决策 → Bind 前写 `topogang.io/gpu-uuids` → plugin `Allocate` 读取校验生效；补充时序图 | ✅ 已修复 |
| C2 | §9.2 | "杜绝死锁"是伪解：前 N 个成员 Reserve 的资源在 Permit 等待期"占而不跑"直至超时，等待期预占问题未解决 | §7.3.1 新增 **GangPrecheck 组级预检**（PreFilter 整组贪心模拟，不满足整组拒绝、不进 Reserve）；§9.2 论证修订为"杜绝永久死锁 + 等待期预占收敛到毫秒级 + 残余代价明确声明" | ✅ 已修复 |
| C3 | §4.2 | 竞品分析缺 Kueue（2025 年 Batch 调度社区官方推荐路径），选型论证不足 | §4.2 新增 Kueue 对比行 + 完整选型论证（调度内核 vs 队列管理层定位差异；不基于 Kueue 的理由；API 对齐 `workloads.k8s.io/PodGroup` 的兼容策略） | ✅ 已修复 |

### 2.2 P1：重大问题（Major，已全部修订）

| ID | 位置（v0.1） | 问题摘要 | 修订内容（v0.2 位置） | 状态 |
|----|--------------|----------|----------------------|------|
| M1 | §7.3.3 / §9.3 | AllocationTracker "以 Agent 为权威 + 从 CRD 重建"与自身写入路径矛盾：Reserve 后 Bind 前 Pod 尚未创建，Agent 观测不到，重建必然丢状态 | §7.3.3 重写：**调度器 Reserve/Unreserve 事件为唯一写入源**；Agent `allocatedTo` 仅对账/告警；故障重建三步（CRD 恢复已 Bind + Pod 清单恢复中间态 + 重建期间暂停新 Reserve）；§9.3 权威源表格同步修订 | ✅ 已修复 |
| M2 | §8.2 / §8.3 | Score 只选节点、Reserve 才选 GPU，两处"最优域"结论可能不一致 | §8.1 定义共享 **best-fit 决策函数 `domainScore`**（容量 + 兄弟亲和 + 内聚度），§8.2 声明一致性约束、§8.3 `SelectGPUs` 强制复用同一函数 | ✅ 已修复 |
| M3 | §8.4 | 1000 Pod 大组排队首会饿死所有任务 | §6.1 新增 `spec.maxSchedulingBatch`（默认 4）；§8.4 增加组内并发调度限制 | ✅ 已修复 |
| M4 | §8.1 | 部分互联拓扑下最大团重叠，未定义选团策略 | §8.1 新增选团目标函数 `domainScore(C)`（α 容量 / β 兄弟亲和 / γ 内聚惩罚） | ✅ 已修复 |
| M5 | §9.1 | Permit 放行是调度器瞬态动作，Controller 如何感知并回写 `status.scheduledByGroup` 链路缺失 | §7.2 / §9.1 新增 **released-generation annotation 闭环**：调度器放行 CAS 递增 → Controller watch → 回写 status → phase 迁移 | ✅ 已修复 |

### 2.3 P2：一般问题（Minor，已全部修订）

| ID | 位置（v0.1） | 问题摘要 | 修订内容（v0.2 位置） | 状态 |
|----|--------------|----------|----------------------|------|
| m1 | §10.3 | 测试只覆盖调度正确性，缺拓扑收益量化验证 | §10.3 新增用例 7：同域 vs 跨域 NCCL all-reduce 对比 benchmark | ✅ 已修复 |
| m2 | §6.2 | `numaNode` 字段无对应调度逻辑 | §3.2 非目标明确列为 v2（字段已预留） | ✅ 已修复 |
| m3 | §11.1 | `topo-agent` privileged 无最小化说明 | §11.1 补充安全最小化（hostPath 白名单、readOnly rootfs、最小 capability） | ✅ 已修复 |
| m4 | §8.5 | 抢占后 WaitingPod 如何感知资源释放未说明 | §8.5 补充：驱逐 → Unreserve 释放 → 资源事件 → 框架 requeue 重新调度 | ✅ 已修复 |
| m5 | §11.1 | scheduler-plugins 与 K8s 版本矩阵未锁定 | §11.1 增加版本矩阵 1.27~1.30 + 升级 E2E 回归 | ✅ 已修复 |
| m6 | §11.2 | 指标缺抢占率/超时率/跨域占比/漂移事件 | §11.2 补充 4 项指标 | ✅ 已修复 |

---

## 3. 修订后自检

| 检查项 | 结论 |
|--------|------|
| GPU 分配时序是否闭环（决策 → annotation → 插件生效 → Agent 对账） | ✅ §7.4 时序图完整 |
| Gang 死锁论证是否站得住（永久死锁 + 等待期预占分开表述） | ✅ §9.2 论证分三层，残余代价显式声明 |
| 技术选型是否经得起面试追问（Kueue / Volcano / scheduler-plugins） | ✅ §4.2 含完整论证与兼容策略 |
| 一致性模型是否有单一权威源且无写冲突 | ✅ §7.3.3 / §9.3 已收敛 |
| 打分与实际落地的 GPU 是否一致 | ✅ §8.1–8.3 共享决策函数 |

---

## 4. 遗留事项（进入开发后验证）

- GangPrecheck 的贪心模拟在节点数量增大时的性能（M2 里程碑压测）。
- 官方 device plugin 兼容模式下拓扑命中率的实测衰减（§7.4 已声明不保证）。
- Kueue 适配层（Workload ↔ PodGroup）按需实现，不阻塞本期。

---

# 第二轮评审（2026-08-25，针对 DESIGN.md v0.2）

## 5. 总体结论

**v0.2 修订有效，C1–C3 与 M1–M5 基本修复到位；修订暴露 3 个新 Major（N1–N3）与 6 个 Minor（N4–N9），已全部修复，文档升级至 v0.3。** 本轮重点验证：batch 并发与 Permit 的交互语义、漂移修复策略的自洽性、预检竞态窗口的兜底路径。

## 6. 问题清单与修订状态

| ID | 严重度 | 位置（v0.2） | 问题摘要 | 修订内容（v0.3 位置） | 状态 |
|----|--------|--------------|----------|----------------------|------|
| N1 | Major | §8.4 | `maxSchedulingBatch` 计数口径未定义：若 Permit 等待成员占名额，`minMember > batch` 的组永远凑不齐 → 设计性死等 | §8.4 定义 `active/waiting` 双状态，Permit 返回 Wait 后**不占名额**；§7.3.1 QueueSort 同步修订；§6.1 spec 注释更新；新增用例 8 | ✅ |
| N2 | Major | §7.3.3 / §9.3 | 漂移修复自相矛盾："Agent 仅告警不覆盖"则物理占用超前的超卖无法自愈 | 漂移分类处置：① 记账超前 → 核对清理；② 物理占用超前 → **Agent 为真相 `locked` 该 GPU（安全阀）**，`SelectGPUs` 先排除 locked；重建后全量对账；用例 10 | ✅ |
| N3 | Major | §7.3.1 / §9.2 | GangPrecheck 与 Permit 间仍有竞态窗口，兜底路径（等超时）未收敛 | 新增**快速失败路径**：成员 Reject/Unschedulable → 立即整组 Reject 重排（GangMemberUnschedulable），超时仅作最后防御；用例 9 | ✅ |
| N4 | Minor | §6.1 / §7.3.1 | `released-generation` 语义模糊（放行批次 vs 成员数）；"已放行直接 Success"分支与超发拒绝冗余 | 注释明确"整组放行 bump 一次，与 minMember 解耦"；Success 分支标注为容错路径 | ✅ |
| N5 | Minor | §8.1 | `domainScore` α 项在容量不满足时语义不清，与强制 nvlink Filter 冲突 | **硬约束与软打分分离**：容量不足 / 含 locked GPU 的域先过滤，打分仅对候选域排序（α 改为容量富余度） | ✅ |
| N6 | Minor | §7.4 / §10.3 | 官方插件兼容路径"命中不保证"与用例 3"命中率 100%"口径冲突 | 官方插件标注为**降级兼容路径**，用例 3 限定自研插件模式 | ✅ |
| N7 | Minor | §7.4 | `gpu-uuids` annotation 可篡改，校验基准未明确 | device plugin 以 **kubelet device manager checkpoint 为物理基准**校验；预留 admission webhook 增强项 | ✅ |
| N8 | Minor | §7.2 / §9.1 | Job 自动重试时 PodGroup 状态如何重置未定义 | 计数随 Pod 事件自然回退，无需显式重置；Controller 不做 Job 生命周期感知 | ✅ |
| N9 | Minor | §10.3 | 大组对队列的阻塞缺基准验证 | 新增用例 11：1000 成员大组 + 100 小任务混跑，小任务延迟 p99 不劣化 > 2 倍 | ✅ |

## 7. 修订后自检

| 检查项 | 结论 |
|--------|------|
| `minMember > maxSchedulingBatch` 的组能否原子放行（无死等） | ✅ §8.4 计数口径明确，用例 8 覆盖 |
| 超卖漂移能否自愈（物理占用超前） | ✅ §7.3.3 locked 安全阀 + 重建对账，用例 10 覆盖 |
| 预检竞态窗口是否有事件级兜底（非超时级） | ✅ §7.3.1 快速失败路径，用例 9 覆盖 |
| 兼容路径（官方 device plugin）口径是否统一 | ✅ §7.4 降级标注 + §10.3 限定范围 |
| 打分/选卡/封锁三处 GPU 视图是否一致 | ✅ §8.1 硬约束（含 locked）→ §8.3 SelectGPUs 复用 |

## 8. 遗留事项（更新）

- 快速失败路径在成员并发 Reject 风暴下的性能（M2 压测补充）。
- `locked` 安全阀的人工/自动解锁策略细节（M3 实现期细化）。
- N1 的 `active/waiting` 状态在 leader 切换后的恢复（归属 AllocationTracker 重建，M3 验证）。

---

# 第三轮评审（2026-08-25，针对 DESIGN.md v0.3）

## 9. 总体结论

**v0.3 修订有效，N1–N9 基本闭环；本轮发现 1 个 Critical（R1：Permit 放行条件 off-by-one，正常形态下 Gang 放行机制永久失效）、4 个 Major（R2–R5）、6 个 Minor（R6–R12），已全部修复，文档升级至 v0.4。R1 为机制级错误，必须在编码前修复并在 M1 单测固化（"恰好 N 成员 → 第 N 个触发放行"）。**

## 10. 问题清单与修订状态

| ID | 严重度 | 位置（v0.3） | 问题摘要 | 修订内容（v0.4 位置） | 状态 |
|----|--------|--------------|----------|----------------------|------|
| R1 | Critical | §7.3.1 Permit 伪代码 | 放行判断在 `AddWaitingPod` 之前且漏算当前成员：`ScheduledByGroup+len(waiting)>=minMember` 需第 N+1 个成员进入才满足 → 恰好 N 成员的组永远无法放行，核心机制失效（与 §7.5 时序图、§10.3 用例 1/2/8 自述行为矛盾） | 伪代码改为**先 `AddWaitingPod` 后判断**（waiting 含当前成员，第 minMember 个成员加入即触发放行）；§7.3.1 关键点、§9.2 论证同步补充；M1 首条单测固化 | ✅ |
| R2 | Major | §7.3.1 GangPrecheck | Running 组补位成员（未调度成员=1）会被"模拟结果 ≥ minMember"误杀，组无法自愈 | 预检作用域限定**未放行组**（`phase ∈ {PreScheduling, Scheduling}`），已放行组补位按单 Pod 调度；比较基准改为"未调度成员全部可放置" | ✅ |
| R3 | Major | §7.4 | 官方插件模式 kubelet 随机选卡与 GPU 级记账必然不一致 → N2 `locked` 演化为全局锁死风暴 | 官方模式**关闭 GPU 级记账**（不 `SelectGPUs`、不写 annotation），`locked` **降级为仅告警不锁定**；自研模式恢复 | ✅ |
| R4 | Major | §8.2 | domainScore（域级）与 TopoAffinity/GangAffinity（节点级）两套打分并存，调用层级未定义，"共享决策"悬空 | 明确层级：Score 对候选节点调用 `bestFitDomain` 求最优域、**以域分数直接作 TopoAffinity**；SelectGPUs 复用同一函数 | ✅ |
| R5 | Major | §8.2 | 跨节点 Gang 无"节点数最小化"，16 worker 可能散落 4 节点（4+4+4+4）而非 2 节点（8+8） | GangAffinity 拆为 `0.7·SameNodeAff + 0.3·NodeGangPacking`（已占节点数惩罚，聚拢跨节点组） | ✅ |
| R6 | Minor | §7.3.1 | 超时定时器每次新成员覆盖重置，与"组开始排队"计时语义不符（成员逐个到达会拉长总等待） | 定时器以组**首次进入等待**为基准，后续成员不重置（伪代码 `if gs.scheduleTimer == nil`） | ✅ |
| R7 | Minor | §7.3.1 | GangPrecheck 无结果缓存，每组每成员 PreFilter 重复 O(未调度成员) 模拟，大组 O(n²) | 预检结果按"拓扑 generation + 未调度成员集合"缓存，快照变化失效重算 | ✅ |
| R8 | Minor | §14 风险表 | "漂移触发 Resync（M1 修订）"残留，与 N2 修订后的 locked 处置矛盾 | 同步为漂移分类处置（N2），并注明官方模式 locked 降级（R3） | ✅ |
| R9 | Minor | §10.3 用例 11 | "不劣化 > 2 倍"表述歧义 | 改为"相对无大组基线**劣化 < 2 倍**" | ✅ |
| R10 | Minor | §8.4 / §13 | waiting 无上限、1000 成员批量放行开销未入基准；leader 切换自愈机制未写明 | 补充 waiting 规模说明 + 用例 12（批量放行）；明确 leader 切换后 waiting pod 经框架 requeue 自动重入 | ✅ |
| R11 | Minor | §7.3.1 快速失败路径 | "rejected 结论"写入链路未定义（框架无统一调度失败回调） | 约定 **Unreserve 钩子**统一上报调度结论（含失败扩展点与原因）；GangPrecheck 否决由组状态缓存直接标记 | ✅ |
| R12 | Minor | §7.3.1 PreFilter | Pod 先于 PodGroup 创建（Controller 异步）时 PreFilter 拒绝重试，时序抖动 | "组不存在"返回 **Wait 而非 Reject**，等待组就绪事件重新入队 | ✅ |

## 11. 修订后自检

| 检查项 | 结论 |
|--------|------|
| 恰好 minMember 成员的组能否由第 N 个成员触发放行 | ✅ R1 伪代码重写（先加入后检查），waiting 含当前成员 |
| Running 组补位成员能否正常调度（不被预检误杀） | ✅ R2 预检作用域限定 + 比较基准修正 |
| 官方插件模式是否会导致 locked 风暴 | ✅ R3 记账模式切换 + locked 降级为仅告警 |
| 打分与选卡是否单函数闭环（无二义） | ✅ R4 调用层级定义 |
| 跨节点组是否聚拢（减少 RDMA 跨节点流量） | ✅ R5 NodeGangPacking |
| 超时计时是否与"组开始排队"语义一致 | ✅ R6 首次进入等待为基准 |
| 快照未变时预检是否复用结果 | ✅ R7 缓存 key 定义 |

## 12. 遗留事项（更新）

- R1 的"恰好 N 成员触发放行"作为 M1 **首条单测**固化（防回归）。
- 批量放行 1000 WaitingPod 的实际开销在 M4 压测验证（用例 12）。
- NodeGangPacking 权重（0.7/0.3）在 M3 用 mock/真机数据校准。
- 官方插件模式节点级降级的实测收益（N6 遗留延续）。
- R11 Unreserve 上报通道在 M2 实现时验证覆盖度（Filter/Score/Reserve/Bind 各失败路径）。

*评审结束。后续修订请追加到本记录并更新 DESIGN.md 版本号。*

---

# 第四轮评审（2026-08-25，针对 DESIGN.md v0.4）

## 13. 总体结论

**v0.4 修订有效，R1–R12 闭环质量高（R1 off-by-one、R2 预检作用域、R3 记账模式切换均为关键修复）。本轮聚焦"部署约束 / 失败终态 / 状态重置 / 缓存正确性"四类问题，发现 4 个 Major（S1–S4）与 16 个 Minor（s1–s16），未发现 Critical。其中 S4（scheduledByGroup 回退清零）若未修复，回退重排后 Gang 语义将被静默破坏，须在编码前修复并 M1 单测固化。建议修订后升级 v0.5。**

## 14. 问题清单与修订状态

### 14.1 Major（S1–S4）

| ID | 位置（v0.4） | 问题摘要 | 建议修订 | 状态 |
|----|--------------|----------|----------|------|
| S1 | §7.3.3 / §7.4 / §9.3 | **GPU 占用感知存在"管理域外盲区"**：AllocationTracker 仅记账本调度器 Reserve/Unreserve；集群中若存在默认调度器调度的 GPU Pod、或混合官方插件节点，其占用对调度器不可见；Agent 对账与 `locked` 均为 60s 周期级 → 最长约 60s 的物理占用盲区 → Filter 放行到已占用 GPU，超卖 | 显式声明部署约束：本调度器管理域内 GPU 节点**全量**使用自研插件（`topogang.io/gpu`）且 GPU Pod 必须 `schedulerName: topogang-scheduler`（可加 admission webhook 强制）；混合/官方插件节点降级为"仅按 allocatable 数量过滤 + 不选卡"并写入 §11.1 部署拓扑；Agent 心跳过期时调度器停止该节点 GPU 分配；暴露盲区窗口告警指标 | ⬜ |
| S2 | §7.3.1（R7 修订点） | **预检缓存 key 设计自相矛盾**：① key 含"组未调度成员集合"，成员逐个 Reserve 后集合必变 → 缓存几乎必然 miss，每组每成员仍重复 O(未调度成员) 模拟，O(n²) 未缓解（与 R7 意图矛盾）；② key 缺 AllocationTracker 版本，其他组 Reserve/Release 后快照实际已变但 key 不变 → 复用过期结论 | key 改为 (拓扑 generation, AllocationTracker epoch, 未调度成员数 k)；声明组成员资源同构假设；AllocationTracker 引入单调 epoch，Reserve/Release 即递增，预检缓存按 epoch 失效；k 递减时允许增量模拟（放置 k 成功 ⇒ 放置 k-1 成功） | ⬜ |
| S3 | §7.2 / §9.1 | **Pod 永久失败时组终态迁移缺失（无限重排循环）**：N8 已声明 Controller 不做 Job 感知，状态机"成员失败且无法恢复 → Failed"无判定者；`restartPolicy: Never` / Job backoffLimit 耗尽后 Pod 进入 Failed 终态且不重建，可调度成员数恒 < minMember → "GangPrecheck 失败 → 退避 → 重排"无限循环，资源视图长期占位 | 定义 Controller 规则：组内存在终态 Failed Pod 且存活+可调度成员数无法再达 minMember 时 → phase=Failed；调度器对 Failed 组拒绝调度；用户重试通过重建 Job（组自然重算）或显式 annotation 复位 | ⬜ |
| S4 | §7.3.1 Permit / §9.1 | **`scheduledByGroup` 在超时回退时清零规则未定义**：组放行后 status.scheduledByGroup=minMember；若组超时/失败回退到 Pending 而字段不清零，调度器缓存 `ScheduledByGroup ≥ minMember` 恒真 → Permit"已放行"分支对重排后第 1 个成员即放行，其余成员按单 Pod 调度 → **All-or-Nothing 语义被破坏** | 定义"组离开 Running（超时/失败/回退）→ scheduledByGroup 置 0"；released-generation 保持单调递增（新批次重新 bump，CAS 保证）；状态机图补充回退路径字段重置说明；补用例：超时回退后重新放行必须重新凑齐 minMember（并入用例 2）；M1 单测固化 | ⬜ |

### 14.2 Minor（s1–s16）

| ID | 位置（v0.4） | 问题摘要 | 建议修订 | 状态 |
|----|--------------|----------|----------|------|
| s1 | §7.3.3 / §7.4 | `gpu-uuids` annotation 写入时机在 Reserve 阶段：未放行即持久化修改 Pod 元数据（副作用、Pod update 权限、scheduler cache 更新事件干扰） | 移至 **PreBind**（框架惯例：PreBind 用于绑定前修改 Pod；Reserve 保持纯账本操作） | ⬜ |
| s2 | §7.3.1 Permit | 无组单 Pod 在 Permit 的行为未定义（`groupState(pod)` 找不到组时） | 显式"无组 Pod 直接 Success，不进 waiting" | ⬜ |
| s3 | §7.3.1 / §8.4 | `maxSchedulingBatch` 并发限制归入 QueueSort 属职责错位——QueueSort 只排序，无法限制框架并发调度 | 明确实现机制：组状态缓存 active 计数 + PreFilter 超限返回 Wait | ⬜ |
| s4 | §7.2 | PodGroup 删除后存量 Pod（孤儿）永久 Wait（R12 后"组不存在返回 Wait"无兜底） | Controller 删除组时解绑成员 annotation，或调度器对孤儿限时拒绝 | ⬜ |
| s5 | §9.1 | `Failed→Pending（用户重试）`与 `Pending→Unknown（控制器失联）`的触发方式/写入方未定义（Controller 失联时无人写 Unknown） | 补充迁移触发机制与写入方 | ⬜ |
| s6 | §6.2 | `spec.gpus[].nvlinkDomain` 与 `spec.domains[]` 冗余，两处均可推出域归属，未定义权威方与一致性校验 | 声明 domains 为权威聚合视图，agent 写入时校验两者一致 | ⬜ |
| s7 | §6.1 / §7.3.1 / §4.2 | 组关联 annotation 命名三处不一致：`scheduling.topogang.io/group-name` / `scheduling.k8s.io/group-name` / `workloads.k8s.io` | 统一为单一名称并注明"语义对齐" | ⬜ |
| s8 | §11.2 | 指标前缀 `topo_*` 与 `topogang_*` 混用 | 统一前缀 | ⬜ |
| s9 | §7.3.2 | 拓扑 CRD 不健康降级"仅按数量过滤"时，数量来源未定义（node.status.allocatable 扩展资源 vs CRD），且该路径下无拓扑数据时超卖防护缺失 | 定义数量来源与降级路径的锁定约束 | ⬜ |
| s10 | §11.2 / §10.3 | `topogang_scheduling_duration_seconds` 口径（是否含 Permit 等待）未定义，与用例 6 p99<500ms 断言可能冲突 | 明确口径（建议拆"cycle 耗时"与"组排队/等待耗时"两指标） | ⬜ |
| s11 | §3.1 | 性能承诺"吞吐提升 2~5 倍"无引用来源 | 标注数据出处或改为"待用例 7 验证" | ⬜ |
| s12 | §11.1 | topo-agent 最小 capability 论证不足（nvidia-smi/DCGM 通常无需 SYS_ADMIN，SYS_ADMIN 本身即高危） | 按实际调用最小化 capability 并说明依据 | ⬜ |
| s13 | §14 | scheduler-plugins 自身版本未锁定（仅锁 K8s 1.27~1.30，未锁对应 release tag） | 风险表补充 scheduler-plugins 版本锁定 | ⬜ |
| s14 | §9.2 / §10.3 | 放行后个别成员 Bind 失败路径未显式声明 | 声明走 R2 补位路径（已放行组补位成员单 Pod 调度）并补用例 | ⬜ |
| s15 | §5.1 | 架构图 Sched↔Cont"拓扑缓存"虚线表述含糊（调度器为何经 Controller 取拓扑数据） | 明确调度器直接 watch NodeGpuTopology（共享 informer 工厂） | ⬜ |
| s16 | §7.2 | Controller watch NodeGpuTopology 的职责未说明（Reconcile 逻辑无对应分支） | 补充或移除该 watch | ⬜ |

## 15. 修订后自检

| 检查项 | 结论 |
|--------|------|
| 谁拥有 GPU 分配权、混部时如何防超卖（部署约束是否明确） | ⬜ S1 |
| 预检缓存是否真正 O(1) 复用、结论是否随账本变化失效 | ⬜ S2 |
| 失败终态（Pod 永久失败）是否有终局而非无限重排 | ⬜ S3 |
| 回退重排后 Gang 的 All-or-Nothing 语义是否保持 | ⬜ S4 |
| 无组单 Pod / 孤儿 Pod / Bind 失败等边界路径是否有定义 | ⬜ s2 / s4 / s14 |
| 命名、指标口径、权限声明是否一致 | ⬜ s6–s13 |

## 16. 修订闭环确认（2026-08-25）

- **S1–S4 及 s1–s16 已全部修订并随 DESIGN.md v0.5 生效**：
  - S1：§3.2/§7.3.3/§7.4/§9.3/§11.1/§11.2/§14 补充管理域约束、心跳过期停止分配、盲区窗口指标；
  - S2：§7.3.1/§7.3.3 缓存 key 改为 `(拓扑 generation, epoch, 成员数 k)` + 增量复用 + epoch 失效；
  - S3：§7.2/§7.3.1/§9.1/§14 失败终态判定与调度器拒绝 Failed 组；
  - S4：§7.2/§7.3.1/§9.1/§9.3/§10.3/§14 回退清零 + released-generation 单调性 + 用例 2 回归；
  - s1 PreBind 写入、s2 无组单 Pod、s3 并发限制机制、s4 孤儿兜底、s5 迁移触发、s6 domains 权威、s7 命名统一、s8/s10 指标口径、s9 降级数量源、s11 引用、s12 capability、s13 版本锁定、s14 Bind 失败路径、s15/s16 架构与 watch 职责——均已落实到对应章节（含附录 A/C）。
- **进入开发后的验证项**（承接 §15 遗留）：S4 回退清零作为 M1 首条单测；S2 增量模拟复杂度于 M2 压测；S1 混部实测窗口于 M3。

## 17. 遗留事项（进入开发后验证）

- S4 的"回退清零 + released-generation 单调性"交互作为 M1 首条单测（与 R1 单测并列，防回归）。
- S2 的增量模拟（k 递减）实现复杂度评估（M2 压测补充）。
- S1 混部模式（官方插件节点 + 自研插件节点）的实测命中衰减与超卖窗口量化。
- s1 PreBind 写 annotation 后，需在 M3 验证 scheduler cache 不因自身修改触发二次调度。

*评审结束。后续修订请追加到本记录并更新 DESIGN.md 版本号。*

---

# 第五轮评审（2026-08-25，针对 DESIGN.md v0.5）

## 18. 总体结论

**v0.5 修订方向正确，S1–S4 修复有效；但本轮聚焦"修订引入的边界与闭环"，发现 5 个 Major（T1–T5）与 7 个 Minor（t1–t7），未发现 Critical。** 重点问题：T2（拓扑不健康处置口径在 §7.1 与 §7.3.3/§9.3 互相矛盾）、T5（孤儿 Pod 被 s2 分支静默放行、§7.2 的限时 Reject 兜底永不触发）、T4（S4 清零后调度器缓存无失效机制）。建议修订后升级 v0.6。

## 19. 问题清单与修订状态

### 19.1 Major（T1–T5）

| ID | 位置（v0.5） | 问题摘要 | 建议修订 | 状态 |
|----|--------------|----------|----------|------|
| T1 | §7.3.3 / §7.4 / §11.1 | **管理域内节点上的 Pod 级混部盲区**：S1 只约束"整节点管理域外"，未覆盖"管理域内节点混入默认调度器 GPU Pod"（webhook 为预留增强项、非默认强制）。该 Pod 消耗 `topogang.io/gpu` 资源但不在 AllocationTracker 中 → GangPrecheck/SelectGPUs 视图乐观 → 选卡与物理冲突 → device plugin（N7 checkpoint 基准）拒绝 Allocate → 整组抖动 | ① 将 webhook 强制校验由"预留增强项"提升为**管理域默认要求**（未部署则视为不满足管理域条件）；② agent 在管理域内节点发现"非 topogang 分配的 GPU 占用"→ 对相应 GPU 打 `locked`（与 N2 同机制）；③ 补充混部场景测试（并入用例 14） | ⬜ |
| T2 | §7.1 vs §7.3.3/§9.3 | **拓扑不健康处置口径矛盾**：§7.1 采集失败→"降级为仅按数量过滤 + 不参与 SelectGPUs"（暗示仍可调度）；§7.3.3/§9.3 S1→"停止该节点新 GPU 分配"（不可调度）。两口径并存，实际行为不确定 | 统一为单一策略（建议：心跳过期→**完全停止新分配**，Filter 直接不返回该节点；仅"CRD 数据缺失但心跳正常"→ 数量过滤不选卡），并同步 §7.1/§7.3.3/§9.3/§11.2 四处表述 | ⬜ |
| T3 | §7.2 / §9.1 / §10.3 | **S3 失败判定与 Job 重试竞态**：`restartPolicy: Never` 但 backoffLimit 未耗尽时 Job 会重建 Failed Pod——"存在终态 Failed Pod"的瞬间 ≠ "无法恢复"，Controller 立即判 Failed 会误杀正在重试的组；且"判定无法恢复"隐含 Job 生命周期感知，与 N8"Controller 不做 Job 感知"边界冲突 | ① 失败判定加**观察窗口**：Failed Pod 存在且持续 ≥ T（默认 60s）期间无新成员 Pod 创建，才判 Failed；② 明确 Controller 仍不读 Job API，仅基于"Failed 终态 + 无新成员"观察，与 N8 表述对齐；③ 用例 13 补充"Job 正在重建时组不被误判"断言 | ⬜ |
| T4 | §7.3.1 / §9.1 | **S4 清零的调度器侧感知缺失（实现闭环缺口）**：Controller 将 `scheduledByGroup` 置 0 后，Permit（L519）读的是调度器本地组状态缓存；若无失效机制，缓存仍为 minMember → "已放行"分支对新批次首批成员误命中 → S4 修复实际失效 | 明确同步机制（建议三选一或组合）：① 调度器 Watch PodGroup status 变化重置缓存；② Permit 判断增加 `phase == Running` 防御（phase 非 Running 一律视为未放行）；③ 依赖 released-generation 变化事件刷新。补用例：回退后立即有补位成员入队，断言其进入等待而非直接放行 | ⬜ |
| T5 | §7.2 / §7.3.1 | **孤儿 Pod 被 s2 分支静默放行**：孤儿（有 `group-name` annotation 但 PodGroup 已删除）与无组单 Pod 在 Permit 中同为 `gs == nil`，s2 分支直接 Success → 孤儿被单独放行、破坏 Gang 语义；§7.2"孤儿等待超 60s 返回 Reject"的兜底因 Pod 根本不会等待而**永不触发** | 区分两类：`gs==nil` 且无 `group-name` annotation → 单 Pod Success；`gs==nil` 但有 annotation（孤儿）→ 返回 Wait/Reject（限时重试后 Reject）。修正 §7.2 兜底描述与伪代码 | ⬜ |

### 19.2 Minor（t1–t7）

| ID | 位置（v0.5） | 问题摘要 | 建议修订 | 状态 |
|----|--------------|----------|----------|------|
| t1 | §11.1 RBAC | s1 移 PreBind 后，写 annotation 需要 `pods/update`；Permit 写 `released-generation` 需要 `podgroups/update`——RBAC 仅列"update status" | RBAC 补充 `pods/update` + `podgroups/update`（含 annotation 写入） | ⬜ |
| t2 | §8.4 / §7.3.1 | "PreFilter 超限返回 Wait"依赖框架对 PreFilter 非 Success 状态的处理语义：框架对 PreFilter Wait 可能按 requeue 处理（周期性重试）而非挂起 → 抖动 | M2 验证框架语义；若不受支持改为 PreFilter 超限返回 Reject（快速失败 + 组退避重试），§8.4 同步 | ⬜ |
| t3 | §9.1 / §10.3 用例13 | "重建 Job 触发 Failed→Pending"与 N8（Controller 不做 Job 感知）冲突：Controller 无法感知 Job 重建；重建后新 Pod 仍关联同名 Failed PodGroup → 一直被 Reject | 明确定义：组 phase=Failed 后出现新成员 Pod（annotation 同组）→ Controller 自动重置 Pending（与 N8 不冲突：仍是 Pod 事件驱动，非读 Job API）；或强制用户先 reset 再重建并写清步骤 | ⬜ |
| t4 | §11.2 | `topogang_scheduling_cycle_seconds` 对"Permit Wait 挂起后 Allow 重入"的多次调度 attempt 累计口径未定义 | 明确按"每次调度 attempt"计，Permit 挂起时段不计入（由 gang_queue_time 覆盖） | ⬜ |
| t5 | §9.1 | s5"Pending → Unknown（失联 > 阈值）"判定条件不足：无法区分"正常排队中的 Pending"与"Controller 失联导致的停滞" | 定义判定（建议：lease 持有者变更 + 组 status 无更新超过阈值），仅告警不干预调度 | ⬜ |
| t6 | §7.3.1 / §7.3.3 | GangPrecheck 缓存以"成员数 k"为 key，依赖组成员资源同构假设；若组内成员 GPU 请求异构（2 卡/4 卡混排），k 相同但放置结果不同 → 缓存误命中 | PreFilter ③ 增加"组内成员 GPU 请求一致"强校验（异构组拒绝或退化为无缓存模拟），与缓存假设自洽 | ⬜ |
| t7 | §7.3.1 Permit | "组已放行直接 Success"分支在调度器重启后依赖 CRD status 恢复；若 Controller 回写滞后，Running 组补位成员会误等超时 | Permit 该分支增加 `phase == Running` 校验（与 T4 同源，一并修复） | ⬜ |

## 20. 修订后自检

| 检查项 | 结论 |
|--------|------|
| 管理域定义是否覆盖 Pod 级混部（非仅节点级） | ⬜ T1 |
| 拓扑不健康处置在 §7.1/§7.3.3/§9.3 是否口径统一 | ⬜ T2 |
| S3 失败判定是否与 Job 重试竞态兼容 | ⬜ T3 |
| S4 清零后调度器缓存是否有失效/防御机制 | ⬜ T4 |
| 孤儿 Pod（组删除）是否不会被单独放行 | ⬜ T5 |
| RBAC / 指标口径 / 缓存假设是否与实现自洽 | ⬜ t1/t4/t6 |

## 21. 遗留事项（进入开发后验证）

- T4/t7 的"phase==Running 防御 + 缓存失效"组合作为 M2 组件测试重点（与 S4 单测并列）。
- t2 的框架 PreFilter Wait 语义在 M2 压测前验证；如不支持，切换 Reject+退避方案。
- T3 观察窗口时长（60s 默认值）在 M3 用真实 Job 重试场景校准。
- T1 的 webhook 强制化对现有部署的影响评估（M3 部署章节）。

## 22. 修订闭环确认（2026-08-25，随 DESIGN.md v0.6 生效）

- **T1–T5 及 t1–t7 已全部修订**：
  - T1：webhook 由预留增强项提升为管理域默认要求（§5.1 架构图/§5.2 组件清单/§7.4/§11.1）；agent 对管理域内非 topogang 占用打 `locked`（§7.3.3）；用例 14 增加 Pod 级混部断言。
  - T2：§7.1/§7.3.2/§7.3.3/§9.3/§11.2 统一两级处置——心跳过期完全停止分配 vs 数据缺失数量过滤不选卡。
  - T3：§7.2 失败判定加观察窗口 T（默认 60s）+ "Failed 终态且窗口内无新成员"判定；§9.1 状态机图同步；用例 13 增加"Job 重试中不被误判"断言。
  - T4/t7：Permit"已放行"分支增加 `phase == Running` 防御（§7.3.1 伪代码 + 关键点）；§9.1 明确调度器 Watch status 失效缓存；新增用例 16。
  - T5：Permit 伪代码区分孤儿（Wait 限时 Reject）与无组单 Pod（Success）；§7.2 兜底描述修正；新增用例 17。
  - t1：§11.1 RBAC 补充 `pods/update` + `podgroups/update`；t2：§8.4 注明框架语义验证与备选方案；t3：§9.1 定义 Failed 后新成员自动重置 Pending（Pod 事件驱动）；t4：§11.2 明确按调度 attempt 计数；t5：§9.1 补充 Unknown 判定条件；t6：§7.3.1 PreFilter ③′ 组内同构强校验 + 用例 18；t7：随 T4 一并修复。
- **进入开发后的验证项**：T4/t7 缓存失效+phase 防御（M2）；t2 框架语义（M2 压测前）；T3 观察窗口校准（M3）；T1 webhook 影响评估（M3）。

*评审结束。后续修订请追加到本记录并更新 DESIGN.md 版本号。*
