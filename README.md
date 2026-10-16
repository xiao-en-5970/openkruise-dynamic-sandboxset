# OpenKruise SandboxSet 外置动态预热池

项目包含两个相互独立、使用同一算法模型的部分：

- `simulation/`：Python 离散事件仿真器和中文 Gradio 控制台；
- `controller/`：基于 OpenKruise Agents 官方 CRD 的注解驱动 Controller，直接调节
  `SandboxSet.spec.replicas`，不修改 sandbox-manager 或 OpenKruise Agents 源码。
- `observer/`：只读监听 SandboxSet、SandboxClaim 和 Sandbox 生命周期，导出可回放轨迹；
- `deploy/`：RBAC、双副本 Deployment、Service 和 PodDisruptionBudget；
- `docs/`：算法验证结论和设计记录。

仿真器和 Controller 都遵循以下 OpenKruise 语义：

- `SandboxSet.spec.replicas` 是未分配库存，即 `available + creating`。
- Sandbox 被 claim 后立即离开池，控制器立刻补齐缺口。
- `scaleStrategy.maxUnavailable` 限制同时处于 `creating` 的数量；`0` 表示不限。
- 是否允许缺货冷创建由官方 `createOnNoStock` 字段决定；本组件不拦截或改写请求。

## 运行控制台

```bash
python3 -m venv .venv
.venv/bin/pip install -r simulation/requirements.txt
.venv/bin/python simulation/app.py
```

浏览器打开 `http://127.0.0.1:7860`。页面支持：

- 恒定、Poisson、突发和潮汐流量；
- 固定或对数正态补货时延、断供注入、整批完成模式；
- 开始、暂停、重置、单步推进；
- 手动调整流量和补货故障；
- 对比固定池、原始中位数策略、提前期需求分位数和生产级安全库存策略；
- 查看 available/creating/waiting/target、时延分布、SLA 和预热成本。

## 策略

1. **固定池**：`target` 始终等于初始池大小。
2. **原始中位数快扩慢缩**：复现早期 `t / r × v` 思路中的公式
   `ceil(rate × median_refill / SLA)`，目标不足时按倍数快扩，过量时按间隔减 1；
   最近 3 个补货中有 2 个超过历史中位数指定倍数时暂停扩容。
3. **提前期需求分位数**：使用
   `rate × max(lead-SLA, 0) + z × sqrt(rate × lead)`，把允许等待和
   Poisson 安全库存分开表达。
4. **生产级安全库存**：加入短期速率、提前预测、补货分位数、容量门槛和缩容稳定窗口；
   持续无流量且无排队时直接降到配置的 `min-scale`，不对 0 做特殊处理。

以补货 40 秒、流量 2/s、最大等待 10 秒为例：

- 原公式得到 `2 × 40 / 10 = 8`；
- 持续流量满足 10 秒等待的确定性库存下界为 `2 × (40-10) = 60`；
- 零等待下界为 `2 × 40 = 80`；
- 随机/突发流量还需要安全库存。

因此“8”不是 OpenKruise 立即补货语义下的可用池大小。控制台保留原公式，
目的是通过相同流量轨迹直接比较 SLA 与成本，而不是预先假定结论。

## 批量验证

```bash
.venv/bin/python -m pytest -q
.venv/bin/python simulation/scenario_matrix.py
```

测试覆盖库存守恒、`maxUnavailable`、空闲快速降到 `min-scale`、2/3 熔断、
随机种子复现和公式反例。场景矩阵覆盖平稳、突发、潮汐、重尾补货、持续断供和受限并发。

## Kubernetes 原生 Controller

Controller 同时支持用户提出的三个简写注解：

```yaml
metadata:
  annotations:
    dynamic-pool: "true"
    min-scale: "1"
    max-scale: "10"
```

也可以使用正式开关注解 `pool-autoscaling.bohr.io/enabled: "true"`。未配置
`min-scale` 时默认保留 1 个库存；显式设置 `min-scale: "0"` 时允许空闲归零。
零池唤醒必须至少具备一种需求信号：创建中的 `SandboxClaim`，或者业务请求入口写入
下述带时间戳的 `pending-demand` 注解。

如果业务要求禁止缺货冷创建，调用方必须使用 OpenKruise Agents 官方字段显式设置
`createOnNoStock=false`。缺货时的等待、超时和 HTTP 状态码保持官方实现语义，本组件
不把它改写为 HTTP 503。

- SandboxClaim：`spec.createOnNoStock: false`；
- E2B create metadata：`e2b.agents.kruise.io/create-on-no-stock: "false"`。

还可以配置：

```yaml
pool-autoscaling.bohr.io/sla-seconds: "10"
pool-autoscaling.bohr.io/refill-seconds: "40"
pool-autoscaling.bohr.io/service-quantile: "0.99"
pool-autoscaling.bohr.io/fast-rate-window-seconds: "5"
pool-autoscaling.bohr.io/medium-rate-window-seconds: "15"
pool-autoscaling.bohr.io/rate-window-seconds: "60"
pool-autoscaling.bohr.io/pending-demand-max-age-seconds: "30"
pool-autoscaling.bohr.io/queue-boost-factor: "0.5"
pool-autoscaling.bohr.io/queue-boost-max: "10"
pool-autoscaling.bohr.io/max-scale-up-step: "4"
pool-autoscaling.bohr.io/idle-to-min-seconds: "120"
pool-autoscaling.bohr.io/scale-down-stabilization-seconds: "180"
pool-autoscaling.bohr.io/scale-down-fraction: "0.05"
pool-autoscaling.bohr.io/safety-guard: "2"
pool-autoscaling.bohr.io/refill-quantile: "0.95"
pool-autoscaling.bohr.io/min-lead-samples: "5"
pool-autoscaling.bohr.io/lead-sample-limit: "128"
pool-autoscaling.bohr.io/capacity-utilization: "0.8"
pool-autoscaling.bohr.io/supply-timeout-multiplier: "3"
pool-autoscaling.bohr.io/overflow-budget: "0.05"
pool-autoscaling.bohr.io/overflow-window-seconds: "900"
pool-autoscaling.bohr.io/min-overflow-samples: "20"
pool-autoscaling.bohr.io/drain-boost-max: "3"
```

为平滑迁移，旧的 `pool-autoscaling.bohr.io/idle-to-zero-seconds` 仍作为
`idle-to-min-seconds` 的兼容别名读取。
连续无需求且无排队达到 `idle-to-min-seconds` 后，Controller 会跳过慢速窗口和逐步缩容，
直接把目标池降到流量底线 `max(min-scale, 近 1 小时请求折算出的 raw floor)`；
`min-scale` 可以是 0，也可以是任意保底值。

仍有需求但目标低于当前池时，先按 `scale-down-stabilization-seconds` 观察
这段窗口内的最高 intent，再按 `scale-down-fraction` 逐步下调，避免脉冲后立刻泄掉库存。
影子回放同样用 `--idle-to-min-seconds`、`--scale-down-stabilization`、
`--scale-down-fraction` 调整，默认分别是 120 / 180 / 0.05。

控制器分别计算 5 秒、15 秒和 60 秒窗口速率，采用三者最大值作为计划流量：

`planned-rate = max(fast-rate, medium-rate, slow-rate)`

`target-intent = lead-time inventory + safety stock + pending demand + guard`

`SandboxClaim` 的未领取副本会自动计入 pending demand。直接 E2B 请求入口若还需要
反馈缺货排队，应原子更新目标 SandboxSet 的以下两个注解：

```yaml
pool-autoscaling.bohr.io/pending-demand: "12"
pool-autoscaling.bohr.io/pending-demand-updated-at: "2026-08-17T07:20:00Z"
```

时间戳超过 `pending-demand-max-age-seconds` 后信号自动失效，避免过期队列永久撑大
库存。只要 pending demand 大于 0，算法立即进入 `SURGE`。队列补偿按照
`ceil(pending × queue-boost-factor)` 计算，并受 `queue-boost-max` 限制，避免同一
突发同时被短窗口速率和完整队列重复放大。`createOnNoStock=true` 的 Claim 已由官方
链路直接冷创建，因此只进入需求速率，不再重复进入预热池队列补偿；
`createOnNoStock=false` 的 Claim 和外部排队信号才参与 queue boost。

所有算法配置都从各自 `SandboxSet.metadata.annotations` 读取，因此可以逐模板调整，
不需要为 Controller 创建多份配置。`doc-compiler` 当前建议：

```yaml
metadata:
  annotations:
    dynamic-pool: "true"
    min-scale: "1"
    max-scale: "500"
    pool-autoscaling.bohr.io/sla-seconds: "30"
    pool-autoscaling.bohr.io/service-quantile: "0.95"
    pool-autoscaling.bohr.io/fast-rate-window-seconds: "5"
    pool-autoscaling.bohr.io/medium-rate-window-seconds: "30"
    pool-autoscaling.bohr.io/queue-boost-factor: "0.5"
    pool-autoscaling.bohr.io/queue-boost-max: "10"
    pool-autoscaling.bohr.io/max-scale-up-step: "4"
spec:
  scaleStrategy:
    maxUnavailable: 10
```

`max-scale-up-step` 限制每轮写入的目标增量；`maxUnavailable` 是 OpenKruise
SandboxSet 的实际并发补货闸门。两者分别约束控制面跳变和供给侧并发。

生产保护能力：

- 从 Sandbox `Ready.lastTransitionTime - creationTimestamp` 自动学习补货时间分位数；
- 样本和去重检查点写入 `sandbox-system/sandbox-pool-controller-state` ConfigMap，
  Controller 重启或 leader 切换后不会重新学习；
- 根据 `maxUnavailable × capacity-utilization / refill-P95` 计算供给吞吐上限，
  区分算法期望库存 `target-intent` 与实际放行库存 `target-applied`；
- 在途 Sandbox 超过 `refill-P95 × supply-timeout-multiplier` 时进入 `DEGRADED`
  并暂停继续扩容，避免调度、镜像或节点故障时无限堆积；
- Deployment 默认双副本，通过 `sandbox-pool-controller` Lease 选主，并配置节点拓扑
  分散、滚动升级零不可用和 `minAvailable=1` 的 PodDisruptionBudget；
- Service 的 `/metrics` 暴露 Prometheus 指标，`/healthz` 和 `/readyz` 用于探针。

算法状态和关键数值也会回写到 SandboxSet 的
`pool-autoscaling.bohr.io/state`、`learned-refill-seconds`、`refill-source`、
`refill-samples`、`demand-rate-fast`、`demand-rate-medium`、`demand-rate-slow`、
`pending-demand-observed`、`queue-boost`、`supply-capacity-rate`、
`target-intent` 和 `target-applied` 注解。

## 冷启动兼容形态

保留官方 `createOnNoStock` 冷启动兜底时，池的目标从「保证池内供给」变为
「用站岗成本换冷启动溢出率」，算法按以下方式利用冷启动统计：

- **冷启动识别**：sandbox-manager 只在冷创建路径写入
  `agents.kruise.io/sandbox-priority: "100"`（`newSandboxFromSandboxSet`），
  池内补货成员不带该注解。Controller 据此把已领取 Sandbox 精确分类为
  冷启动或暖池命中，并按分钟桶持久化统计。该分类依赖此上游不变量。
- **溢出预算**（`overflow-budget`，默认 `0.05`，`0` 表示回退旧
  `service-quantile` 口径）：安全库存的 z 值改为
  `Φ⁻¹(1 − ε)`，即按「允许 ε 比例请求走冷启动」定池，而不是按延迟分位。
  ε=0.01 时 z≈2.33（与 P99 一致），ε=0.05 时 z≈1.64，ε=0.10 时 z≈1.28。
- **闭环校准**：每个缩容稳定窗评估一次窗口内实测溢出率 `r̂`
  （窗口 `overflow-window-seconds`，至少 `min-overflow-samples` 个样本）：
  `r̂ > 2ε` 时 z 上调 0.15，`r̂ = 0` 时下调 0.05，`r̂ < ε/2` 时下调 0.10；
  有效 z 被限制在 `[0.84, 3.09]`。调整量持久化在状态 ConfigMap 中，
  重启或 leader 切换不重学。
- **排空加速**：窗口内溢出率为 0（样本充足）且无 pending 且非 SURGE 时，
  缩容步幅放大为 `scale-down-fraction × drain-boost-max`（封顶 30%/轮），
  因为此时库存已被验证充足，多余部分属于纯站岗成本；1 小时流量底线
  仍然保护潮汐型周期需求。
- **补货学习防污染**：每个补货样本记录采集时刻的在途冷创建数，
  学习补货分位数时剔除冷创建竞争期的样本（干净样本足够时），
  避免「一边冷启动一边补池」的竞争把 `learned-refill-seconds` 永久抬高。

实测溢出率、有效 z 和排空加速状态通过
`pool-autoscaling.bohr.io/overflow-ratio`、`cold-served-replicas`、
`z-effective`、`drain-boost` 注解和同名 Prometheus 指标
（`sandbox_pool_overflow_ratio` 等）暴露。

1800 秒场景矩阵（40s 补货、冷启动比池补货慢 1.5 倍）显示：ε=0.05 相比旧
P99 口径空闲成本下降约 0.4%~5.4%（稳态 −5.4%，断供 −3.9%，潮汐 −0.4%），
暖池命中率与溢出率变化在 ±0.4 个百分点内；ε=0.10 成本下降 4.2%~11.3%，
溢出率上升约 0.5 个百分点。详见
[`docs/VALIDATION.md`](docs/VALIDATION.md)。

本地构建和部署：

```bash
cd controller
go test ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o sandbox-pool-controller .
docker build -t sandbox-pool-controller:dev .
kind load docker-image sandbox-pool-controller:dev --name sandbox-pool-poc
kubectl apply -f ../deploy/controller.yaml
```

完整的仿真和实集群验证记录见 [`docs/VALIDATION.md`](docs/VALIDATION.md)。

## 生产流量影子回放

观察器只有 `get/list/watch` 权限，不会 PATCH `SandboxSet`。下面以
`default/doc-compiler` 为例，把真实 Claim 需求和补货耗时回放到
`min-scale=1 / max-scale=500` 的动态策略中：

```bash
kubectl create configmap sandbox-pool-observer-script \
  -n sandbox-system \
  --from-file=observer.py=observer/observer.py \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/observer-poc.yaml

kubectl exec -n sandbox-system deployment/sandbox-pool-observer -- \
  curl -fsS http://127.0.0.1:8080/api/v1/export \
  > docs/doc-compiler-observation.json

.venv/bin/python simulation/replay_production.py \
  docs/doc-compiler-observation-traffic.json \
  --extend-with docs/doc-compiler-observation-live.json \
  --min-replicas 1 --max-replicas 500 --sla-seconds 30 \
  --idle-to-min-seconds 120 \
  --scale-down-stabilization 180 \
  --scale-down-fraction 0.05 \
  --json-output docs/doc-compiler-comparison.json \
  --html-output docs/doc-compiler-comparison.html
```

对比成本使用 `sandbox·seconds`，固定池不再用 `窗口时长 × spec` 一笔带过：

- 固定池理论供给：站岗库存 `available + 补池 creating`（目标保持 `spec`），
  加上缺货时 `createOnNoStock` 的冷创建时间；领走后到补齐的这段 creating 会计入供给；
- 动态链路：影子池内 Ready/Creating 资源时间；
- 两边都排除 Sandbox Ready 并交付后的业务运行时间。
- `窗口时长 × spec` 只作为“从没被领走过”的空转下界，不是对比页上的供给。

观察器大约每秒把空转库存积分一次；`/api/v1/export` 给出的供给资源时间是
**进程启动到这次导出**，不是某一段突发窗口。对比页每 60 秒重载最近一次导出。

生成的 [`docs/doc-compiler-comparison.html`](docs/doc-compiler-comparison.html)
先列模板客观数据（固定池规格、典型冷拉 P50/P95、常驻空转成本、全冷拉成本），
再列两边相对这条基线的提升，最后才是真实固定池 vs 动态池的同一套指标对照。

## 模型边界

- 时间按可配置步长推进，默认 1 秒；不是 Kubernetes 控制器性能压测。
- `warm_sandbox_seconds` 只计算池内 available/creating 资源，不包含已交付给
  用户后的业务运行成本。
- “2/3 超过中位数 3 倍”是启发式熔断，不是正态分布的 3σ 检验。
- 补货耗时样本以 sandbox 完成为单位；同一批次可能高度相关，可切换
  `batch` 模式让整批共享一次时延样本。
- 动态换镜像属于独立冷启动/原地升级路径，不计入正常自动扩缩容预测。
- `min-scale=0` 的池要重新唤醒，必须能从 `SandboxClaim` 或带时间戳的
  `pending-demand` 注解获得缺货需求信号。
