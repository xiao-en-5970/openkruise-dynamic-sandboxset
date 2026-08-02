# OpenKruise SandboxSet 动态预热池

项目包含两个相互独立、使用同一算法模型的部分：

- `simulation/`：Python 离散事件仿真器和中文 Gradio 控制台；
- `controller/`：可部署到 Kubernetes 的注解驱动动态预热池 Controller，直接调节
  `SandboxSet.spec.replicas`。
- `deploy/`：RBAC、双副本 Deployment、Service 和 PodDisruptionBudget；
- `docs/`：算法验证结论和设计记录。

仿真器和 Controller 都遵循以下 OpenKruise 语义：

- `SandboxSet.spec.replicas` 是未分配库存，即 `available + creating`。
- Sandbox 被 claim 后立即离开池，控制器立刻补齐缺口。
- `scaleStrategy.maxUnavailable` 限制同时处于 `creating` 的数量；`0` 表示不限。
- 仿真器默认让缺货请求等待补货；实集群可通过 `createOnNoStock` 选择冷创建。

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
2. **原始中位数快扩慢缩**：复现 `docs/我的思路.md` 的公式
   `ceil(rate × median_refill / SLA)`，目标不足时按倍数快扩，过量时按间隔减 1；
   最近 3 个补货中有 2 个超过历史中位数指定倍数时暂停扩容。
3. **提前期需求分位数**：使用
   `rate × max(lead-SLA, 0) + z × sqrt(rate × lead)`，把允许等待和
   Poisson 安全库存分开表达。
4. **生产级安全库存**：加入短期速率、提前预测、补货分位数、容量门槛和缩容稳定窗口。

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

测试覆盖库存守恒、`maxUnavailable`、快扩慢缩、2/3 熔断、随机种子复现和
公式反例。场景矩阵覆盖平稳、突发、潮汐、重尾补货、持续断供和受限并发。

## Kubernetes 原生 Controller

Controller 同时支持用户提出的三个简写注解：

```yaml
metadata:
  annotations:
    dynamic-pool: "true"
    min-scale: "0"
    max-scale: "10"
```

还可以配置：

```yaml
pool-autoscaling.bohr.io/sla-seconds: "10"
pool-autoscaling.bohr.io/refill-seconds: "40"
pool-autoscaling.bohr.io/service-quantile: "0.99"
pool-autoscaling.bohr.io/rate-window-seconds: "60"
pool-autoscaling.bohr.io/idle-to-zero-seconds: "600"
pool-autoscaling.bohr.io/safety-guard: "2"
pool-autoscaling.bohr.io/refill-quantile: "0.95"
pool-autoscaling.bohr.io/min-lead-samples: "5"
pool-autoscaling.bohr.io/lead-sample-limit: "128"
pool-autoscaling.bohr.io/capacity-utilization: "0.8"
pool-autoscaling.bohr.io/supply-timeout-multiplier: "3"
```

控制器统计每个模板近期 `SandboxClaim` 数量，使用
`rate × max(refill-SLA, 0) + z × sqrt(rate × refill) + guard`
计算库存；需求上升时扩容，无请求超过 idle 时间后回落到 `min-scale`。

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
`refill-samples`、
`demand-rate`、`supply-capacity-rate`、`target-intent` 和 `target-applied` 注解。

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

## 模型边界

- 时间按可配置步长推进，默认 1 秒；不是 Kubernetes 控制器性能压测。
- `warm_sandbox_seconds` 只计算池内 available/creating 资源，不包含已交付给
  用户后的业务运行成本。
- “2/3 超过中位数 3 倍”是启发式熔断，不是正态分布的 3σ 检验。
- 补货耗时样本以 sandbox 完成为单位；同一批次可能高度相关，可切换
  `batch` 模式让整批共享一次时延样本。
- 动态换镜像属于独立冷启动/原地升级路径，不计入正常自动扩缩容预测。
