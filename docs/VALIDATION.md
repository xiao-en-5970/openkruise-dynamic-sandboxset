# 固定随机种子验证结论

> 当前支持边界（2026-08-17）：Controller 作为 OpenKruise Agents 的外置组件，
> 读写官方 SandboxSet、SandboxClaim 和 Sandbox 对象，不修改 sandbox-agent。
> SandboxClaim 的未领取副本可直接形成排队信号；直接 E2B 请求入口可以选择写入带
> 时间戳的 `pending-demand` 注解。默认 `min-scale=1`，显式配置 0 且具备上述需求
> 信号时支持空闲归零和重新唤醒。

运行方式：

```bash
.venv/bin/python -m pytest -q
.venv/bin/python simulation/scenario_matrix.py
```

Python 单元测试结果：`14 passed`；Go Controller 测试全部通过。

## 关键场景结果

| 场景 | 策略 | SLA 命中 | P95 等待 | 峰值池 | 最终排队 |
|---|---|---:|---:|---:|---:|
| 平稳 2/s | 固定池 20 | 1.67% | 420s | 20 | 900 |
| 平稳 2/s | 原始中位数策略 | 1.67% | 451s | 20 | 963 |
| 平稳 2/s | 提前期需求分位数 | 64.67% | 24s | 82 | 33 |
| 突发 15/s | 原始中位数策略 | 1.63% | 381s | 68 | 518 |
| 突发 15/s | 提前期需求分位数 | 14.80% | 76s | 396 | 39 |
| 补货并发上限 5 | 提前期需求分位数 | 1.11% | 539s | 118 | 1710 |

## 判断

1. `40s / 10s × 2/s = 8` 不能覆盖持续 2/s 流量。OpenKruise 的池吞吐上限
   近似为 `replicas / refill_lead_time`；池为 8 时只有约 0.2/s。
2. 允许等待 10 秒时，确定性库存下界约为
   `2 × (40-10) = 60`；零等待下界约为 `2 × 40 = 80`。随机流量还要增加
   安全库存。
3. 快扩慢缩能抑制目标值抖动，但扩容无法穿越 40 秒补货提前期。未预判的突发
   到来后再翻倍，前 40 秒仍然缺货。
4. `maxUnavailable` 是决定性约束。并发补货上限为 5、单次补货 40 秒时，最大
   补货能力约 0.125/s；任何目标池算法都无法承载 3/s 的持续流量。
5. “最近 3 次中 2 次超过历史中位数 3 倍”可以作为供给异常熔断，但不是 3σ。
   在重尾分布中会误报，在真正断供时又要等异常补货完成才有样本，因此生产设计
   还应结合创建超时、在途年龄和 Kubernetes Pending 原因。
6. 提前期需求分位数基线明显优于原公式，但它也不是突发预测器。要接近严格 SLA，
   需要使用历史潮汐/突发分位数提前升池，并保证底层并发补货能力不低于请求速率。

这些结果只验证策略与 OpenKruise 库存语义，不代表真实集群控制器、调度器或镜像
拉取性能；上线前仍需用生产指标回放校准补货时延分布和流量过程。

## kind 实集群算法验证

2026-08-02 在单节点 kind 集群和 OpenKruise Agents 上部署了
`controller/` 中的原生 Controller，验证结果如下：

- `SandboxSet` 初始 `replicas=0`，只使用 `dynamic-pool: "true"`、
  `min-scale: "0"`、`max-scale: "10"` 开启动态管理，其余算法参数用完整注解覆盖；
- 在约 1 秒内同时创建 6 个 `SandboxClaim`，10 秒统计窗口得到 `rate=0.6/s`；
- 参数为补货 5 秒、SLA 1 秒、P95、安全底仓 1，计算目标为 7；
- Controller 第一次看到部分请求时从 0 扩至 4，下一次 2 秒调谐看到全部请求后从
  4 扩至 7，证明上调过程不会被慢缩窗口阻挡；
- `maxUnavailable=10` 时 7 个池内 Sandbox 并行创建；pause 镜像已缓存，本次从
  发起请求到目标库存 7 全部 Ready 约 2 秒；
- 最后一次请求 20 秒后，目标从 7 直接降至 `min-scale=0`，7 个预热 Sandbox
  被删除，6 个已 Claim 的业务 Sandbox 保留，不会被池缩容误删。

## 生产保护增强验证

随后补齐并验证了生产保护能力：

- 从现存 6 个 Sandbox 的 Ready 条件学习补货 P95，因 Kubernetes 时间戳为秒级且
  pause 镜像已缓存，6 个样本均按最小 1 秒计，`learned-refill-seconds=1.000`；
- 样本、Ready 检查点和 UID 去重集合成功写入
  `sandbox-system/sandbox-pool-controller-state` ConfigMap；
- 删除当前 leader 后，新副本约 0.6 秒取得 Lease，两个 Controller Pod 均保持
  Ready，重启前后 6 个样本和学习结果一致；
- `/metrics` 已暴露需求速率、补货 P95、样本数、供给吞吐、期望/实际目标、
  available、creating、控制器状态和 leader 状态；
- 容量门槛场景中，需求为 `0.4/s`、估算供给能力为 `0.2/s`，状态进入
  `DEGRADED`；算法保留 `intent=5`，第一步只放行 `applied=1`；
- 故障镜像场景中，在途 Sandbox 超过 2 秒阈值后进入 `DEGRADED`，
  `intent=2`、`applied=1`，证明超时熔断阻止了继续堆积。
- 突发需求单元测试验证 5/15/60 秒窗口取最大值，慢窗口不会稀释快速窗口；
- 排队需求单元测试验证未领取 SandboxClaim 和新鲜 `pending-demand` 会直接增加
  `target-intent`，并可将显式 `min-scale=0` 的池从 0 唤醒；过期时间戳会被忽略。

这些结果验证控制回路和故障保护机制，不代表生产镜像、GPU 调度器或节点扩容性能。
当前仍使用定时调谐而非事件 informer，流量预测也只基于近期 Claim 窗口；生产上线
前仍应进行历史流量回放，并按模板类别校准窗口、分位数和超时倍数。
