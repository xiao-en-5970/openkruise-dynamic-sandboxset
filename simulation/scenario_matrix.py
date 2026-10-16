"""Run reproducible scenarios and print a Markdown comparison table."""

from __future__ import annotations

from dataclasses import replace

from simulator import SimulationConfig, run_comparison, steady_state_bounds


SCENARIOS = {
    "steady": SimulationConfig(duration_seconds=600, arrival_rate=2),
    "burst": SimulationConfig(
        duration_seconds=600,
        arrival_pattern="burst",
        arrival_rate=1,
        burst_rate=15,
        burst_start=180,
        burst_duration=45,
    ),
    "tidal": SimulationConfig(
        duration_seconds=900,
        arrival_pattern="tidal",
        arrival_rate=4,
        tidal_amplitude=0.9,
        tidal_period=300,
    ),
    "heavy-tail": SimulationConfig(
        duration_seconds=900,
        arrival_pattern="poisson",
        arrival_rate=2,
        refill_distribution="lognormal",
        refill_sigma=0.8,
    ),
    "supply-outage": SimulationConfig(
        duration_seconds=900,
        arrival_rate=2,
        outage_start=180,
        outage_duration=180,
        outage_multiplier=8,
    ),
    "maxUnavailable=5": SimulationConfig(
        duration_seconds=600,
        arrival_rate=3,
        max_unavailable=5,
    ),
}


def main() -> None:
    print(
        "| 场景 | 策略 | SLA命中 | P95等待(s) | 峰值池 | "
        "预热成本(sandbox·s) | 最终排队 |"
    )
    print("|---|---|---:|---:|---:|---:|---:|")
    for name, config in SCENARIOS.items():
        for simulator in run_comparison(config).values():
            summary = simulator.summary()
            print(
                f"| {name} | {summary.policy} | {summary.sla_rate:.2%} | "
                f"{summary.p95_wait:.1f} | {summary.peak_target} | "
                f"{summary.warm_sandbox_seconds:.0f} | {summary.pending} |"
            )

    example = replace(SCENARIOS["steady"], refill_median_seconds=40, sla_seconds=10)
    bounds = steady_state_bounds(
        example.arrival_rate,
        example.refill_median_seconds,
        example.sla_seconds,
    )
    print()
    print(
        "40s 补货、2/s、10s SLA："
        f"原公式={bounds['original_formula']}，"
        f"SLA确定性下界={bounds['sla_wait']}，"
        f"零等待下界={bounds['zero_wait']}。"
    )
    cold_main()


COLD_SCENARIOS = {
    "steady-2/s": SimulationConfig(duration_seconds=1800, arrival_rate=2),
    "burst": SimulationConfig(
        duration_seconds=1800,
        arrival_pattern="burst",
        arrival_rate=1,
        burst_rate=15,
        burst_start=600,
        burst_duration=45,
    ),
    "tidal": SimulationConfig(
        duration_seconds=1800,
        arrival_pattern="tidal",
        arrival_rate=4,
        tidal_amplitude=0.9,
        tidal_period=300,
    ),
    "supply-outage": SimulationConfig(
        duration_seconds=1800,
        arrival_rate=2,
        outage_start=600,
        outage_duration=300,
        outage_multiplier=8,
    ),
}

OVERFLOW_BUDGETS = (0.0, 0.01, 0.05, 0.10)


def cold_main() -> None:
    """Cold-start compatible experiments: warm-hit vs idle-cost trade-off.

    cold_start=True models official createOnNoStock semantics (empty-pool
    requests are served by standalone cold creation, 1.5x slower than pool
    refill). overflow_budget=0 keeps the legacy service-quantile sizing.
    """
    print()
    print(
        "| 场景 | 溢出预算 | SLA命中 | 暖池命中率 | 溢出率 | P95等待(s) | "
        "峰值池 | 池成本(sandbox·s) | 冷启动数 |"
    )
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for name, base in COLD_SCENARIOS.items():
        for budget in OVERFLOW_BUDGETS:
            config = replace(base, cold_start=True, overflow_budget=budget)
            simulator = run_comparison(
                config, policy_kinds=("production",)
            )["production"]
            summary = simulator.summary()
            warm_hit = summary.immediate / summary.requests if summary.requests else 1.0
            label = f"{budget:.0%}" if budget > 0 else "旧P99"
            print(
                f"| {name} | {label} | {summary.sla_rate:.2%} | {warm_hit:.2%} | "
                f"{summary.overflow_rate:.2%} | {summary.p95_wait:.1f} | "
                f"{summary.peak_target} | {summary.warm_sandbox_seconds:.0f} | "
                f"{summary.cold_served} |"
            )


if __name__ == "__main__":
    main()
