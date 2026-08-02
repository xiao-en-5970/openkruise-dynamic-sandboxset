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


if __name__ == "__main__":
    main()
