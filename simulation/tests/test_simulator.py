from __future__ import annotations

from policies import (
    AdaptiveMedianPolicy,
    LeadTimeQuantilePolicy,
    Observation,
    ProductionPolicy,
)
from simulator import (
    SimulationConfig,
    create_simulator,
    run_comparison,
    steady_state_bounds,
)


def test_document_example_bounds_show_eight_is_not_sufficient() -> None:
    assert steady_state_bounds(rate=2, lead=40, sla=10) == {
        "zero_wait": 80,
        "sla_wait": 60,
        "original_formula": 8,
    }


def test_openkruise_inventory_reconciles_to_target() -> None:
    config = SimulationConfig(
        duration_seconds=120,
        initial_replicas=12,
        arrival_rate=1,
        refill_median_seconds=10,
    )
    simulator = create_simulator(config, "fixed").run()
    for row in simulator.timeline[1:]:
        assert row["available"] + row["creating"] == row["target"]


def test_max_unavailable_limits_concurrent_refills() -> None:
    config = SimulationConfig(
        duration_seconds=100,
        initial_replicas=20,
        arrival_rate=8,
        refill_median_seconds=20,
        max_unavailable=3,
    )
    simulator = create_simulator(config, "fixed").run()
    assert max(int(row["creating"]) for row in simulator.timeline) <= 3


def test_adaptive_policy_fast_up_slow_down() -> None:
    policy = AdaptiveMedianPolicy(
        8,
        scale_up_factor=2,
        scale_down_interval=60,
        max_replicas=100,
    )
    high = Observation(5, 8, 3, 10, 40)
    assert policy.desired_replicas(high) == 16

    low_before_interval = Observation(30, 16, 0.1, 10, 40)
    assert policy.desired_replicas(low_before_interval) == 16

    low_after_interval = Observation(60, 16, 0.1, 10, 40)
    assert policy.desired_replicas(low_after_interval) == 15


def test_two_of_three_refill_anomalies_open_breaker() -> None:
    policy = AdaptiveMedianPolicy(8)
    for duration in (10, 10, 10, 40, 40):
        policy.observe_refill(duration)
    assert policy.breaker_open

    observation = Observation(10, 8, 10, 10, 40)
    assert policy.desired_replicas(observation) == 8


def test_quantile_policy_includes_safety_stock() -> None:
    policy = LeadTimeQuantilePolicy(service_quantile=0.99)
    target = policy.desired_replicas(Observation(0, 0, 2, 10, 40))
    assert target > 60


def test_quantile_baseline_beats_original_formula_on_steady_load() -> None:
    config = SimulationConfig(
        duration_seconds=600,
        initial_replicas=8,
        max_replicas=200,
        arrival_rate=2,
        refill_median_seconds=40,
        sla_seconds=10,
    )
    results = run_comparison(config, ("adaptive", "quantile"))
    adaptive = results["adaptive"].summary()
    quantile = results["quantile"].summary()
    assert quantile.sla_rate > adaptive.sla_rate
    assert adaptive.peak_target <= 16


def test_seed_makes_poisson_simulation_reproducible() -> None:
    config = SimulationConfig(
        duration_seconds=80,
        arrival_pattern="poisson",
        arrival_rate=2.5,
        refill_distribution="lognormal",
        seed=2026,
    )
    first = create_simulator(config, "fixed").run().summary()
    second = create_simulator(config, "fixed").run().summary()
    assert first == second


def test_production_policy_uses_safe_lead_time_inventory() -> None:
    policy = ProductionPolicy(8, service_quantile=0.99, min_guard=2)
    target = policy.desired_replicas(
        Observation(
            now=0,
            current_target=8,
            estimated_rate=2,
            sla_seconds=10,
            configured_lead_seconds=40,
        )
    )
    assert target > 60
    assert policy.intent == target


def test_production_policy_capacity_gate_preserves_intent() -> None:
    policy = ProductionPolicy(20)
    target = policy.desired_replicas(
        Observation(
            now=0,
            current_target=20,
            estimated_rate=3,
            sla_seconds=10,
            configured_lead_seconds=40,
            max_unavailable=5,
        )
    )
    assert policy.breaker_open
    assert policy.intent > target
    assert target == 20


def test_production_policy_prewarms_before_scheduled_burst() -> None:
    config = SimulationConfig(
        duration_seconds=300,
        initial_replicas=20,
        max_replicas=600,
        arrival_pattern="burst",
        arrival_rate=1,
        burst_rate=10,
        burst_start=120,
        burst_duration=60,
        refill_median_seconds=40,
        sla_seconds=10,
    )
    results = run_comparison(config, ("adaptive", "production"))
    old = results["adaptive"].summary()
    new = results["production"].summary()
    assert new.sla_rate > old.sla_rate
    assert new.p95_wait < old.p95_wait
