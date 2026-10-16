"""Cold-start compatible form: overflow statistics and calibrated sizing."""

from __future__ import annotations

from simulator import SimulationConfig, WarmPoolSimulator, create_simulator
from policies import Observation, ProductionPolicy


def test_cold_start_serves_empty_pool_without_queueing() -> None:
    config = SimulationConfig(
        duration_seconds=600,
        arrival_pattern="burst",
        arrival_rate=1,
        burst_rate=15,
        burst_start=180,
        burst_duration=45,
        initial_replicas=0,
        min_replicas=0,
        cold_start=True,
    )
    simulator = create_simulator(config, "production").run()
    summary = simulator.summary()
    assert summary.pending == 0
    assert summary.cold_served > 0
    assert summary.completed + summary.pending == summary.requests
    # Cold creation is slower than SLA by default (1.5x of 40s refill).
    assert summary.sla_rate < 1.0


def test_cold_mode_sizes_below_legacy_quantile_mode() -> None:
    base = dict(
        duration_seconds=900,
        arrival_pattern="burst",
        arrival_rate=1,
        burst_rate=15,
        burst_start=300,
        burst_duration=45,
        initial_replicas=0,
        min_replicas=0,
        cold_start=True,
    )
    tolerant = create_simulator(
        SimulationConfig(**{**base, "overflow_budget": 0.05}), "production"
    ).run().summary()
    legacy = create_simulator(
        SimulationConfig(**{**base, "overflow_budget": 0.0}), "production"
    ).run().summary()
    assert tolerant.peak_target <= legacy.peak_target
    assert tolerant.warm_sandbox_seconds < legacy.warm_sandbox_seconds


def test_overflow_ratio_tracks_window_over_cold_share() -> None:
    simulator = WarmPoolSimulator(
        SimulationConfig(cold_start=True, initial_replicas=0, min_replicas=0),
        create_simulator(
            SimulationConfig(cold_start=True, initial_replicas=0, min_replicas=0),
            "production",
        ).policy,
    )
    now = 1000.0
    simulator.served_history.extend(
        [(now - 500, True), (now - 400, False), (now - 100, True)]
    )
    simulator.now = now
    ratio, samples = simulator._overflow_stats(900)
    assert samples == 3
    assert abs(ratio - 2 / 3) < 1e-9
    ratio, samples = simulator._overflow_stats(150)
    assert samples == 1
    assert abs(ratio - 1.0) < 1e-9


def _observation(**overrides: object) -> Observation:
    values = dict(
        now=1000.0,
        current_target=50,
        estimated_rate=0.5,
        sla_seconds=10.0,
        configured_lead_seconds=40.0,
        waiting=0,
        history_rate=0.5,
    )
    values.update(overrides)
    return Observation(**values)  # type: ignore[arg-type]


def test_sustained_overflow_raises_z_and_effective_z_is_clamped() -> None:
    policy = ProductionPolicy(10, overflow_budget=0.05, min_overflow_samples=5)
    policy.desired_replicas(
        _observation(overflow_ratio=0.5, overflow_samples=50)
    )
    base_z = policy.z_effective
    for _ in range(30):
        policy.desired_replicas(
            _observation(now=policy.last_z_step + 181, overflow_ratio=0.5, overflow_samples=50)
        )
    assert policy.z_effective > base_z
    assert policy.z_effective <= 3.09


def test_zero_overflow_enables_drain_boost_and_decays_z() -> None:
    policy = ProductionPolicy(
        50,
        overflow_budget=0.05,
        min_overflow_samples=5,
        drain_boost_max=3.0,
        scale_down_fraction=0.05,
    )
    # Warm-only window with intent below target: drain boost should arm.
    policy.desired_replicas(
        _observation(overflow_ratio=0.0, overflow_samples=50)
    )
    assert policy.drain_boost_active
    # Insufficient samples disarm it.
    policy.desired_replicas(
        _observation(overflow_ratio=-1.0, overflow_samples=3)
    )
    assert not policy.drain_boost_active


def test_overflow_budget_sizes_below_legacy_quantile() -> None:
    tolerant = ProductionPolicy(10, overflow_budget=0.05, min_overflow_samples=5)
    legacy = ProductionPolicy(10, overflow_budget=0.0)
    tolerant.desired_replicas(
        _observation(overflow_ratio=-1.0, overflow_samples=0)
    )
    legacy.desired_replicas(
        _observation(overflow_ratio=-1.0, overflow_samples=0)
    )
    assert tolerant.intent < legacy.intent
