"""Discrete-time event simulator for an OpenKruise SandboxSet warm pool."""

from __future__ import annotations

import math
import random
import statistics
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Deque, Iterable

from policies import Observation, PoolPolicy, build_policy


@dataclass
class SimulationConfig:
    duration_seconds: int = 600
    step_seconds: float = 1.0
    seed: int = 7
    initial_replicas: int = 20
    max_replicas: int = 500
    sla_seconds: float = 10.0
    arrival_pattern: str = "constant"
    arrival_rate: float = 2.0
    burst_rate: float = 12.0
    burst_start: float = 180.0
    burst_duration: float = 60.0
    tidal_amplitude: float = 0.8
    tidal_period: float = 300.0
    refill_distribution: str = "fixed"
    refill_median_seconds: float = 40.0
    refill_sigma: float = 0.45
    outage_start: float = -1.0
    outage_duration: float = 0.0
    outage_multiplier: float = 5.0
    max_unavailable: int = 0
    replenish_mode: str = "openkruise"
    control_interval: float = 5.0
    rate_window: float = 60.0
    scale_up_factor: float = 2.0
    scale_down_interval: float = 60.0
    anomaly_multiple: float = 3.0
    service_quantile: float = 0.99
    lead_quantile: float = 0.95
    min_guard: int = 2
    production_scale_down_fraction: float = 0.10
    production_stabilization: float = 120.0
    capacity_utilization: float = 0.80
    supply_timeout_multiplier: float = 2.0

    def normalized(self) -> "SimulationConfig":
        values = asdict(self)
        values["duration_seconds"] = max(1, int(self.duration_seconds))
        values["step_seconds"] = max(0.1, float(self.step_seconds))
        values["initial_replicas"] = max(0, int(self.initial_replicas))
        values["max_replicas"] = max(
            values["initial_replicas"], int(self.max_replicas)
        )
        values["sla_seconds"] = max(0.001, float(self.sla_seconds))
        values["arrival_rate"] = max(0.0, float(self.arrival_rate))
        values["burst_rate"] = max(0.0, float(self.burst_rate))
        values["refill_median_seconds"] = max(
            values["step_seconds"], float(self.refill_median_seconds)
        )
        values["max_unavailable"] = max(0, int(self.max_unavailable))
        values["control_interval"] = max(
            values["step_seconds"], float(self.control_interval)
        )
        values["rate_window"] = max(values["step_seconds"], float(self.rate_window))
        values["lead_quantile"] = min(0.999, max(0.5, float(self.lead_quantile)))
        values["min_guard"] = max(0, int(self.min_guard))
        values["production_scale_down_fraction"] = min(
            0.5,
            max(0.01, float(self.production_scale_down_fraction)),
        )
        values["production_stabilization"] = max(
            values["control_interval"],
            float(self.production_stabilization),
        )
        values["capacity_utilization"] = min(
            0.99,
            max(0.1, float(self.capacity_utilization)),
        )
        values["supply_timeout_multiplier"] = max(
            1.0,
            float(self.supply_timeout_multiplier),
        )
        return SimulationConfig(**values)


@dataclass(order=True)
class RefillTask:
    ready_at: float
    started_at: float = field(compare=False)


@dataclass
class SimulationSummary:
    policy: str
    requests: int
    immediate: int
    completed: int
    pending: int
    sla_met: int
    sla_rate: float
    cold_wait_rate: float
    p50_wait: float
    p95_wait: float
    p99_wait: float
    max_wait: float
    final_target: int
    peak_target: int
    warm_sandbox_seconds: float
    scale_events: int
    breaker_seconds: float


def _percentile(values: Iterable[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _poisson(rng: random.Random, mean: float) -> int:
    if mean <= 0:
        return 0
    if mean > 50:
        return max(0, round(rng.gauss(mean, math.sqrt(mean))))
    limit = math.exp(-mean)
    product = 1.0
    count = 0
    while product > limit:
        count += 1
        product *= rng.random()
    return count - 1


class WarmPoolSimulator:
    """One simulation session.

    A claimed sandbox immediately leaves SandboxSet inventory.  The controller
    then reconciles ``available + creating`` back to ``target_replicas``.
    """

    def __init__(
        self,
        config: SimulationConfig,
        policy: PoolPolicy,
    ) -> None:
        self.config = config.normalized()
        self.policy = policy
        self.rng = random.Random(self.config.seed)
        self.now = 0.0
        self.target_replicas = self.config.initial_replicas
        self.available = self.config.initial_replicas
        self.refills: list[RefillTask] = []
        self.waiting: Deque[float] = deque()
        self.latencies: list[float] = []
        self.requests = 0
        self.immediate = 0
        self.completed = 0
        self.sla_met = 0
        self.warm_sandbox_seconds = 0.0
        self.breaker_seconds = 0.0
        self.peak_target = self.target_replicas
        self.scale_events = 0
        self.last_control = -self.config.control_interval
        self.arrival_history: Deque[tuple[float, int]] = deque()
        self.event_log: Deque[str] = deque(maxlen=300)
        self.timeline: list[dict[str, float | int | bool | str]] = []
        self._deterministic_arrival_credit = 0.0
        self._manual_burst_until = -1.0
        self._manual_burst_rate = 0.0
        self._manual_outage_until = -1.0
        self._manual_outage_multiplier = 1.0
        self._record()

    @property
    def creating(self) -> int:
        return len(self.refills)

    def inject_burst(self, rate: float, duration: float) -> None:
        self._manual_burst_rate = max(0.0, rate)
        self._manual_burst_until = self.now + max(0.0, duration)
        self.event_log.append(
            f"t={self.now:.0f}s 手动注入流量 {rate:.2f}/s，持续 {duration:.0f}s"
        )

    def inject_outage(self, duration: float, multiplier: float) -> None:
        self._manual_outage_until = self.now + max(0.0, duration)
        self._manual_outage_multiplier = max(1.0, multiplier)
        self.event_log.append(
            f"t={self.now:.0f}s 注入补货变慢 ×{multiplier:.1f}，持续 {duration:.0f}s"
        )

    def recover_supply(self) -> None:
        self._manual_outage_until = self.now
        self._manual_outage_multiplier = 1.0
        self.event_log.append(f"t={self.now:.0f}s 手动恢复补货")

    def _arrival_rate_at(self, at: float) -> float:
        cfg = self.config
        if at < self._manual_burst_until:
            return self._manual_burst_rate
        if cfg.arrival_pattern == "burst":
            if cfg.burst_start <= at < cfg.burst_start + cfg.burst_duration:
                return cfg.burst_rate
            return cfg.arrival_rate
        if cfg.arrival_pattern == "tidal":
            phase = 2.0 * math.pi * at / max(1.0, cfg.tidal_period)
            return max(0.0, cfg.arrival_rate * (1.0 + cfg.tidal_amplitude * math.sin(phase)))
        return cfg.arrival_rate

    def _arrival_rate(self) -> float:
        return self._arrival_rate_at(self.now)

    def _forecast_rate(self, horizon: float) -> float:
        """Forecast known scenario traffic over the next replenishment horizon."""
        horizon = max(self.config.step_seconds, horizon)
        samples = 20
        return max(
            self._arrival_rate_at(self.now + horizon * index / samples)
            for index in range(samples + 1)
        )

    def _arrivals_this_step(self, dt: float) -> int:
        mean = self._arrival_rate() * dt
        if self.config.arrival_pattern == "poisson":
            return _poisson(self.rng, mean)
        self._deterministic_arrival_credit += mean
        count = int(self._deterministic_arrival_credit)
        self._deterministic_arrival_credit -= count
        return count

    def _outage_multiplier(self) -> float:
        cfg = self.config
        if self.now < self._manual_outage_until:
            return self._manual_outage_multiplier
        if cfg.outage_start <= self.now < cfg.outage_start + cfg.outage_duration:
            return max(1.0, cfg.outage_multiplier)
        return 1.0

    def _sample_refill_duration(self) -> float:
        cfg = self.config
        median = cfg.refill_median_seconds
        if cfg.refill_distribution == "lognormal":
            duration = self.rng.lognormvariate(math.log(median), cfg.refill_sigma)
        else:
            duration = median
        return max(cfg.step_seconds, duration * self._outage_multiplier())

    def _complete_refills(self) -> None:
        ready = [task for task in self.refills if task.ready_at <= self.now]
        if not ready:
            return
        self.refills = [task for task in self.refills if task.ready_at > self.now]
        for task in ready:
            duration = task.ready_at - task.started_at
            self.policy.observe_refill(duration)
            self.available += 1
        self._serve_waiting()

    def _serve_waiting(self) -> None:
        while self.available > 0 and self.waiting:
            arrived_at = self.waiting.popleft()
            latency = max(0.0, self.now - arrived_at)
            self.available -= 1
            self.completed += 1
            self.latencies.append(latency)
            if latency <= self.config.sla_seconds:
                self.sla_met += 1

    def _accept_arrivals(self, count: int) -> None:
        self.requests += count
        immediate = min(self.available, count)
        if immediate:
            self.available -= immediate
            self.immediate += immediate
            self.completed += immediate
            self.sla_met += immediate
            self.latencies.extend([0.0] * immediate)
        for _ in range(count - immediate):
            self.waiting.append(self.now)
        self.arrival_history.append((self.now, count))

    def _estimated_rate(self) -> float:
        cutoff = self.now - self.config.rate_window
        while self.arrival_history and self.arrival_history[0][0] < cutoff:
            self.arrival_history.popleft()
        if not self.arrival_history:
            return 0.0
        elapsed = min(
            self.config.rate_window,
            max(self.config.step_seconds, self.now - self.arrival_history[0][0] + self.config.step_seconds),
        )
        return sum(count for _, count in self.arrival_history) / elapsed

    def _control(self) -> None:
        if self.now - self.last_control + 1e-9 < self.config.control_interval:
            return
        self.last_control = self.now
        oldest_creating_age = max(
            (self.now - task.started_at for task in self.refills),
            default=0.0,
        )
        desired = self.policy.desired_replicas(
            Observation(
                now=self.now,
                current_target=self.target_replicas,
                estimated_rate=self._estimated_rate(),
                sla_seconds=self.config.sla_seconds,
                configured_lead_seconds=self.config.refill_median_seconds,
                forecast_rate=self._forecast_rate(
                    self.config.refill_median_seconds
                ),
                available=self.available,
                creating=self.creating,
                waiting=len(self.waiting),
                oldest_creating_age=oldest_creating_age,
                max_unavailable=self.config.max_unavailable,
            )
        )
        desired = min(self.config.max_replicas, max(0, int(desired)))
        if desired != self.target_replicas:
            old = self.target_replicas
            self.target_replicas = desired
            self.peak_target = max(self.peak_target, desired)
            self.scale_events += 1
            self.event_log.append(
                f"t={self.now:.0f}s target {old} → {desired}"
                + ("（熔断）" if self.policy.breaker_open else "")
            )
            self._scale_down_excess()

    def _scale_down_excess(self) -> None:
        excess = self.available + self.creating - self.target_replicas
        if excess <= 0:
            return
        remove_available = min(self.available, excess)
        self.available -= remove_available
        excess -= remove_available
        if excess > 0:
            self.refills.sort(reverse=True)
            del self.refills[:excess]

    def _launch_refills(self) -> None:
        shortage = self.target_replicas - self.available - self.creating
        if shortage <= 0:
            return
        if self.config.max_unavailable > 0:
            capacity = max(0, self.config.max_unavailable - self.creating)
            shortage = min(shortage, capacity)
        if shortage <= 0:
            return
        common_duration = None
        if self.config.replenish_mode == "batch":
            common_duration = self._sample_refill_duration()
        for _ in range(shortage):
            duration = common_duration or self._sample_refill_duration()
            self.refills.append(
                RefillTask(ready_at=self.now + duration, started_at=self.now)
            )

    def _record(self) -> None:
        self.timeline.append(
            {
                "time": self.now,
                "available": self.available,
                "creating": self.creating,
                "waiting": len(self.waiting),
                "target": self.target_replicas,
                "requests": self.requests,
                "estimated_rate": self._estimated_rate(),
                "forecast_rate": self._forecast_rate(
                    self.config.refill_median_seconds
                ),
                "breaker": self.policy.breaker_open,
                "policy_intent": int(
                    getattr(self.policy, "intent", self.target_replicas)
                ),
                "policy_state": str(getattr(self.policy, "state", "NORMAL")),
                "warm_cost": self.warm_sandbox_seconds,
            }
        )

    def step(self, seconds: float | None = None) -> None:
        remaining = self.config.step_seconds if seconds is None else max(0.0, seconds)
        while remaining > 1e-9 and self.now < self.config.duration_seconds:
            dt = min(self.config.step_seconds, remaining)
            self.now += dt
            self._complete_refills()
            arrivals = self._arrivals_this_step(dt)
            self._accept_arrivals(arrivals)
            self._control()
            self._launch_refills()
            self.warm_sandbox_seconds += (self.available + self.creating) * dt
            if self.policy.breaker_open:
                self.breaker_seconds += dt
            self._record()
            remaining -= dt

    def run(self) -> "WarmPoolSimulator":
        while self.now < self.config.duration_seconds:
            self.step()
        return self

    def summary(self) -> SimulationSummary:
        pending_waits = [max(0.0, self.now - arrived) for arrived in self.waiting]
        observed_waits = self.latencies + pending_waits
        return SimulationSummary(
            policy=self.policy.name,
            requests=self.requests,
            immediate=self.immediate,
            completed=self.completed,
            pending=len(self.waiting),
            sla_met=self.sla_met,
            sla_rate=self.sla_met / self.requests if self.requests else 1.0,
            cold_wait_rate=(self.requests - self.immediate) / self.requests
            if self.requests
            else 0.0,
            p50_wait=_percentile(observed_waits, 0.50),
            p95_wait=_percentile(observed_waits, 0.95),
            p99_wait=_percentile(observed_waits, 0.99),
            max_wait=max(observed_waits, default=0.0),
            final_target=self.target_replicas,
            peak_target=self.peak_target,
            warm_sandbox_seconds=self.warm_sandbox_seconds,
            scale_events=self.scale_events,
            breaker_seconds=self.breaker_seconds,
        )


def create_simulator(config: SimulationConfig, policy_kind: str) -> WarmPoolSimulator:
    return WarmPoolSimulator(
        config,
        build_policy(
            policy_kind,
            initial_replicas=config.initial_replicas,
            max_replicas=config.max_replicas,
            scale_up_factor=config.scale_up_factor,
            scale_down_interval=config.scale_down_interval,
            anomaly_multiple=config.anomaly_multiple,
            service_quantile=config.service_quantile,
            lead_quantile=config.lead_quantile,
            min_guard=config.min_guard,
            scale_down_fraction=config.production_scale_down_fraction,
            scale_down_stabilization=config.production_stabilization,
            capacity_utilization=config.capacity_utilization,
            timeout_multiplier=config.supply_timeout_multiplier,
        ),
    )


def run_comparison(
    config: SimulationConfig,
    policy_kinds: Iterable[str] = ("fixed", "adaptive", "quantile", "production"),
) -> dict[str, WarmPoolSimulator]:
    return {
        kind: create_simulator(config, kind).run()
        for kind in policy_kinds
    }


def steady_state_bounds(rate: float, lead: float, sla: float) -> dict[str, int]:
    """Return the deterministic inventory bounds discussed in the UI."""
    rate = max(0.0, rate)
    lead = max(0.0, lead)
    sla = max(0.0, sla)
    return {
        "zero_wait": math.ceil(rate * lead),
        "sla_wait": math.ceil(rate * max(0.0, lead - sla)),
        "original_formula": math.ceil(rate * lead / max(0.001, sla)),
    }
