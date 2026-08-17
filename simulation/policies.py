"""Warm-pool sizing policies used by the simulator.

The policies intentionally have no UI or Kubernetes dependencies.  They only
consume observations and return the desired SandboxSet ``spec.replicas``.
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import dataclass
from statistics import NormalDist
from typing import Deque, Protocol


@dataclass(frozen=True)
class Observation:
    now: float
    current_target: int
    estimated_rate: float
    sla_seconds: float
    configured_lead_seconds: float
    forecast_rate: float = 0.0
    fast_rate: float = 0.0
    medium_rate: float = 0.0
    available: int = 0
    creating: int = 0
    waiting: int = 0
    oldest_wait_seconds: float = 0.0
    oldest_creating_age: float = 0.0
    max_unavailable: int = 0


class PoolPolicy(Protocol):
    name: str

    def observe_refill(self, duration: float) -> None: ...

    def desired_replicas(self, observation: Observation) -> int: ...

    @property
    def breaker_open(self) -> bool: ...


class FixedPolicy:
    name = "固定池"

    def __init__(self, replicas: int) -> None:
        self.replicas = max(0, int(replicas))

    def observe_refill(self, duration: float) -> None:
        del duration

    def desired_replicas(self, observation: Observation) -> int:
        del observation
        return self.replicas

    @property
    def breaker_open(self) -> bool:
        return False


class AdaptiveMedianPolicy:
    """The original median-based fast-up/slow-down proposal.

    ``optimal`` reproduces the original formula:
    ``rate * median_refill / SLA``.  It is deliberately kept as a policy to
    make the formula testable against queueing outcomes, rather than silently
    replacing it with a corrected formula.
    """

    name = "原始中位数快扩慢缩"

    def __init__(
        self,
        initial_replicas: int,
        *,
        history_size: int = 100,
        recent_size: int = 3,
        anomaly_multiple: float = 3.0,
        anomaly_votes: int = 2,
        scale_up_factor: float = 2.0,
        scale_down_interval: float = 60.0,
        min_replicas: int = 0,
        max_replicas: int = 10_000,
    ) -> None:
        self.target = max(min_replicas, int(initial_replicas))
        self.history: Deque[float] = deque(maxlen=max(3, history_size))
        self.recent: Deque[float] = deque(maxlen=max(3, recent_size))
        self.anomaly_multiple = max(1.0, anomaly_multiple)
        self.anomaly_votes = max(1, anomaly_votes)
        self.scale_up_factor = max(1.01, scale_up_factor)
        self.scale_down_interval = max(1.0, scale_down_interval)
        self.min_replicas = max(0, min_replicas)
        self.max_replicas = max(self.min_replicas, max_replicas)
        self.last_scale_down = 0.0

    def observe_refill(self, duration: float) -> None:
        duration = max(0.0, float(duration))
        self.history.append(duration)
        self.recent.append(duration)

    def _lead_median(self, fallback: float) -> float:
        return statistics.median(self.history) if self.history else fallback

    @property
    def breaker_open(self) -> bool:
        if len(self.history) < 3 or len(self.recent) < 3:
            return False
        historical_median = statistics.median(self.history)
        if historical_median <= 0:
            return False
        threshold = historical_median * self.anomaly_multiple
        votes = sum(value > threshold for value in self.recent)
        return votes >= self.anomaly_votes

    def desired_replicas(self, observation: Observation) -> int:
        lead = self._lead_median(observation.configured_lead_seconds)
        sla = max(0.001, observation.sla_seconds)
        optimum = math.ceil(observation.estimated_rate * lead / sla)
        optimum = min(self.max_replicas, max(self.min_replicas, optimum))

        self.target = min(
            self.max_replicas,
            max(self.min_replicas, observation.current_target, self.target),
        )
        if optimum > self.target and not self.breaker_open:
            while self.target < optimum:
                self.target = max(
                    self.target + 1,
                    math.ceil(self.target * self.scale_up_factor),
                )
                if self.target >= self.max_replicas:
                    break
        elif (
            optimum < self.target
            and observation.now - self.last_scale_down >= self.scale_down_interval
        ):
            self.target -= 1
            self.last_scale_down = observation.now
        return min(self.max_replicas, max(self.min_replicas, self.target))


class LeadTimeQuantilePolicy:
    """A demand-during-lead-time baseline with explicit safety stock.

    The deterministic term ``rate * max(lead - SLA, 0)`` bounds the expected
    queue gap when waiting up to ``SLA`` is acceptable.  Poisson variance is
    represented by ``z * sqrt(rate * lead)`` safety stock.
    """

    name = "提前期需求分位数"

    def __init__(
        self,
        *,
        service_quantile: float = 0.99,
        history_size: int = 100,
        min_replicas: int = 0,
        max_replicas: int = 10_000,
    ) -> None:
        self.service_quantile = min(0.9999, max(0.5001, service_quantile))
        self.history: Deque[float] = deque(maxlen=max(3, history_size))
        self.min_replicas = max(0, min_replicas)
        self.max_replicas = max(self.min_replicas, max_replicas)

    def observe_refill(self, duration: float) -> None:
        self.history.append(max(0.0, float(duration)))

    @property
    def breaker_open(self) -> bool:
        return False

    def desired_replicas(self, observation: Observation) -> int:
        lead = (
            statistics.median(self.history)
            if self.history
            else observation.configured_lead_seconds
        )
        rate = max(0.0, observation.estimated_rate)
        mean_demand = rate * max(0.0, lead - observation.sla_seconds)
        lead_demand = max(0.0, rate * lead)
        z_score = NormalDist().inv_cdf(self.service_quantile)
        safety_stock = z_score * math.sqrt(lead_demand)
        target = math.ceil(mean_demand + safety_stock)
        return min(self.max_replicas, max(self.min_replicas, target))


def _quantile(values: Deque[float], probability: float, fallback: float) -> float:
    if not values:
        return fallback
    ordered = sorted(values)
    position = (len(ordered) - 1) * min(1.0, max(0.0, probability))
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


class ProductionPolicy:
    """Production-oriented inventory controller.

    The policy keeps demand intent separate from the applied target, sizes the
    pool from a service quantile over replenishment lead time, predicts known
    near-term traffic, blocks unsafe scale-down, and detects supply capacity
    that cannot keep up with sustained demand. Managed production pools keep at
    least one warm sandbox because failed stock-out requests are not available
    as a zero-pool wake-up signal.
    """

    name = "新算法：生产级安全库存"

    def __init__(
        self,
        initial_replicas: int,
        *,
        service_quantile: float = 0.99,
        lead_quantile: float = 0.95,
        history_size: int = 100,
        min_guard: int = 2,
        scale_down_fraction: float = 0.10,
        scale_down_stabilization: float = 120.0,
        capacity_utilization: float = 0.80,
        timeout_multiplier: float = 2.0,
        min_replicas: int = 1,
        max_replicas: int = 10_000,
    ) -> None:
        self.min_replicas = max(1, int(min_replicas))
        self.max_replicas = max(self.min_replicas, int(max_replicas))
        self.target = min(
            self.max_replicas,
            max(self.min_replicas, int(initial_replicas)),
        )
        self.intent = self.target
        self.state = "NORMAL"
        self.service_quantile = min(0.9999, max(0.5001, service_quantile))
        self.lead_quantile = min(0.999, max(0.5, lead_quantile))
        self.history: Deque[float] = deque(maxlen=max(10, history_size))
        self.desired_history: Deque[tuple[float, int]] = deque()
        self.min_guard = max(0, int(min_guard))
        self.scale_down_fraction = min(0.5, max(0.01, scale_down_fraction))
        self.scale_down_stabilization = max(1.0, scale_down_stabilization)
        self.capacity_utilization = min(0.99, max(0.1, capacity_utilization))
        self.timeout_multiplier = max(1.0, timeout_multiplier)
        self.fast_rate = 0.0
        self.slow_rate = 0.0
        self.planned_rate = 0.0
        self.queue_boost = 0
        self.recovering_until = 0.0

    def observe_refill(self, duration: float) -> None:
        self.history.append(max(0.0, float(duration)))

    @property
    def breaker_open(self) -> bool:
        return self.state == "DEGRADED"

    def _lead_time(self, fallback: float) -> float:
        return _quantile(self.history, self.lead_quantile, fallback)

    def _supply_degraded(
        self,
        observation: Observation,
        planned_rate: float,
        lead: float,
    ) -> bool:
        if (
            observation.creating > 0
            and observation.oldest_creating_age
            > self.timeout_multiplier * max(1.0, lead)
        ):
            return True
        if observation.max_unavailable > 0:
            safe_capacity = (
                self.capacity_utilization
                * observation.max_unavailable
                / max(0.001, lead)
            )
            if planned_rate > safe_capacity:
                return True
        return False

    def desired_replicas(self, observation: Observation) -> int:
        measured = max(0.0, observation.estimated_rate)
        self.fast_rate = measured if self.fast_rate == 0 else 0.5 * measured + 0.5 * self.fast_rate
        self.slow_rate = measured if self.slow_rate == 0 else 0.15 * measured + 0.85 * self.slow_rate
        planned_rate = max(
            measured,
            self.fast_rate,
            self.slow_rate,
            max(0.0, observation.fast_rate),
            max(0.0, observation.medium_rate),
            max(0.0, observation.forecast_rate),
        )
        self.planned_rate = planned_rate
        lead = self._lead_time(observation.configured_lead_seconds)
        horizon = max(0.0, lead - observation.sla_seconds)
        base_inventory = planned_rate * horizon
        z_score = NormalDist().inv_cdf(self.service_quantile)
        safety_stock = z_score * math.sqrt(max(0.0, planned_rate * lead))
        # Every queued request represents one missing ready sandbox. Add that
        # inventory debt to the steady-state target so a growing queue cannot
        # remain invisible behind a smoothed demand rate.
        self.queue_boost = min(
            self.max_replicas,
            max(0, int(observation.waiting)),
        )
        raw_intent = math.ceil(
            base_inventory + safety_stock + self.queue_boost + self.min_guard
        )
        self.intent = min(
            self.max_replicas,
            max(self.min_replicas, raw_intent),
        )

        self.desired_history.append((observation.now, self.intent))
        cutoff = observation.now - self.scale_down_stabilization
        while self.desired_history and self.desired_history[0][0] < cutoff:
            self.desired_history.popleft()

        if self._supply_degraded(observation, planned_rate, lead):
            self.state = "DEGRADED"
            self.target = max(self.target, observation.current_target)
            return self.target

        if self.state == "DEGRADED":
            self.state = "RECOVERING"
            self.recovering_until = observation.now + max(lead, 30.0)

        if self.intent > self.target:
            if self.state == "RECOVERING":
                step = max(10, math.ceil(max(1, self.target) * 0.20))
                self.target = min(self.intent, self.target + step)
            else:
                self.target = self.intent
                self.state = (
                    "SURGE"
                    if observation.waiting > 0 or planned_rate > measured
                    else "NORMAL"
                )
        elif self.intent < self.target:
            if observation.now < self.recovering_until:
                self.state = "RECOVERING"
                return self.target
            stabilized_floor = max(
                (desired for _, desired in self.desired_history),
                default=self.intent,
            )
            if stabilized_floor < self.target and observation.waiting == 0:
                reduction = max(1, math.ceil(self.target * self.scale_down_fraction))
                self.target = max(stabilized_floor, self.target - reduction)
            self.state = "NORMAL"
        elif self.state != "RECOVERING":
            self.state = "SURGE" if observation.waiting > 0 else "NORMAL"

        return min(
            self.max_replicas,
            max(self.min_replicas, self.target),
        )


def build_policy(
    kind: str,
    *,
    initial_replicas: int,
    max_replicas: int,
    scale_up_factor: float = 2.0,
    scale_down_interval: float = 60.0,
    anomaly_multiple: float = 3.0,
    service_quantile: float = 0.99,
    lead_quantile: float = 0.95,
    min_guard: int = 2,
    scale_down_fraction: float = 0.10,
    scale_down_stabilization: float = 120.0,
    capacity_utilization: float = 0.80,
    timeout_multiplier: float = 2.0,
) -> PoolPolicy:
    if kind == "fixed":
        return FixedPolicy(initial_replicas)
    if kind == "adaptive":
        return AdaptiveMedianPolicy(
            initial_replicas,
            max_replicas=max_replicas,
            scale_up_factor=scale_up_factor,
            scale_down_interval=scale_down_interval,
            anomaly_multiple=anomaly_multiple,
        )
    if kind == "quantile":
        return LeadTimeQuantilePolicy(
            max_replicas=max_replicas,
            service_quantile=service_quantile,
        )
    if kind == "production":
        return ProductionPolicy(
            initial_replicas,
            max_replicas=max_replicas,
            service_quantile=service_quantile,
            lead_quantile=lead_quantile,
            min_guard=min_guard,
            scale_down_fraction=scale_down_fraction,
            scale_down_stabilization=scale_down_stabilization,
            capacity_utilization=capacity_utilization,
            timeout_multiplier=timeout_multiplier,
        )
    raise ValueError(f"unknown policy kind: {kind}")
