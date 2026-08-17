from __future__ import annotations

from app import (
    CONFIG_FIELDS,
    Session,
    _inject_burst,
    _inject_outage,
    _pause,
    _recover,
)
from policies import ProductionPolicy
from simulator import SimulationConfig


POLICY_LABEL = "新算法：生产级安全库存（推荐）"


def _default_values() -> tuple[object, ...]:
    config = SimulationConfig()
    return tuple(getattr(config, field) for field in CONFIG_FIELDS)


def test_manual_controls_initialize_a_missing_session() -> None:
    values = _default_values()

    paused = _pause(None, POLICY_LABEL, *values)[0]
    assert isinstance(paused, Session)
    assert paused.running is False
    assert isinstance(paused.simulator.policy, ProductionPolicy)

    burst = _inject_burst(None, 25, 30, POLICY_LABEL, *values)[0]
    assert isinstance(burst, Session)
    assert burst.simulator._manual_burst_rate == 25
    assert burst.simulator._manual_burst_until == 30

    outage = _inject_outage(None, 120, 8, POLICY_LABEL, *values)[0]
    assert isinstance(outage, Session)
    assert outage.simulator._manual_outage_until == 120
    assert outage.simulator._manual_outage_multiplier == 8

    recovered = _recover(None, POLICY_LABEL, *values)[0]
    assert isinstance(recovered, Session)
    assert recovered.simulator._manual_outage_multiplier == 1
