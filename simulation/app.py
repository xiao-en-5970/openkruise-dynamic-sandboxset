"""Interactive Gradio control window for the warm-pool simulator."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any

import gradio as gr
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from simulator import (
    SimulationConfig,
    WarmPoolSimulator,
    create_simulator,
    run_comparison,
    steady_state_bounds,
)


POLICIES = {
    "旧算法：中位数 + 翻倍扩容": "adaptive",
    "新算法：生产级安全库存（推荐）": "production",
    "参考：固定大小预热池": "fixed",
    "参考：简单提前期分位数": "quantile",
}

CONFIG_FIELDS = [
    "duration_seconds",
    "seed",
    "initial_replicas",
    "max_replicas",
    "sla_seconds",
    "arrival_pattern",
    "arrival_rate",
    "burst_rate",
    "burst_start",
    "burst_duration",
    "tidal_amplitude",
    "tidal_period",
    "refill_distribution",
    "refill_median_seconds",
    "refill_sigma",
    "outage_start",
    "outage_duration",
    "outage_multiplier",
    "max_unavailable",
    "replenish_mode",
    "control_interval",
    "rate_window",
    "scale_up_factor",
    "scale_down_interval",
    "anomaly_multiple",
    "service_quantile",
    "lead_quantile",
    "min_guard",
    "production_scale_down_fraction",
    "production_stabilization",
    "capacity_utilization",
    "supply_timeout_multiplier",
]


@dataclass
class Session:
    simulator: WarmPoolSimulator
    running: bool = False


def _config(values: tuple[Any, ...]) -> SimulationConfig:
    return SimulationConfig(**dict(zip(CONFIG_FIELDS, values, strict=True))).normalized()


def _inventory_figure(simulator: WarmPoolSimulator) -> go.Figure:
    frame = pd.DataFrame(simulator.timeline)
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    for column, label in (
        ("available", "可立即领取"),
        ("creating", "正在补货"),
        ("waiting", "排队请求"),
        ("target", "实际目标池"),
        ("policy_intent", "算法建议池"),
    ):
        figure.add_trace(
            go.Scatter(x=frame["time"], y=frame[column], name=label, mode="lines"),
            secondary_y=False,
        )
    figure.add_trace(
        go.Scatter(
            x=frame["time"],
            y=frame["estimated_rate"],
            name="实际估计速率/s",
            mode="lines",
            line={"dash": "dot"},
        ),
        secondary_y=True,
    )
    figure.update_layout(
        title=f"{simulator.policy.name} · 已模拟 {simulator.now:.0f} 秒",
        margin={"l": 40, "r": 40, "t": 55, "b": 35},
        height=420,
        legend={"orientation": "h"},
    )
    figure.update_yaxes(title_text="沙盒数量 / 排队请求", secondary_y=False)
    figure.update_yaxes(title_text="请求/s", secondary_y=True)
    return figure


def _wait_figure(simulator: WarmPoolSimulator) -> go.Figure:
    pending = [max(0.0, simulator.now - value) for value in simulator.waiting]
    waits = simulator.latencies + pending
    figure = go.Figure()
    figure.add_trace(go.Histogram(x=waits, nbinsx=40, name="等待时延"))
    figure.add_vline(
        x=simulator.config.sla_seconds,
        line_dash="dash",
        annotation_text="SLA",
    )
    figure.update_layout(
        title="请求等待时延分布（含当前排队）",
        xaxis_title="秒",
        yaxis_title="请求数",
        margin={"l": 40, "r": 20, "t": 55, "b": 35},
        height=320,
    )
    return figure


def _summary_rows(simulator: WarmPoolSimulator) -> list[list[Any]]:
    summary = simulator.summary()
    return [
        ["请求总数", summary.requests],
        ["即时交付", summary.immediate],
        ["仍在排队", summary.pending],
        ["SLA 命中率", f"{summary.sla_rate:.2%}"],
        ["发生等待比例", f"{summary.cold_wait_rate:.2%}"],
        ["P50 / P95 / P99", f"{summary.p50_wait:.1f}s / {summary.p95_wait:.1f}s / {summary.p99_wait:.1f}s"],
        ["最大等待", f"{summary.max_wait:.1f}s"],
        ["当前 / 峰值目标池", f"{summary.final_target} / {summary.peak_target}"],
        ["预热资源成本", f"{summary.warm_sandbox_seconds:,.0f} sandbox·s"],
        ["扩缩容 / 熔断时间", f"{summary.scale_events} 次 / {summary.breaker_seconds:.0f}s"],
    ]


def _dashboard(session: Session) -> tuple[go.Figure, go.Figure, list[list[Any]], str, str]:
    simulator = session.simulator
    bounds = steady_state_bounds(
        simulator.config.arrival_rate,
        simulator.config.refill_median_seconds,
        simulator.config.sla_seconds,
    )
    policy_state = str(getattr(simulator.policy, "state", "NORMAL"))
    state_labels = {
        "NORMAL": "正常调节",
        "SURGE": "提前扩容",
        "DEGRADED": "供给能力不足",
        "RECOVERING": "供给恢复中",
    }
    status = (
        f"### {'正在自动运行' if session.running else '已暂停'}\n"
        f"已模拟 **{simulator.now:.0f}/{simulator.config.duration_seconds} 秒**；"
        f"控制器状态：**{state_labels.get(policy_state, policy_state)}**。\n\n"
        f"当前输入下，旧公式建议 **{bounds['original_formula']}** 个；"
        f"持续流量要尽量在 SLA 内交付，基础库存至少约 **{bounds['sla_wait']}** 个；"
        f"完全不等待约需 **{bounds['zero_wait']}** 个。"
    )
    events = "\n".join(f"- {line}" for line in list(simulator.event_log)[-12:])
    return (
        _inventory_figure(simulator),
        _wait_figure(simulator),
        _summary_rows(simulator),
        events or "- 暂无扩缩容或注入事件",
        status,
    )


def _new_session(
    policy_label: str,
    running: bool,
    *config_values: Any,
) -> tuple[Session, go.Figure, go.Figure, list[list[Any]], str, str]:
    config = _config(config_values)
    session = Session(create_simulator(config, POLICIES[policy_label]), running)
    return (session, *_dashboard(session))


def _start(
    session: Session | None,
    policy_label: str,
    *config_values: Any,
) -> tuple[Session, go.Figure, go.Figure, list[list[Any]], str, str]:
    if session is None or session.simulator.now >= session.simulator.config.duration_seconds:
        return _new_session(policy_label, True, *config_values)
    session.running = True
    return (session, *_dashboard(session))


def _pause(session: Session) -> tuple[Session, go.Figure, go.Figure, list[list[Any]], str, str]:
    session.running = False
    return (session, *_dashboard(session))


def _step(
    session: Session | None,
    policy_label: str,
    *config_values: Any,
) -> tuple[Session, go.Figure, go.Figure, list[list[Any]], str, str]:
    if session is None:
        session = _new_session(policy_label, False, *config_values)[0]
    session.running = False
    session.simulator.step(5.0)
    return (session, *_dashboard(session))


def _timer_tick(
    session: Session | None,
) -> tuple[Any, Any, Any, Any, Any, Any]:
    if session is None or not session.running:
        return session, gr.skip(), gr.skip(), gr.skip(), gr.skip(), gr.skip()
    session.simulator.step(5.0)
    if session.simulator.now >= session.simulator.config.duration_seconds:
        session.running = False
    return (session, *_dashboard(session))


def _inject_burst(
    session: Session, rate: float, duration: float
) -> tuple[Session, go.Figure, go.Figure, list[list[Any]], str, str]:
    session.simulator.inject_burst(rate, duration)
    return (session, *_dashboard(session))


def _inject_outage(
    session: Session, duration: float, multiplier: float
) -> tuple[Session, go.Figure, go.Figure, list[list[Any]], str, str]:
    session.simulator.inject_outage(duration, multiplier)
    return (session, *_dashboard(session))


def _recover(
    session: Session,
) -> tuple[Session, go.Figure, go.Figure, list[list[Any]], str, str]:
    session.simulator.recover_supply()
    return (session, *_dashboard(session))


def _comparison(*config_values: Any) -> tuple[list[list[Any]], go.Figure, str]:
    config = _config(config_values)
    simulations = run_comparison(config, ("adaptive", "production"))
    rows: list[list[Any]] = []
    figure = go.Figure()
    for simulator in simulations.values():
        summary = simulator.summary()
        rows.append(
            [
                summary.policy,
                summary.requests,
                f"{summary.sla_rate:.2%}",
                f"{summary.p95_wait:.1f}s",
                f"{summary.cold_wait_rate:.2%}",
                summary.peak_target,
                f"{summary.warm_sandbox_seconds:,.0f}",
                summary.pending,
            ]
        )
        frame = pd.DataFrame(simulator.timeline)
        figure.add_trace(
            go.Scatter(
                x=frame["time"],
                y=frame["waiting"],
                name=summary.policy,
                mode="lines",
            )
        )
    figure.update_layout(
        title="策略对比：排队请求数",
        xaxis_title="秒",
        yaxis_title="排队请求",
        height=420,
        legend={"orientation": "h"},
    )
    bounds = steady_state_bounds(
        config.arrival_rate,
        config.refill_median_seconds,
        config.sla_seconds,
    )
    old = simulations["adaptive"].summary()
    new = simulations["production"].summary()
    sla_delta = (new.sla_rate - old.sla_rate) * 100
    cost_delta = new.warm_sandbox_seconds - old.warm_sandbox_seconds
    conclusion = (
        "### 怎么看结果\n"
        f"- 新算法的 SLA 命中率比旧算法 **{sla_delta:+.2f} 个百分点**。\n"
        f"- 新算法预热资源变化 **{cost_delta:+,.0f} sandbox·s**；正数代表用更多资源换取更快交付。\n"
        f"- 当前输入下，旧公式只算出 **{bounds['original_formula']}** 个，"
        f"而持续流量的 SLA 基础库存约为 **{bounds['sla_wait']}** 个。\n\n"
        "重点同时观察 **SLA 命中率、P95 等待、最终排队、预热成本**，"
        "不能只看池大小。"
    )
    return rows, figure, conclusion


def build_app() -> gr.Blocks:
    defaults = SimulationConfig()
    with gr.Blocks(title="沙盒预热池：新旧算法实验室") as app:
        gr.Markdown(
            "# 沙盒预热池：新旧算法实验室\n"
            "用同一组流量和补货条件，对比你的**旧算法**与改进后的**生产级新算法**。\n\n"
            "**最简单的用法：** 选择“突发流量” → 点击底部“运行新旧算法对比” → "
            "比较 SLA 命中率和 P95 等待。这里仅做数学仿真，不会操作真实集群。"
        )
        session = gr.State(None)
        with gr.Row():
            with gr.Column(scale=2):
                policy = gr.Dropdown(
                    list(POLICIES),
                    value="新算法：生产级安全库存（推荐）",
                    label="单独观察哪个算法",
                )
                with gr.Accordion("第一步：设置业务流量", open=True):
                    duration = gr.Slider(
                        60,
                        3600,
                        defaults.duration_seconds,
                        step=30,
                        label="总共模拟多少秒",
                    )
                    seed = gr.Number(
                        defaults.seed,
                        precision=0,
                        label="随机种子（相同数字可复现同一次实验）",
                    )
                    initial = gr.Slider(
                        0,
                        500,
                        defaults.initial_replicas,
                        step=1,
                        label="开始时已经准备好的沙盒数",
                    )
                    maximum = gr.Slider(
                        10,
                        2000,
                        defaults.max_replicas,
                        step=10,
                        label="最多允许准备多少个沙盒",
                    )
                    sla = gr.Slider(
                        1,
                        120,
                        defaults.sla_seconds,
                        step=1,
                        label="用户最多愿意等待几秒（SLA）",
                    )
                    arrival_pattern = gr.Dropdown(
                        [
                            ("平稳流量", "constant"),
                            ("随机波动流量", "poisson"),
                            ("突然暴增后恢复", "burst"),
                            ("周期性潮汐流量", "tidal"),
                        ],
                        value=defaults.arrival_pattern,
                        label="流量怎么变化",
                    )
                    arrival_rate = gr.Slider(
                        0,
                        50,
                        defaults.arrival_rate,
                        step=0.1,
                        label="平时每秒来多少个请求",
                    )
                    burst_rate = gr.Slider(
                        0,
                        100,
                        defaults.burst_rate,
                        step=0.5,
                        label="暴增时每秒来多少个请求",
                    )
                    burst_start = gr.Slider(
                        0,
                        1800,
                        defaults.burst_start,
                        step=5,
                        label="第几秒开始暴增",
                    )
                    burst_duration = gr.Slider(
                        1,
                        600,
                        defaults.burst_duration,
                        step=5,
                        label="暴增持续多少秒",
                    )
                    tidal_amplitude = gr.Slider(
                        0,
                        2,
                        defaults.tidal_amplitude,
                        step=0.05,
                        label="潮汐起伏幅度",
                    )
                    tidal_period = gr.Slider(
                        30,
                        1800,
                        defaults.tidal_period,
                        step=10,
                        label="一个潮汐周期多少秒",
                    )
                with gr.Accordion("第二步：设置补货能力", open=True):
                    refill_distribution = gr.Dropdown(
                        [
                            ("每次补货时间基本一样", "fixed"),
                            ("补货偶尔会明显变慢", "lognormal"),
                        ],
                        value=defaults.refill_distribution,
                        label="补货时间是否稳定",
                    )
                    refill_median = gr.Slider(
                        1,
                        300,
                        defaults.refill_median_seconds,
                        step=1,
                        label="正常补出一个沙盒需要几秒",
                    )
                    refill_sigma = gr.Slider(
                        0.05,
                        1.5,
                        defaults.refill_sigma,
                        step=0.05,
                        label="补货时间波动程度（越大越不稳定）",
                    )
                    outage_start = gr.Number(
                        defaults.outage_start,
                        label="第几秒开始模拟供给故障（-1 表示不故障）",
                    )
                    outage_duration = gr.Slider(
                        0,
                        1200,
                        defaults.outage_duration,
                        step=10,
                        label="供给故障持续多少秒",
                    )
                    outage_multiplier = gr.Slider(
                        1,
                        20,
                        defaults.outage_multiplier,
                        step=0.5,
                        label="故障期间补货慢多少倍",
                    )
                    max_unavailable = gr.Slider(
                        0,
                        500,
                        defaults.max_unavailable,
                        step=1,
                        label="最多同时补多少个（0 表示不限制）",
                    )
                    replenish_mode = gr.Dropdown(
                        [
                            ("OpenKruise：领取一个就补一个", "openkruise"),
                            ("整批同时完成", "batch"),
                        ],
                        value=defaults.replenish_mode,
                        label="补货方式",
                    )
                with gr.Accordion("高级参数：算法专家使用", open=False):
                    control_interval = gr.Slider(
                        1, 60, defaults.control_interval, step=1, label="多久重新计算一次"
                    )
                    rate_window = gr.Slider(
                        5, 600, defaults.rate_window, step=5, label="旧算法观察多久的流量"
                    )
                    scale_up_factor = gr.Slider(
                        1.1, 4, defaults.scale_up_factor, step=0.1, label="旧算法每次扩容倍数"
                    )
                    scale_down_interval = gr.Slider(
                        5, 600, defaults.scale_down_interval, step=5, label="旧算法每减 1 个的间隔"
                    )
                    anomaly_multiple = gr.Slider(
                        1.5, 10, defaults.anomaly_multiple, step=0.5, label="旧算法供给异常倍数"
                    )
                    service_quantile = gr.Slider(
                        0.8,
                        0.999,
                        defaults.service_quantile,
                        step=0.001,
                        label="新算法服务把握度（0.99 表示 99%）",
                    )
                    lead_quantile = gr.Slider(
                        0.5,
                        0.999,
                        defaults.lead_quantile,
                        step=0.001,
                        label="新算法采用多保守的补货时间",
                    )
                    min_guard = gr.Slider(
                        0,
                        100,
                        defaults.min_guard,
                        step=1,
                        label="新算法额外保底沙盒数",
                    )
                    production_scale_down_fraction = gr.Slider(
                        0.01,
                        0.5,
                        defaults.production_scale_down_fraction,
                        step=0.01,
                        label="新算法每次最多缩减比例",
                    )
                    production_stabilization = gr.Slider(
                        5,
                        1200,
                        defaults.production_stabilization,
                        step=5,
                        label="新算法缩容前稳定观察时间",
                    )
                    capacity_utilization = gr.Slider(
                        0.1,
                        0.99,
                        defaults.capacity_utilization,
                        step=0.01,
                        label="补货能力最多使用比例",
                    )
                    supply_timeout_multiplier = gr.Slider(
                        1,
                        10,
                        defaults.supply_timeout_multiplier,
                        step=0.5,
                        label="超过正常补货时间多少倍算异常",
                    )
            with gr.Column(scale=5):
                status = gr.Markdown("调整左侧参数后，可以单独运行一个算法，也可以在页面底部直接比较新旧算法。")
                with gr.Row():
                    reset = gr.Button("应用左侧参数并重置", variant="secondary")
                    start = gr.Button("开始自动模拟", variant="primary")
                    pause = gr.Button("暂停观察")
                    step = gr.Button("向前模拟 5 秒")
                inventory_plot = gr.Plot()
                wait_plot = gr.Plot()
                metrics = gr.Dataframe(
                    headers=["指标", "值"],
                    datatype=["str", "str"],
                    interactive=False,
                    label="本次实验结果",
                )
                events = gr.Markdown("- 暂无事件")
                with gr.Row():
                    manual_rate = gr.Slider(0, 100, 20, step=1, label="临时改成每秒多少请求")
                    manual_duration = gr.Slider(5, 300, 30, step=5, label="临时流量持续多久")
                    inject_burst = gr.Button("现在注入一波流量")
                with gr.Row():
                    manual_outage_duration = gr.Slider(5, 600, 120, step=5, label="供给故障持续多久")
                    manual_outage_multiplier = gr.Slider(1, 30, 10, step=1, label="补货临时变慢多少倍")
                    inject_outage = gr.Button("现在模拟供给故障", variant="stop")
                    recover = gr.Button("立即恢复供给")

        config_inputs = [
            duration,
            seed,
            initial,
            maximum,
            sla,
            arrival_pattern,
            arrival_rate,
            burst_rate,
            burst_start,
            burst_duration,
            tidal_amplitude,
            tidal_period,
            refill_distribution,
            refill_median,
            refill_sigma,
            outage_start,
            outage_duration,
            outage_multiplier,
            max_unavailable,
            replenish_mode,
            control_interval,
            rate_window,
            scale_up_factor,
            scale_down_interval,
            anomaly_multiple,
            service_quantile,
            lead_quantile,
            min_guard,
            production_scale_down_fraction,
            production_stabilization,
            capacity_utilization,
            supply_timeout_multiplier,
        ]
        dashboard_outputs = [session, inventory_plot, wait_plot, metrics, events, status]
        reset.click(
            lambda label, *values: _new_session(label, False, *values),
            [policy, *config_inputs],
            dashboard_outputs,
        )
        start.click(_start, [session, policy, *config_inputs], dashboard_outputs)
        pause.click(_pause, [session], dashboard_outputs)
        step.click(_step, [session, policy, *config_inputs], dashboard_outputs)
        inject_burst.click(
            _inject_burst,
            [session, manual_rate, manual_duration],
            dashboard_outputs,
        )
        inject_outage.click(
            _inject_outage,
            [session, manual_outage_duration, manual_outage_multiplier],
            dashboard_outputs,
        )
        recover.click(_recover, [session], dashboard_outputs)
        timer = gr.Timer(1.0)
        timer.tick(_timer_tick, [session], dashboard_outputs)

        gr.Markdown(
            "## 第三步：一键比较新旧算法\n"
            "两个算法会使用完全相同的随机流量和补货过程，结果可以直接比较。"
        )
        compare_button = gr.Button("运行新旧算法对比", variant="primary")
        compare_table = gr.Dataframe(
            headers=[
                "策略",
                "总请求数",
                "按时交付比例",
                "95%请求等待不超过",
                "需要等待的比例",
                "准备过的最大池",
                "预热资源总成本",
                "结束时仍在排队",
            ],
            interactive=False,
        )
        compare_plot = gr.Plot()
        conclusion = gr.Markdown()
        compare_button.click(
            _comparison,
            config_inputs,
            [compare_table, compare_plot, conclusion],
        )
    return app


if __name__ == "__main__":
    build_app().queue(default_concurrency_limit=8, max_size=64).launch(
        server_name="0.0.0.0",
        server_port=int(os.environ.get("PORT", "7860")),
        show_error=True,
    )
