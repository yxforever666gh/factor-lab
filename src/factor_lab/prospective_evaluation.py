"""Pure, preregistered evaluation of prospective schema-2 outcomes.

The evaluator accepts only the rich ledger schema-2 envelope containing an
embedded, self-hashed :class:`~factor_lab.prospective_execution.CycleOutcome`.
It performs no filesystem, clock, ledger, or network access.  All evidence is
validated and canonically reordered before metrics are calculated.

The evidence schedule is deliberately resistant to optional stopping:

* the first outcome for every offset proves engineering closure only;
* the first six outcomes for every offset may reject, but can never promote;
* the first twenty-five outcomes for every offset form a one-year directional
  gate, never a stable-profit or performance-promotion claim.

Additional outcomes are validated and bound into the evaluation input digest,
but never rewrite an already-defined 60- or 250-outcome cohort.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math
import re
import statistics
from typing import Any
import unicodedata

from .prospective_execution import CycleOutcome, SleeveAccountState
from .prospective_targets import OFFSET_COUNT, SLEEVE_CAPITAL_FEN


EVALUATION_SCHEMA_VERSION = 1
EVALUATOR_ID = "factor-lab/prospective-evaluation/5.2"
OUTCOME_SCHEMA_VERSION = 2
RETURN_SCALE_PPB = 1_000_000_000
METRIC_SCALE_PPM = 1_000_000
PERIODS_PER_YEAR_HEX = float(25.2).hex()
DAILY_PERIODS_PER_YEAR_HEX = float(252.0).hex()

ENGINEERING_TOTAL_OUTCOMES = 10
ENGINEERING_MIN_PER_OFFSET = 1
EARLY_STOP_TOTAL_OUTCOMES = 60
EARLY_STOP_MIN_PER_OFFSET = 6
FORMAL_GATE_TOTAL_OUTCOMES = 250
FORMAL_GATE_MIN_PER_OFFSET = 25

EARLY_REJECT_MAX_POSITIVE_NET_OFFSETS = 2
EARLY_REJECT_MAX_POSITIVE_ACTIVE_OFFSETS = 2
FORMAL_MIN_POSITIVE_NET_OFFSETS = 8
FORMAL_MIN_POSITIVE_ACTIVE_OFFSETS = 7
FORMAL_WORST_MAX_DRAWDOWN_MIN_PPM = -250_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OUTCOME_KEYS = frozenset(
    {
        "schema_version",
        "decision_record_sha256",
        "attestation_receipt_record_sha256",
        "execution_snapshot_sha256",
        "cycle_outcome_sha256",
        "cycle_outcome",
    }
)


class ProspectiveEvaluationError(ValueError):
    """Raised when evaluation evidence or its accounting chain is invalid."""


def _canonical_json_bytes(value: Any) -> bytes:
    """Encode canonical JSON while forbidding JSON floating-point numbers."""

    def normalize(item: Any, path: str) -> Any:
        if item is None or isinstance(item, bool) or type(item) is int:
            return item
        if isinstance(item, str):
            result = unicodedata.normalize("NFC", item)
            if any(0xD800 <= ord(character) <= 0xDFFF for character in result):
                raise ProspectiveEvaluationError(
                    f"Unicode surrogate is forbidden at {path}"
                )
            return result
        if isinstance(item, Mapping):
            output: dict[str, Any] = {}
            for raw_key, raw_value in item.items():
                if not isinstance(raw_key, str):
                    raise ProspectiveEvaluationError(f"non-string JSON key at {path}")
                key = unicodedata.normalize("NFC", raw_key)
                if key in output:
                    raise ProspectiveEvaluationError(
                        f"duplicate key after Unicode normalization at {path}"
                    )
                output[key] = normalize(raw_value, f"{path}.{key}")
            return output
        if isinstance(item, (list, tuple)):
            return [
                normalize(child, f"{path}[{index}]") for index, child in enumerate(item)
            ]
        if isinstance(item, float):
            raise ProspectiveEvaluationError(
                f"JSON floating-point value is forbidden at {path}"
            )
        raise ProspectiveEvaluationError(
            f"unsupported JSON value {type(item).__name__} at {path}"
        )

    return json.dumps(
        normalize(value, "$"),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_payload(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _contract_payload() -> dict[str, Any]:
    return {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "evaluator_id": EVALUATOR_ID,
        "input_contract": {
            "outer_outcome_schema_version": OUTCOME_SCHEMA_VERSION,
            "embedded_type": "factor_lab.prospective_execution.CycleOutcome",
            "exact_outer_keys": sorted(_OUTCOME_KEYS),
            "accepted_execution_statuses": ["complete"],
            "upstream_trust_boundary": (
                "caller must supply only rich schema-2 outcomes recovered by strict "
                "prospective ledger replay; this pure evaluator does not establish "
                "decision receipt or transparency-log provenance"
            ),
            "not_executed_semantics": "forbidden_in_5.2_production_outcomes",
            "ordering": ["offset", "signal_date", "holding_start_date"],
            "continuity": [
                "first previous account state equals deterministic offset genesis",
                "same-offset previous state hash equals prior next state hash",
                "same-offset opening NAV equals prior ending NAV",
                "same-offset prior holding end equals next holding start",
                "cycle_count starts at one and is contiguous",
            ],
        },
        "evidence_schedule": {
            "terminal_insolvency": {
                "trigger": "any_verified_cycle_ending_nav_fen_equals_zero",
                "minimum_outcomes": 1,
                "terminal_rejection": True,
                "performance_claim_allowed": False,
            },
            "engineering_closure": {
                "total_outcomes": ENGINEERING_TOTAL_OUTCOMES,
                "minimum_per_offset": ENGINEERING_MIN_PER_OFFSET,
                "performance_claim_allowed": False,
            },
            "early_stop": {
                "total_outcomes": EARLY_STOP_TOTAL_OUTCOMES,
                "minimum_per_offset": EARLY_STOP_MIN_PER_OFFSET,
                "cohort": "first_6_per_offset",
                "promotion_allowed": False,
                "terminal_rejection_precedes_formal_gate": True,
                "reject_if_both": {
                    "positive_net_compound_offset_count_lte": (
                        EARLY_REJECT_MAX_POSITIVE_NET_OFFSETS
                    ),
                    "positive_active_compound_offset_count_lte": (
                        EARLY_REJECT_MAX_POSITIVE_ACTIVE_OFFSETS
                    ),
                },
            },
            "one_year_directional_gate": {
                "total_outcomes": FORMAL_GATE_TOTAL_OUTCOMES,
                "minimum_per_offset": FORMAL_GATE_MIN_PER_OFFSET,
                "cohort": "first_25_per_offset",
                "stable_profit_claim_allowed": False,
            },
        },
        "metric_contract": {
            "periods_per_year_hex": PERIODS_PER_YEAR_HEX,
            "return_input_scale_ppb": RETURN_SCALE_PPB,
            "annualization": "geometric_compound_then_power_25.2_over_cycle_count",
            "active_cagr": "net_cagr_minus_benchmark_cagr",
            "net_sharpe": "sample_mean_over_sample_std_times_sqrt_25.2",
            "zero_or_tiny_std_sharpe": "zero_when_sample_std_lte_1e-12",
            "offset_max_drawdown": "all_holding_session_NAV_with_new_cycle_start_overwriting_shared_boundary",
            "master_portfolio": (
                "sum_ten_real_sleeve_daily_NAV_from_earliest_first_start_through_"
                "earliest_25th_end_unseeded_sleeves_are_CNY_5m_cash"
            ),
            "master_benchmark": (
                "sum_ten_compounded_stateful_cycle_benchmarks_using_frozen_"
                "decision_rosters_without_endpoint_reweighting"
            ),
            "master_daily_periods_per_year_hex": DAILY_PERIODS_PER_YEAR_HEX,
            "master_daily_sharpe": "sample_mean_over_sample_std_times_sqrt_252",
            "master_daily_max_drawdown": "full_common_daily_master_NAV_including_initial_total_cash",
            "cross_offset_quantile": "type_7_linear",
            "calculated_metric_encoding": "binary64_hex_string",
            "json_float_allowed": False,
        },
        "one_year_directional_gate": {
            "net_cagr_q20_gt_ppb": 0,
            "net_sharpe_q20_gt_ppb": 0,
            "active_cagr_q20_gt_ppb": 0,
            "positive_net_offset_count_gte": FORMAL_MIN_POSITIVE_NET_OFFSETS,
            "positive_active_offset_count_gte": FORMAL_MIN_POSITIVE_ACTIVE_OFFSETS,
            "worst_max_drawdown_gte_ppm": FORMAL_WORST_MAX_DRAWDOWN_MIN_PPM,
            "master_terminal_wealth_gt_initial": True,
            "master_net_cagr_gt_ppb": 0,
            "master_active_cagr_gt_ppb": 0,
            "master_daily_sharpe_gt_ppb": 0,
            "master_daily_max_drawdown_gte_ppm": FORMAL_WORST_MAX_DRAWDOWN_MIN_PPM,
            "all_checks_required": True,
        },
    }


EVALUATION_CONTRACT = _contract_payload()
EVALUATION_CONTRACT_CANONICAL_JSON = _canonical_json_bytes(EVALUATION_CONTRACT)
EVALUATION_CONTRACT_SHA256 = hashlib.sha256(
    EVALUATION_CONTRACT_CANONICAL_JSON
).hexdigest()


def canonical_evaluation_json_bytes(
    evaluation: Mapping[str, Any], *, verify_hash: bool = True
) -> bytes:
    """Return canonical bytes for an evaluation, optionally checking self-hash."""

    if not isinstance(evaluation, Mapping):
        raise ProspectiveEvaluationError("evaluation must be a mapping")
    value = dict(evaluation)
    if verify_hash:
        actual = value.pop("evaluation_sha256", None)
        if not isinstance(actual, str) or not _SHA256_RE.fullmatch(actual):
            raise ProspectiveEvaluationError("evaluation_sha256 is missing or invalid")
        expected = _sha256_payload(value)
        if actual != expected:
            raise ProspectiveEvaluationError("evaluation_sha256 does not match payload")
    return _canonical_json_bytes(evaluation)


def _require_sha(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ProspectiveEvaluationError(f"{label} must be a lowercase SHA-256")
    return value


def _date(value: Any, *, label: str) -> date:
    if not isinstance(value, str):
        raise ProspectiveEvaluationError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ProspectiveEvaluationError(
            f"{label} must be a canonical ISO date"
        ) from exc
    if parsed.isoformat() != value:
        raise ProspectiveEvaluationError(f"{label} must be a canonical ISO date")
    return parsed


def _hex(value: float) -> str:
    if not math.isfinite(value):
        raise ProspectiveEvaluationError("calculated metric is not finite")
    return (0.0 if value == 0.0 else value).hex()


@dataclass(frozen=True)
class _VerifiedOutcome:
    outer: Mapping[str, Any]
    outer_sha256: str
    cycle: CycleOutcome


def _verify_outcome(value: Mapping[str, Any]) -> _VerifiedOutcome:
    if not isinstance(value, Mapping) or set(value) != set(_OUTCOME_KEYS):
        raise ProspectiveEvaluationError(
            "outcome must be an exact rich schema-2 mapping"
        )
    if value.get("schema_version") != OUTCOME_SCHEMA_VERSION:
        raise ProspectiveEvaluationError("outcome schema_version must be 2")
    for name in (
        "decision_record_sha256",
        "attestation_receipt_record_sha256",
        "execution_snapshot_sha256",
        "cycle_outcome_sha256",
    ):
        _require_sha(value.get(name), label=name)
    nested = value.get("cycle_outcome")
    if not isinstance(nested, Mapping):
        raise ProspectiveEvaluationError("cycle_outcome must be a mapping")
    if type(nested.get("schema_version")) is not int:
        raise ProspectiveEvaluationError(
            "embedded CycleOutcome schema_version must be an integer"
        )
    next_state = nested.get("next_account_state")
    if (
        not isinstance(next_state, Mapping)
        or type(next_state.get("schema_version")) is not int
    ):
        raise ProspectiveEvaluationError(
            "embedded accounting-state schema_version must be an integer"
        )
    try:
        cycle = CycleOutcome.from_mapping(nested)
    except Exception as exc:
        raise ProspectiveEvaluationError("embedded CycleOutcome is invalid") from exc
    if _canonical_json_bytes(nested) != _canonical_json_bytes(cycle.to_dict()):
        raise ProspectiveEvaluationError(
            "embedded CycleOutcome is not its exact canonical self-hashed mapping"
        )
    if value["cycle_outcome_sha256"] != cycle.outcome_sha256:
        raise ProspectiveEvaluationError("outer and embedded outcome hashes differ")
    if value["execution_snapshot_sha256"] != cycle.execution_snapshot_sha256:
        raise ProspectiveEvaluationError("outer and embedded execution hashes differ")
    if cycle.execution_status != "complete":
        raise ProspectiveEvaluationError("5.2 evaluation accepts only complete outcomes")
    observed = datetime.fromisoformat(
        cycle.observation_available_at_utc[:-1] + "+00:00"
    ).astimezone(timezone.utc)
    if observed.date() < _date(cycle.holding_end_date, label="holding_end_date"):
        raise ProspectiveEvaluationError("outcome was observed before its holding end")
    return _VerifiedOutcome(
        outer=dict(value),
        outer_sha256=_sha256_payload(value),
        cycle=cycle,
    )


def _validate_uniqueness(rows: Sequence[_VerifiedOutcome]) -> None:
    fields: tuple[tuple[str, list[Any]], ...] = (
        ("outer outcome", [row.outer_sha256 for row in rows]),
        ("decision", [row.outer["decision_record_sha256"] for row in rows]),
        (
            "attestation receipt",
            [row.outer["attestation_receipt_record_sha256"] for row in rows],
        ),
        ("cycle outcome", [row.cycle.outcome_sha256 for row in rows]),
        ("generation result", [row.cycle.generation_result_sha256 for row in rows]),
        ("execution snapshot", [row.cycle.execution_snapshot_sha256 for row in rows]),
        ("signal date", [row.cycle.signal_date for row in rows]),
        (
            "offset/signal",
            [(row.cycle.offset, row.cycle.signal_date) for row in rows],
        ),
    )
    for label, values in fields:
        if len(set(values)) != len(values):
            raise ProspectiveEvaluationError(f"duplicate {label} evidence")


def _return_reconciles(cycle: CycleOutcome) -> bool:
    if cycle.opening_nav_fen <= 0:
        return False
    implied = int(
        round((cycle.ending_nav_fen / cycle.opening_nav_fen - 1.0) * RETURN_SCALE_PPB)
    )
    tolerance = 2 + math.ceil(RETURN_SCALE_PPB / cycle.opening_nav_fen)
    return abs(implied - cycle.net_return_ppb) <= tolerance


def _validate_continuity(rows: Sequence[_VerifiedOutcome]) -> None:
    if not rows:
        return
    deployments = {row.cycle.deployment_sha256 for row in rows}
    contracts = {row.cycle.contract_sha256 for row in rows}
    if len(deployments) != 1 or len(contracts) != 1:
        raise ProspectiveEvaluationError(
            "one evaluation cannot mix deployments or execution contracts"
        )
    by_offset: dict[int, list[CycleOutcome]] = {
        offset: [] for offset in range(OFFSET_COUNT)
    }
    for row in rows:
        by_offset[row.cycle.offset].append(row.cycle)
        if not _return_reconciles(row.cycle):
            raise ProspectiveEvaluationError(
                "cycle net return does not reconcile to opening/ending NAV"
            )
    for offset, cycles in by_offset.items():
        cycles.sort(
            key=lambda cycle: (
                cycle.signal_date,
                cycle.holding_start_date,
                cycle.holding_end_date,
            )
        )
        if not cycles:
            continue
        genesis = SleeveAccountState.genesis(
            deployment_sha256=cycles[0].deployment_sha256,
            offset=offset,
        )
        if (
            cycles[0].previous_account_state_sha256 != genesis.state_sha256
            or cycles[0].opening_nav_fen != genesis.nav_fen
            or cycles[0].next_account_state.cycle_count != 1
        ):
            raise ProspectiveEvaluationError(
                f"offset {offset} does not begin at deterministic genesis"
            )
        for index, cycle in enumerate(cycles):
            if cycle.next_account_state.cycle_count != index + 1:
                raise ProspectiveEvaluationError(
                    f"offset {offset} cycle_count is not contiguous"
                )
            if index == 0:
                continue
            previous = cycles[index - 1]
            if cycle.signal_date <= previous.signal_date:
                raise ProspectiveEvaluationError(
                    f"offset {offset} signal dates are not increasing"
                )
            if previous.holding_end_date != cycle.holding_start_date:
                raise ProspectiveEvaluationError(
                    f"offset {offset} holding windows are not continuous"
                )
            if (
                cycle.previous_account_state_sha256
                != previous.next_account_state.state_sha256
            ):
                raise ProspectiveEvaluationError(
                    f"offset {offset} accounting state hash is not continuous"
                )
            if cycle.opening_nav_fen != previous.ending_nav_fen:
                raise ProspectiveEvaluationError(
                    f"offset {offset} NAV is not continuous"
                )


def _as_return(value: int) -> float:
    try:
        result = value / RETURN_SCALE_PPB
    except OverflowError as exc:
        raise ProspectiveEvaluationError(
            "return integer cannot be represented"
        ) from exc
    if not math.isfinite(result) or result < -1.0:
        raise ProspectiveEvaluationError(
            "cycle return is outside the compoundable domain"
        )
    return result


def _compound_and_annualize(values: Sequence[float]) -> tuple[float, float]:
    if not values:
        raise ProspectiveEvaluationError("cannot annualize an empty return series")
    wealth = 1.0
    for value in values:
        wealth *= 1.0 + value
        if not math.isfinite(wealth):
            raise ProspectiveEvaluationError("compounded wealth is not finite")
    if wealth <= 0.0:
        return -1.0, -1.0
    total = wealth - 1.0
    try:
        annualized = wealth ** (float.fromhex(PERIODS_PER_YEAR_HEX) / len(values)) - 1.0
    except OverflowError as exc:
        raise ProspectiveEvaluationError("annualized return overflowed") from exc
    if not math.isfinite(total) or not math.isfinite(annualized):
        raise ProspectiveEvaluationError("annualized return is not finite")
    return total, annualized


def _sharpe(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    deviation = math.sqrt(max(0.0, variance))
    if deviation <= 1e-12:
        return 0.0
    result = mean / deviation * math.sqrt(float.fromhex(PERIODS_PER_YEAR_HEX))
    if not math.isfinite(result):
        raise ProspectiveEvaluationError("Sharpe ratio is not finite")
    return result


def _offset_daily_nav(cycles: Sequence[CycleOutcome]) -> dict[str, int]:
    daily: dict[str, int] = {}
    for cycle in cycles:
        for point in cycle.daily_path:
            # Same-offset cycles touch at one opening boundary.  The new
            # cycle's post-trade NAV is authoritative because it includes the
            # newly incurred execution cost.
            daily[point.date] = point.account_nav_fen
    return daily


def _max_drawdown(cycles: Sequence[CycleOutcome]) -> float:
    navs = [cycles[0].opening_nav_fen, *_offset_daily_nav(cycles).values()]
    peak = navs[0]
    worst = 0.0
    for nav in navs:
        peak = max(peak, nav)
        drawdown = nav / peak - 1.0 if peak > 0 else -1.0
        worst = min(worst, drawdown)
    return worst


@dataclass(frozen=True)
class _OffsetMetrics:
    offset: int
    cycle_count: int
    net_total: float
    benchmark_total: float
    net_cagr: float
    benchmark_cagr: float
    active_cagr: float
    net_sharpe: float
    max_drawdown: float
    complete_count: int
    not_executed_count: int
    turnover_ppm_sum: int
    fees_fen_sum: int
    first_signal_date: str
    last_holding_end_date: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            "offset": self.offset,
            "cycle_count": self.cycle_count,
            "first_signal_date": self.first_signal_date,
            "last_holding_end_date": self.last_holding_end_date,
            "complete_count": self.complete_count,
            "not_executed_count": self.not_executed_count,
            "turnover_ppm_sum": self.turnover_ppm_sum,
            "fees_fen_sum": self.fees_fen_sum,
            "net_compound_return_hex": _hex(self.net_total),
            "benchmark_compound_return_hex": _hex(self.benchmark_total),
            "net_cagr_hex": _hex(self.net_cagr),
            "benchmark_cagr_hex": _hex(self.benchmark_cagr),
            "active_cagr_hex": _hex(self.active_cagr),
            "net_sharpe_hex": _hex(self.net_sharpe),
            "holding_session_max_drawdown_hex": _hex(self.max_drawdown),
        }


def _offset_metrics(offset: int, cycles: Sequence[CycleOutcome]) -> _OffsetMetrics:
    net = [_as_return(cycle.net_return_ppb) for cycle in cycles]
    benchmark = [_as_return(cycle.benchmark_return_ppb) for cycle in cycles]
    net_total, net_cagr = _compound_and_annualize(net)
    benchmark_total, benchmark_cagr = _compound_and_annualize(benchmark)
    return _OffsetMetrics(
        offset=offset,
        cycle_count=len(cycles),
        net_total=net_total,
        benchmark_total=benchmark_total,
        net_cagr=net_cagr,
        benchmark_cagr=benchmark_cagr,
        active_cagr=net_cagr - benchmark_cagr,
        net_sharpe=_sharpe(net),
        max_drawdown=_max_drawdown(cycles),
        complete_count=sum(cycle.execution_status == "complete" for cycle in cycles),
        not_executed_count=0,
        turnover_ppm_sum=sum(cycle.turnover_ppm for cycle in cycles),
        fees_fen_sum=sum(cycle.fees_fen for cycle in cycles),
        first_signal_date=cycles[0].signal_date,
        last_holding_end_date=cycles[-1].holding_end_date,
    )


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ProspectiveEvaluationError("cross-offset metric is empty")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _summary(values: Sequence[float]) -> dict[str, str]:
    return {
        "q20_hex": _hex(_quantile(values, 0.2)),
        "median_hex": _hex(_quantile(values, 0.5)),
        "worst_hex": _hex(min(values)),
    }


def _performance_mapping(metrics: Sequence[_OffsetMetrics]) -> dict[str, Any]:
    positive_net = sum(metric.net_total > 0.0 for metric in metrics)
    positive_active = sum(metric.active_cagr > 0.0 for metric in metrics)
    return {
        "cohort_cycle_count_per_offset": metrics[0].cycle_count,
        "cohort_outcome_count": sum(metric.cycle_count for metric in metrics),
        "offsets": [metric.to_mapping() for metric in metrics],
        "cross_offset": {
            "net_cagr": _summary([metric.net_cagr for metric in metrics]),
            "benchmark_cagr": _summary([metric.benchmark_cagr for metric in metrics]),
            "active_cagr": _summary([metric.active_cagr for metric in metrics]),
            "net_sharpe": _summary([metric.net_sharpe for metric in metrics]),
            "holding_session_max_drawdown": _summary(
                [metric.max_drawdown for metric in metrics]
            ),
        },
        "positive_net_compound_offset_count": positive_net,
        "positive_active_compound_offset_count": positive_active,
    }


def _decode_summary(
    performance: Mapping[str, Any], metric: str, statistic: str
) -> float:
    return float.fromhex(str(performance["cross_offset"][metric][statistic]))


def _nav_path_metrics(navs: Sequence[int], *, initial_nav: int) -> dict[str, Any]:
    if not navs or initial_nav <= 0:
        raise ProspectiveEvaluationError("master NAV path is empty or has invalid genesis")
    returns: list[float] = []
    previous = initial_nav
    for nav in navs:
        if nav < 0:
            raise ProspectiveEvaluationError("master NAV cannot be negative")
        returns.append(nav / previous - 1.0 if previous > 0 else -1.0)
        previous = nav
    total = navs[-1] / initial_nav - 1.0
    if navs[-1] <= 0:
        cagr = -1.0
    else:
        cagr = (navs[-1] / initial_nav) ** (
            float.fromhex(DAILY_PERIODS_PER_YEAR_HEX) / len(returns)
        ) - 1.0
    peak = initial_nav
    max_drawdown = 0.0
    for nav in navs:
        peak = max(peak, nav)
        max_drawdown = min(max_drawdown, nav / peak - 1.0 if peak > 0 else -1.0)
    mean = statistics.fmean(returns)
    deviation = (
        math.sqrt(
            sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
        )
        if len(returns) >= 2
        else 0.0
    )
    daily_sharpe = (
        mean / deviation * math.sqrt(float.fromhex(DAILY_PERIODS_PER_YEAR_HEX))
        if deviation > 1e-12
        else 0.0
    )
    if not all(math.isfinite(value) for value in (total, cagr, daily_sharpe, max_drawdown)):
        raise ProspectiveEvaluationError("master performance metric is not finite")
    return {
        "terminal_nav_fen": navs[-1],
        "compound_return": total,
        "cagr": cagr,
        "daily_sharpe": daily_sharpe,
        "max_drawdown": max_drawdown,
    }


def _master_performance(
    by_offset: Mapping[int, Sequence[CycleOutcome]], *, cohort_size: int
) -> dict[str, Any]:
    cohorts = {
        offset: list(by_offset[offset][:cohort_size]) for offset in range(OFFSET_COUNT)
    }
    common_start = min(cycles[0].holding_start_date for cycles in cohorts.values())
    common_end = min(cycles[-1].holding_end_date for cycles in cohorts.values())

    account_paths: dict[int, dict[str, int]] = {}
    benchmark_paths: dict[int, dict[str, int]] = {}
    all_dates: set[str] = set()
    for offset, cycles in cohorts.items():
        account_path = _offset_daily_nav(cycles)
        benchmark_path: dict[str, int] = {}
        opening_benchmark_nav = SLEEVE_CAPITAL_FEN
        for cycle in cycles:
            for point in cycle.daily_path:
                benchmark_path[point.date] = int(
                    round(
                        opening_benchmark_nav
                        * point.benchmark_index_ppb
                        / RETURN_SCALE_PPB
                    )
                )
            opening_benchmark_nav = benchmark_path[cycle.holding_end_date]
        account_paths[offset] = account_path
        benchmark_paths[offset] = benchmark_path
        all_dates.update(
            value for value in account_path if common_start <= value <= common_end
        )

    dates = sorted(all_dates)
    if not dates or dates[0] != common_start or dates[-1] != common_end:
        raise ProspectiveEvaluationError("master cohort does not cover its common interval")
    master_navs: list[int] = []
    master_benchmark_navs: list[int] = []
    for current_date in dates:
        account_total = 0
        benchmark_total = 0
        for offset in range(OFFSET_COUNT):
            first_start = cohorts[offset][0].holding_start_date
            if current_date < first_start:
                account_total += SLEEVE_CAPITAL_FEN
                benchmark_total += SLEEVE_CAPITAL_FEN
                continue
            try:
                account_total += account_paths[offset][current_date]
                benchmark_total += benchmark_paths[offset][current_date]
            except KeyError as exc:
                raise ProspectiveEvaluationError(
                    f"offset {offset} omits a master-portfolio session"
                ) from exc
        master_navs.append(account_total)
        master_benchmark_navs.append(benchmark_total)

    initial_total = SLEEVE_CAPITAL_FEN * OFFSET_COUNT
    strategy = _nav_path_metrics(master_navs, initial_nav=initial_total)
    benchmark = _nav_path_metrics(master_benchmark_navs, initial_nav=initial_total)
    return {
        "common_start_date": common_start,
        "common_end_date": common_end,
        "session_count": len(dates),
        "initial_nav_fen": initial_total,
        "terminal_nav_fen": strategy["terminal_nav_fen"],
        "benchmark_terminal_nav_fen": benchmark["terminal_nav_fen"],
        "net_compound_return_hex": _hex(strategy["compound_return"]),
        "benchmark_compound_return_hex": _hex(benchmark["compound_return"]),
        "net_cagr_hex": _hex(strategy["cagr"]),
        "benchmark_cagr_hex": _hex(benchmark["cagr"]),
        "active_cagr_hex": _hex(strategy["cagr"] - benchmark["cagr"]),
        "daily_sharpe_hex": _hex(strategy["daily_sharpe"]),
        "daily_max_drawdown_hex": _hex(strategy["max_drawdown"]),
    }


def evaluate_prospective_outcomes(
    outcomes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate prospective evidence and return its canonical gate evaluation."""

    if isinstance(outcomes, (str, bytes, bytearray)) or not isinstance(
        outcomes, Sequence
    ):
        raise ProspectiveEvaluationError(
            "outcomes must be a finite sequence of mappings"
        )
    verified = [_verify_outcome(value) for value in outcomes]
    verified.sort(
        key=lambda row: (
            row.cycle.offset,
            row.cycle.signal_date,
            row.cycle.holding_start_date,
            row.cycle.outcome_sha256,
        )
    )
    _validate_uniqueness(verified)
    _validate_continuity(verified)

    by_offset: dict[int, list[CycleOutcome]] = {
        offset: [] for offset in range(OFFSET_COUNT)
    }
    for row in verified:
        by_offset[row.cycle.offset].append(row.cycle)
    counts = [len(by_offset[offset]) for offset in range(OFFSET_COUNT)]
    total = len(verified)
    engineering_ready = (
        total >= ENGINEERING_TOTAL_OUTCOMES
        and min(counts, default=0) >= ENGINEERING_MIN_PER_OFFSET
    )
    early_ready = (
        total >= EARLY_STOP_TOTAL_OUTCOMES
        and min(counts, default=0) >= EARLY_STOP_MIN_PER_OFFSET
    )
    formal_ready = (
        total >= FORMAL_GATE_TOTAL_OUTCOMES
        and min(counts, default=0) >= FORMAL_GATE_MIN_PER_OFFSET
    )

    performance: dict[str, Any] | None = None
    early_checks: dict[str, Any] | None = None
    formal_checks: dict[str, bool] | None = None
    direction_gate_passed = False
    reject_major_direction = False
    insolvent = any(row.cycle.ending_nav_fen == 0 for row in verified)
    if early_ready:
        early_metrics = [
            _offset_metrics(
                offset,
                by_offset[offset][:EARLY_STOP_MIN_PER_OFFSET],
            )
            for offset in range(OFFSET_COUNT)
        ]
        early_performance = _performance_mapping(early_metrics)
        positive_net = early_performance["positive_net_compound_offset_count"]
        positive_active = early_performance["positive_active_compound_offset_count"]
        early_reject = (
            positive_net <= EARLY_REJECT_MAX_POSITIVE_NET_OFFSETS
            and positive_active <= EARLY_REJECT_MAX_POSITIVE_ACTIVE_OFFSETS
        )
        early_checks = {
            "positive_net_compound_offset_count": positive_net,
            "positive_active_compound_offset_count": positive_active,
            "positive_net_lte_2": (
                positive_net <= EARLY_REJECT_MAX_POSITIVE_NET_OFFSETS
            ),
            "positive_active_lte_2": (
                positive_active <= EARLY_REJECT_MAX_POSITIVE_ACTIVE_OFFSETS
            ),
            "reject_if_both": early_reject,
            "promotion_allowed": False,
        }
    else:
        early_performance = None
        early_reject = False

    # A preregistered early rejection is terminal.  Supplying outcomes that
    # would only have existed after the route should have stopped cannot
    # resurrect the direction at the 250-outcome gate.
    if insolvent:
        stage = "terminal_insolvency"
        cohort_size = 0
        reject_major_direction = True
        status = "reject_major_direction"
    elif early_reject:
        stage = "early_stop"
        cohort_size = EARLY_STOP_MIN_PER_OFFSET
        performance = early_performance
        reject_major_direction = True
        status = "reject_major_direction"
    elif formal_ready:
        stage = "one_year_directional_gate"
        cohort_size = FORMAL_GATE_MIN_PER_OFFSET
        metrics = [
            _offset_metrics(offset, by_offset[offset][:cohort_size])
            for offset in range(OFFSET_COUNT)
        ]
        performance = _performance_mapping(metrics)
        master = _master_performance(by_offset, cohort_size=cohort_size)
        performance["master_portfolio"] = master
        formal_checks = {
            "net_cagr_q20_gt_zero": (
                _decode_summary(performance, "net_cagr", "q20_hex") > 0.0
            ),
            "net_sharpe_q20_gt_zero": (
                _decode_summary(performance, "net_sharpe", "q20_hex") > 0.0
            ),
            "active_cagr_q20_gt_zero": (
                _decode_summary(performance, "active_cagr", "q20_hex") > 0.0
            ),
            "positive_net_offset_count_gte_8": (
                performance["positive_net_compound_offset_count"]
                >= FORMAL_MIN_POSITIVE_NET_OFFSETS
            ),
            "positive_active_offset_count_gte_7": (
                performance["positive_active_compound_offset_count"]
                >= FORMAL_MIN_POSITIVE_ACTIVE_OFFSETS
            ),
            "worst_max_drawdown_gte_minus_25pct": (
                _decode_summary(
                    performance,
                    "holding_session_max_drawdown",
                    "worst_hex",
                )
                >= FORMAL_WORST_MAX_DRAWDOWN_MIN_PPM / METRIC_SCALE_PPM
            ),
            "master_terminal_wealth_gt_initial": (
                master["terminal_nav_fen"] > master["initial_nav_fen"]
            ),
            "master_net_cagr_gt_zero": (
                float.fromhex(master["net_cagr_hex"]) > 0.0
            ),
            "master_active_cagr_gt_zero": (
                float.fromhex(master["active_cagr_hex"]) > 0.0
            ),
            "master_daily_sharpe_gt_zero": (
                float.fromhex(master["daily_sharpe_hex"]) > 0.0
            ),
            "master_daily_max_drawdown_gte_minus_25pct": (
                float.fromhex(master["daily_max_drawdown_hex"])
                >= FORMAL_WORST_MAX_DRAWDOWN_MIN_PPM / METRIC_SCALE_PPM
            ),
        }
        direction_gate_passed = all(formal_checks.values())
        reject_major_direction = not direction_gate_passed
        status = (
            "one_year_directional_gate_passed"
            if direction_gate_passed
            else "one_year_directional_gate_failed"
        )
    elif early_ready:
        stage = "early_stop"
        cohort_size = EARLY_STOP_MIN_PER_OFFSET
        performance = early_performance
        status = "continue_accumulating"
    elif engineering_ready:
        stage = "engineering_closure"
        cohort_size = ENGINEERING_MIN_PER_OFFSET
        status = "engineering_closure_only"
    else:
        stage = "collecting"
        cohort_size = 0
        status = "accumulating"

    ordered_outer = [row.outer for row in verified]
    payload: dict[str, Any] = {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "evaluator_id": EVALUATOR_ID,
        "evaluation_contract_sha256": EVALUATION_CONTRACT_SHA256,
        "input": {
            "outcome_count": total,
            "ordered_outer_outcomes_sha256": _sha256_payload(ordered_outer),
            "ordered_cycle_outcome_sha256s": [
                row.cycle.outcome_sha256 for row in verified
            ],
            "offset_counts": [
                {"offset": offset, "outcome_count": counts[offset]}
                for offset in range(OFFSET_COUNT)
            ],
            "deployment_sha256": (
                verified[0].cycle.deployment_sha256 if verified else None
            ),
            "execution_contract_sha256": (
                verified[0].cycle.contract_sha256 if verified else None
            ),
        },
        "stage": stage,
        "status": status,
        "cohort_cycle_count_per_offset": cohort_size,
        "engineering_closure_complete": engineering_ready,
        "early_stop_evaluated": early_ready,
        "formal_gate_evaluated": formal_ready and not early_reject,
        "performance": performance,
        "early_stop_checks": early_checks,
        "formal_gate_checks": formal_checks,
        "reject_major_direction": reject_major_direction,
        "direction_gate_passed": direction_gate_passed,
        "performance_promotion_claim_allowed": False,
        "stable_profit_claim_allowed": False,
    }
    payload["evaluation_sha256"] = _sha256_payload(payload)
    canonical_evaluation_json_bytes(payload, verify_hash=True)
    return payload


__all__ = [
    "EARLY_STOP_MIN_PER_OFFSET",
    "EARLY_STOP_TOTAL_OUTCOMES",
    "ENGINEERING_MIN_PER_OFFSET",
    "ENGINEERING_TOTAL_OUTCOMES",
    "EVALUATION_CONTRACT",
    "EVALUATION_CONTRACT_CANONICAL_JSON",
    "EVALUATION_CONTRACT_SHA256",
    "EVALUATION_SCHEMA_VERSION",
    "EVALUATOR_ID",
    "FORMAL_GATE_MIN_PER_OFFSET",
    "FORMAL_GATE_TOTAL_OUTCOMES",
    "FORMAL_MIN_POSITIVE_ACTIVE_OFFSETS",
    "FORMAL_MIN_POSITIVE_NET_OFFSETS",
    "FORMAL_WORST_MAX_DRAWDOWN_MIN_PPM",
    "PERIODS_PER_YEAR_HEX",
    "ProspectiveEvaluationError",
    "canonical_evaluation_json_bytes",
    "evaluate_prospective_outcomes",
]
