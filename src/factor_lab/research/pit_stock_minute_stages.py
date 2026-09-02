"""Fail-closed orchestration for the two 13.0 minute development stages.

The orchestration layer is intentionally independent of providers and runners.
Callers close over any paths or configuration they need and supply two zero-
argument callbacks.  A failed stage-1 gate must not invoke, inspect, or prepare
stage 2.  A successful stage 2 is accepted only when every candidate-account
artifact is canonically identical to the already-opened stage-1 result.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
import json
from math import isfinite
from types import MappingProxyType
from typing import Any, Callable, Literal, Mapping

import pandas as pd

from factor_lab.research.pit_stock import PITStockContractError


CANDIDATE_ROLES = ("candidate_base", "candidate_stress")
CANDIDATE_FRAME_NAMES = (
    "orders",
    "postings",
    "daily_nav",
    "periods",
    "group_pnl",
)

# These are the only frame columns allowed to differ between stage artifacts.
# They identify the orchestration envelope, not an account observation.
IGNORED_FRAME_STAGE_METADATA_COLUMNS = (
    "stage",
    "stage_id",
    "stage_name",
    "stage_metadata",
)

# Role-indexed phase-gate sections are projected back to the two candidate
# accounts.  Benchmark role entries are deliberately excluded.
PHASE_GATE_CANDIDATE_ROLE_KEYS = (
    "positive_segments",
    "operational",
    "size",
    "industry",
)
PHASE_GATE_SHARED_KEYS = (
    "market_state",
    "future_input_violation_count",
    "stage_1_checks",
    "stage_1_passed",
)

# These fields necessarily change when ADV500 is introduced or merely describe
# the stage envelope.  No other phase-gate field is silently ignored.
IGNORED_PHASE_GATE_KEYS = (
    "base_edge_vs_adv500",
    "complete",
    "passed",
    "stage",
    "stage_id",
    "stage_name",
    "stage_metadata",
)
IGNORED_PHASE_GATE_CHECK_KEYS = (
    "candidate_base_above_adv500_every_segment",
    # The legacy aggregate check spans every role in stage 2.  Candidate-role
    # operational mappings are compared exactly above instead.
    "operational",
)
IGNORED_TOP_LEVEL_METRIC_KEYS = (
    "adv500_base",
    "adv500_stress",
    "minute_provider_protocol",
    "stage",
    "stage_id",
    "stage_name",
    "stage_metadata",
)


@dataclass(frozen=True)
class MinuteDevelopmentStageDecision:
    """Explicit terminal decision from one two-stage development attempt."""

    status: Literal["stopped_after_stage_1", "stage_2_completed"]
    reason: str
    stage_2_call_count: int
    stage_1_result: Any
    stage_2_result: Any | None
    candidate_identity_sha256: Mapping[str, str] | None


def _canonical_value(value: Any) -> Any:
    """Convert supported research artifacts into strict canonical JSON data."""

    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_canonical_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
        )
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, pd.Timedelta):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None or value is pd.NA or value is pd.NaT:
        return None

    # NumPy/pandas scalars expose ``item``; reduce them before the primitive
    # checks without coercing arbitrary user objects to strings.
    if not isinstance(value, (str, bytes, bool, int, float)) and hasattr(
        value, "item"
    ):
        try:
            return _canonical_value(value.item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, bytes):
        return {"__bytes_hex__": value.hex()}
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if pd.isna(value):
            return None
        if not isfinite(value):
            raise PITStockContractError(
                "minute stage identity contains an infinite float"
            )
        return value
    if isinstance(value, str):
        return value
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    raise PITStockContractError(
        f"minute stage identity contains unsupported value {type(value).__name__}"
    )


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _candidate_frame_payload(frame: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(frame, pd.DataFrame):
        raise PITStockContractError(f"minute stage {name} is not a DataFrame")
    if "role" not in frame.columns:
        raise PITStockContractError(f"minute stage {name} lacks role")
    columns = [
        column
        for column in frame.columns
        if str(column) not in IGNORED_FRAME_STAGE_METADATA_COLUMNS
    ]
    work = frame.loc[
        frame["role"].astype(str).isin(CANDIDATE_ROLES), columns
    ].reset_index(drop=True)
    return {
        "columns": [str(column) for column in columns],
        # Row order is part of the frozen artifact identity; DataFrame indexes
        # are not, because every persisted 13.0 artifact is index-free.
        "rows": [
            [_canonical_value(value) for value in row]
            for row in work.itertuples(index=False, name=None)
        ],
    }


def _candidate_role_mapping(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PITStockContractError(
            f"minute stage phase_gate.{field} is not a mapping"
        )
    missing = [role for role in CANDIDATE_ROLES if role not in value]
    if missing:
        raise PITStockContractError(
            f"minute stage phase_gate.{field} lacks candidate roles"
        )
    return {role: _canonical_value(value[role]) for role in CANDIDATE_ROLES}


def _phase_gate_projection(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PITStockContractError("minute stage phase_gate is not a mapping")
    known = (
        set(PHASE_GATE_CANDIDATE_ROLE_KEYS)
        | set(PHASE_GATE_SHARED_KEYS)
        | set(IGNORED_PHASE_GATE_KEYS)
        | {"checks"}
    )
    unknown = sorted(str(key) for key in set(value) - known)
    if unknown:
        raise PITStockContractError(
            f"minute stage phase_gate has unclassified keys: {unknown}"
        )

    projection: dict[str, Any] = {}
    for field in PHASE_GATE_CANDIDATE_ROLE_KEYS:
        if field not in value:
            raise PITStockContractError(
                f"minute stage phase_gate lacks frozen field {field}"
            )
        projection[field] = _candidate_role_mapping(value[field], field=field)
    for field in PHASE_GATE_SHARED_KEYS:
        if field not in value:
            raise PITStockContractError(
                f"minute stage phase_gate lacks frozen field {field}"
            )
        projection[field] = _canonical_value(value[field])

    checks = value.get("checks")
    if not isinstance(checks, Mapping):
        raise PITStockContractError("minute stage phase_gate.checks is not a mapping")
    projection["checks"] = {
        str(key): _canonical_value(item)
        for key, item in checks.items()
        if str(key) not in IGNORED_PHASE_GATE_CHECK_KEYS
    }
    return projection


def _metrics_projection(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PITStockContractError("minute stage metrics is not a mapping")
    missing = [role for role in CANDIDATE_ROLES if role not in value]
    if missing or "phase_gate" not in value:
        raise PITStockContractError(
            "minute stage metrics lacks candidate roles or phase_gate"
        )
    known = (
        set(CANDIDATE_ROLES)
        | {"phase_gate"}
        | set(IGNORED_TOP_LEVEL_METRIC_KEYS)
    )
    unknown = sorted(str(key) for key in set(value) - known)
    if unknown:
        raise PITStockContractError(
            f"minute stage metrics has unclassified keys: {unknown}"
        )
    return {
        **{role: _canonical_value(value[role]) for role in CANDIDATE_ROLES},
        "phase_gate": _phase_gate_projection(value["phase_gate"]),
    }


def candidate_result_identity(result: Any) -> dict[str, str]:
    """Hash the frozen candidate-only projection of one simulation result."""

    components: dict[str, str] = {}
    for name in CANDIDATE_FRAME_NAMES:
        try:
            frame = getattr(result, name)
        except AttributeError as exc:
            raise PITStockContractError(
                f"minute stage result lacks {name}"
            ) from exc
        components[name] = _canonical_sha256(
            _candidate_frame_payload(frame, name=name)
        )
    try:
        metrics = result.metrics
    except AttributeError as exc:
        raise PITStockContractError("minute stage result lacks metrics") from exc
    components["metrics"] = _canonical_sha256(_metrics_projection(metrics))
    components["overall"] = _canonical_sha256(components)
    return components


def _stage_1_passed(result: Any) -> bool:
    """Read exactly the sole field authorized for the stage decision."""

    try:
        value = result.metrics["phase_gate"]["stage_1_passed"]
    except (AttributeError, KeyError, TypeError) as exc:
        raise PITStockContractError(
            "minute stage-1 result lacks metrics.phase_gate.stage_1_passed"
        ) from exc
    if type(value) is not bool:
        raise PITStockContractError("minute stage_1_passed must be a boolean")
    return value


def orchestrate_minute_development_stages(
    stage_1_runner: Callable[[], Any],
    stage_2_runner: Callable[[], Any],
) -> MinuteDevelopmentStageDecision:
    """Run the cost-bounded two-stage development protocol exactly once.

    A false stage-1 flag is terminal and the stage-2 callback is never touched.
    On success, stage 1 is canonically frozen before invoking stage 2 so a
    callback that mutates a shared result cannot evade the identity comparison.
    """

    if not callable(stage_1_runner) or not callable(stage_2_runner):
        raise TypeError("minute stage runners must be callable")
    stage_1_result = stage_1_runner()
    if not _stage_1_passed(stage_1_result):
        return MinuteDevelopmentStageDecision(
            status="stopped_after_stage_1",
            reason="stage_1_gate_failed",
            stage_2_call_count=0,
            stage_1_result=stage_1_result,
            stage_2_result=None,
            candidate_identity_sha256=None,
        )

    stage_1_identity = candidate_result_identity(stage_1_result)
    stage_2_result = stage_2_runner()
    stage_2_identity = candidate_result_identity(stage_2_result)
    mismatches = [
        name
        for name in (*CANDIDATE_FRAME_NAMES, "metrics", "overall")
        if stage_1_identity[name] != stage_2_identity[name]
    ]
    if mismatches:
        raise PITStockContractError(
            "stage-2 candidate replay differs from stage 1: "
            + ", ".join(mismatches)
        )
    return MinuteDevelopmentStageDecision(
        status="stage_2_completed",
        reason="stage_2_candidate_replay_exact",
        stage_2_call_count=1,
        stage_1_result=stage_1_result,
        stage_2_result=stage_2_result,
        candidate_identity_sha256=MappingProxyType(dict(stage_1_identity)),
    )


__all__ = [
    "CANDIDATE_FRAME_NAMES",
    "CANDIDATE_ROLES",
    "IGNORED_FRAME_STAGE_METADATA_COLUMNS",
    "IGNORED_PHASE_GATE_CHECK_KEYS",
    "IGNORED_PHASE_GATE_KEYS",
    "IGNORED_TOP_LEVEL_METRIC_KEYS",
    "MinuteDevelopmentStageDecision",
    "PHASE_GATE_CANDIDATE_ROLE_KEYS",
    "PHASE_GATE_SHARED_KEYS",
    "candidate_result_identity",
    "orchestrate_minute_development_stages",
]
