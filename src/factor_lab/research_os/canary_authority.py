"""Cross-check physical canary claims against immutable shadow authority rows."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from . import orm
from .shadow_authority import ShadowEvidenceAuthority

try:
    from sqlalchemy import inspect, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session
except ImportError:  # pragma: no cover - production dependencies are mandatory.
    Engine = Any  # type: ignore[assignment,misc]
    Session = inspect = select = None  # type: ignore[assignment,misc]


_SESSION_FIELDS = (
    "account_id",
    "trade_date",
    "role_binding_id",
    "epoch_id",
    "evidence_window_hash",
    "evidence_class",
    "decision_snapshot_id",
    "execution_snapshot_id",
    "mark_snapshot_id",
    "rebalanced",
    "cash",
    "positions_value",
    "nav",
    "benchmark_nav",
    "position_count",
    "account_event_hash",
    "account_event_sequence",
    "session_hash",
    "created_at",
)


def physical_canary_session_errors(
    engine: Engine,
    metadata: Mapping[str, Any],
    *,
    sessions: Sequence[date],
    evidence_by_snapshot: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[str, ...]:
    """Return deterministic errors for a claimed 50x20 canary session chain.

    The canary run record is only an index.  Authority remains in
    ``ros_shadow_sessions`` and the registered snapshot rows, so every claimed
    date/hash/event/binding must match the exact persisted projection.
    """

    errors: list[str] = []
    account_id = str(metadata.get("account_id") or "")
    role_binding_id = str(metadata.get("role_binding_id") or "")
    expected_dates = tuple(item.isoformat() for item in sessions[1:])
    expected_hashes = tuple(map(str, metadata.get("shadow_session_hashes") or ()))
    expected_events = tuple(
        map(str, metadata.get("shadow_account_event_hashes") or ())
    )
    snapshot_map = dict(evidence_by_snapshot or {})
    if not snapshot_map:
        raw_evidence = metadata.get("snapshot_evidence")
        if isinstance(raw_evidence, Sequence) and not isinstance(
            raw_evidence, (str, bytes, bytearray)
        ):
            snapshot_map = {
                str(item.get("snapshot_id") or ""): item
                for item in raw_evidence
                if isinstance(item, Mapping)
            }
    if not account_id or not role_binding_id or len(expected_dates) != 20:
        return ("shadow_session_identity_missing",)
    if Session is None or inspect is None or select is None:
        return ("shadow_session_authority_unavailable",)
    try:
        inspector = inspect(engine)
        if not inspector.has_table("ros_shadow_sessions"):
            return ("shadow_session_authority_table_missing",)
        with Session(engine) as authority_session:
            rows = tuple(
                {field: getattr(model, field) for field in _SESSION_FIELDS}
                for model in authority_session.scalars(
                    select(orm.ShadowSessionModel)
                    .where(orm.ShadowSessionModel.account_id == account_id)
                    .where(orm.ShadowSessionModel.trade_date >= expected_dates[0])
                    .where(orm.ShadowSessionModel.trade_date <= expected_dates[-1])
                    .order_by(orm.ShadowSessionModel.trade_date)
                )
            )
    except Exception:
        return ("shadow_session_authority_query_failed",)

    observed_dates = tuple(str(row["trade_date"]) for row in rows)
    observed_hashes = tuple(str(row["session_hash"]) for row in rows)
    observed_events = tuple(str(row["account_event_hash"]) for row in rows)
    if observed_dates != expected_dates:
        errors.append("shadow_session_dates_not_exact")
    if observed_hashes != expected_hashes:
        errors.append("shadow_session_hashes_not_authoritative")
    if observed_events != expected_events:
        errors.append("shadow_session_events_not_authoritative")
    if any(str(row["role_binding_id"] or "") != role_binding_id for row in rows):
        errors.append("shadow_session_role_binding_mismatch")

    for row in rows:
        try:
            ShadowEvidenceAuthority._validate_stored_projection(
                SimpleNamespace(**dict(row))
            )
        except Exception:
            errors.append("shadow_session_content_hash_invalid")
            continue
        trade_date = str(row["trade_date"])
        if not (
            str(row["evidence_class"] or "") == "engineering"
            and row["epoch_id"] is None
            and row["evidence_window_hash"] is None
        ):
            errors.append("shadow_session_evidence_class_invalid")
        execution = snapshot_map.get(str(row["execution_snapshot_id"] or ""))
        mark = snapshot_map.get(str(row["mark_snapshot_id"] or ""))
        if not (
            execution is not None
            and str(execution.get("tier") or "") == "gold"
            and str(execution.get("role") or "") == "execution"
            and str(execution.get("trade_date") or "") == trade_date
        ):
            errors.append("shadow_execution_snapshot_binding_invalid")
        if not (
            mark is not None
            and str(mark.get("tier") or "") == "gold"
            and str(mark.get("role") or "") == "mark"
            and str(mark.get("trade_date") or "") == trade_date
        ):
            errors.append("shadow_mark_snapshot_binding_invalid")
        decision_id = row["decision_snapshot_id"]
        if decision_id is not None:
            decision = snapshot_map.get(str(decision_id))
            if not (
                decision is not None
                and str(decision.get("tier") or "") == "gold"
                and str(decision.get("role") or "") == "mark"
                and str(decision.get("trade_date") or "") < trade_date
            ):
                errors.append("shadow_decision_snapshot_binding_invalid")
    return tuple(sorted(set(errors)))


__all__ = ["physical_canary_session_errors"]
