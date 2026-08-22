"""Lightweight, read-only WebUI projection for the Research OS catalog.

The production source is ``FACTOR_LAB_DATABASE_URL`` (PostgreSQL by default).
SQLite is accepted only when a caller explicitly opts in, which keeps test
fixtures useful without turning the legacy SQLite database into an implicit
source of Research OS truth.

This module never initializes schemas and never reads artifact JSON files.  A
missing or unavailable catalog is represented as an unavailable projection so
the WebUI can fail quickly and honestly.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
from typing import Any
from urllib.parse import urlparse

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.contracts import StatisticalBudget
from factor_lab.research_os.readiness_audit import (
    READINESS_AUDIT_RUN_TYPE,
    ProductionReadinessAudit,
    ReadinessAuditError,
)
from factor_lab.research_os.runtime import ResearchOSSettings


_ACTIVE_RECOVERY_STATUSES = {"open", "diagnosing", "observing"}
_BLOCKING_DATA_STATUSES = {"disputed", "quarantined", "frozen"}
_PARTITION_STATUSES = (
    "pending",
    "running",
    "succeeded",
    "disputed",
    "quarantined",
    "failed",
)
_PRODUCTION_LEDGER_TABLES = {
    "ros_partition_runs",
    "ros_data_incidents",
    "ros_evidence_epochs",
    "ros_shadow_role_bindings",
    "ros_shadow_sessions",
    "ros_shadow_accounts",
}
_FORWARD_EVIDENCE_TARGET = 60
_RISK_PRIORITY = {
    "frozen_data": 100,
    "dormant": 80,
    "reduced": 70,
    "probation": 50,
    "retired": 40,
    "shadow": 20,
    "walk_forward": 15,
    "canary": 10,
    "preregistered": 5,
    "proposed": 1,
    "active": 0,
}


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _isoish(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _is_sqlite(database_url: str) -> bool:
    value = str(database_url)
    return (
        value in {":memory:", "sqlite://", "sqlite:///:memory:"}
        or value.startswith("sqlite:///")
        or "://" not in value
    )


def _database_kind(database_url: str) -> str:
    if _is_sqlite(database_url):
        return "sqlite_test"
    if database_url.startswith(("postgresql://", "postgresql+")):
        return "postgresql"
    return "unsupported"


def _postgres_available(database_url: str, timeout_seconds: float) -> bool:
    parsed = urlparse(database_url)
    host = parsed.hostname
    port = parsed.port or 5432
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _unavailable_projection(
    *,
    source: str,
    reason: str,
    generated_at: datetime,
) -> dict[str, Any]:
    production = _empty_production_projection(reason=reason)
    return {
        "available": False,
        "authoritative": False,
        "source": source,
        "status": "unavailable",
        "reason": reason,
        "generated_at": _iso(generated_at),
        "champion": {
            "published": False,
            "name": "尚未读取",
            "status": "unavailable",
            "sleeve_weights": [],
            "benchmark_weight": None,
            "cash_weight": None,
            "as_of": None,
            "nav": None,
            "benchmark_nav": None,
        },
        "risk": {
            "state": "unknown",
            "label": "状态未知",
            "tone": "warn",
            "affected_sleeves": [],
        },
        "data_health": {
            "status": "unavailable",
            "label": "未连接",
            "tone": "warn",
            "snapshot_id": None,
            "tier": None,
            "as_of": None,
            "promotion_blocked": True,
            "trust_labels": [],
            "blocking_reasons": [reason],
        },
        "recovery_sla": {
            "open_count": 0,
            "overdue_count": 0,
            "within_sla_count": 0,
            "next_deadline": None,
            "cases": [],
        },
        "research": {
            "experiment_count": 0,
            "trial_count": 0,
            "experiment_statuses": {},
            "recent_experiments": [],
        },
        "sleeves": [],
        "runs": [],
        "totals": {},
        **production,
    }


def _empty_production_projection(*, reason: str | None = None) -> dict[str, Any]:
    """Return a stable UI contract when migration 0007 is not readable.

    The fallback is intentionally empty.  It never attempts to reconstruct
    production state from artifacts, local caches, or legacy SQLite tables.
    """

    schema_available = reason is None
    unavailable_reason = reason or None
    return {
        "backfill": {
            "available": schema_available,
            "reason": unavailable_reason,
            "total_partitions": 0,
            "succeeded_partitions": 0,
            "completion_rate": 0.0,
            "by_status": {status: 0 for status in _PARTITION_STATUSES},
            "datasets": [],
            "troubled_partitions": [],
        },
        "data_incidents": {
            "available": schema_available,
            "reason": unavailable_reason,
            "open_count": 0,
            "by_stage": {},
            "recent_open": [],
        },
        "gold_readiness": {
            "ready": False,
            "label": "尚未就绪",
            "tone": "warn",
            "accepted_snapshot_id": None,
            "as_of": None,
            "blockers": [unavailable_reason] if unavailable_reason else [],
        },
        "production_readiness": {
            "ready": False,
            "status": "config_valid_canary_pending",
            "label": "等待生产就绪审计",
            "tone": "warn",
            "audit_id": None,
            "audited_at": None,
            "accepted_session_count": 0,
            "latest_session": None,
            "blockers": [unavailable_reason] if unavailable_reason else [
                "尚无 PostgreSQL 生产就绪审计"
            ],
            "checks": [],
        },
        "evidence_epoch": {
            "status": "not_frozen",
            "label": "尚未冻结正式 epoch",
            "tone": "warn",
            "epoch_id": None,
            "frozen_at": None,
            "activated_at": None,
            "first_forward_session": None,
            "evidence_window_hash": None,
            "common_session_count": 0,
            "target_session_count": _FORWARD_EVIDENCE_TARGET,
            "remaining_session_count": _FORWARD_EVIDENCE_TARGET,
            "progress_percent": 0.0,
            "ready_challenger_count": 0,
            "comparisons": [],
        },
        "shadow_roles": {
            "available": schema_available,
            "reason": unavailable_reason,
            "champion": None,
            "champions": [],
            "challengers": [],
            "active_role_count": 0,
        },
    }


def _production_readiness_projection(runs: list[Any]) -> dict[str, Any]:
    labels = {
        "config_valid_canary_pending": "配置有效，等待物理 canary",
        "canary_ready": "物理 canary 已闭合，等待全量回填",
        "backfill_complete": "全量回填完成，等待正式门禁",
        "formal_epoch_ready": "正式 epoch 已具备冻结条件",
    }
    invalid_count = 0
    for run in runs:
        try:
            audit = ProductionReadinessAudit.from_run(run)
        except ReadinessAuditError:
            invalid_count += 1
            continue
        status = audit.status.value
        return {
            "ready": audit.ready,
            "status": status,
            "label": labels.get(status, status),
            "tone": "good" if audit.ready else "warn",
            "audit_id": audit.audit_id,
            "audited_at": _iso(audit.audited_at),
            "accepted_session_count": audit.accepted_session_count,
            "latest_session": audit.latest_session,
            "blockers": list(audit.blockers),
            "checks": [item.to_dict() for item in audit.checks],
        }
    blockers = ["尚无 PostgreSQL 生产就绪审计"]
    if invalid_count:
        blockers.append(f"{invalid_count} 条就绪审计未通过内容哈希校验")
    return {
        **_empty_production_projection()["production_readiness"],
        "blockers": blockers,
    }


def _extract_champion(runs: list[Any]) -> tuple[dict[str, Any], str | None]:
    """Extract only explicitly published champion payloads from run metadata."""

    for run in runs:
        metadata = dict(run.metadata or {})
        result = metadata.get("result")
        result = dict(result) if isinstance(result, Mapping) else {}
        outputs = metadata.get("outputs")
        outputs = dict(outputs) if isinstance(outputs, Mapping) else {}
        operation_result = metadata.get("operation_result")
        operation_result = (
            dict(operation_result) if isinstance(operation_result, Mapping) else {}
        )
        operation_outputs = operation_result.get("outputs")
        operation_outputs = (
            dict(operation_outputs) if isinstance(operation_outputs, Mapping) else {}
        )
        candidates: list[Any] = [
            metadata.get("champion"),
            metadata.get("static_champion"),
            metadata.get("projection"),
            result.get("champion"),
            result.get("static_champion"),
            result.get("projection"),
            outputs.get("champion"),
            outputs.get("static_champion"),
            outputs.get("projection"),
            operation_outputs.get("champion"),
            operation_outputs.get("static_champion"),
            operation_outputs.get("projection"),
        ]
        normalized_type = str(run.run_type).replace("-", "_").lower()
        if "champion" in normalized_type or normalized_type in {
            "portfolio_publish",
            "portfolio_allocation",
        }:
            candidates.extend(
                [metadata.get("allocation"), result.get("allocation"), result]
            )

        for raw in candidates:
            if not isinstance(raw, Mapping):
                continue
            payload = dict(raw)
            raw_weights = payload.get("sleeve_weights")
            effective = payload.get("effective_allocation")
            effective = dict(effective) if isinstance(effective, Mapping) else {}
            if not isinstance(raw_weights, Mapping):
                raw_weights = effective.get("sleeve_weights")
            if not isinstance(raw_weights, Mapping):
                continue
            weights = []
            for sleeve_id, weight in raw_weights.items():
                try:
                    normalized_weight = float(weight)
                except (TypeError, ValueError):
                    continue
                weights.append(
                    {
                        "sleeve_id": str(sleeve_id),
                        "name": str(sleeve_id),
                        "weight": normalized_weight,
                    }
                )
            weights.sort(key=lambda item: (-item["weight"], item["sleeve_id"]))
            return (
                {
                    "published": True,
                    "name": str(payload.get("name") or "Champion"),
                    "status": str(payload.get("status") or "published"),
                    "sleeve_weights": weights,
                    "benchmark_weight": payload.get(
                        "benchmark_weight", effective.get("benchmark_weight")
                    ),
                    "cash_weight": payload.get(
                        "cash_weight", effective.get("cash_weight")
                    ),
                    "as_of": str(
                        payload.get("as_of")
                        or payload.get("generated_at")
                        or _iso(run.completed_at or run.started_at)
                    ),
                    "nav": payload.get("nav"),
                    "benchmark_nav": payload.get("benchmark_nav"),
                    "account_id": payload.get("account_id"),
                },
                run.run_id,
            )

    return (
        {
            "published": False,
            "name": "尚未发布",
            "status": "not_published",
            "sleeve_weights": [],
            "benchmark_weight": None,
            "cash_weight": None,
            "as_of": None,
            "nav": None,
            "benchmark_nav": None,
            "account_id": None,
        },
        None,
    )


def _lifecycle_projection(
    events: list[Any], champion: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    latest_by_sleeve: dict[str, Any] = {}
    for event in events:
        latest_by_sleeve.setdefault(str(event.sleeve_id), event)

    champion_weights = {
        row["sleeve_id"]: row["weight"] for row in champion.get("sleeve_weights", [])
    }
    sleeves: list[dict[str, Any]] = []
    for sleeve_id, event in latest_by_sleeve.items():
        evidence = dict(event.evidence or {})
        state = _enum_value(event.to_state)
        sleeves.append(
            {
                "sleeve_id": sleeve_id,
                "name": str(evidence.get("sleeve_name") or sleeve_id),
                "state": state,
                "weight": champion_weights.get(sleeve_id),
                "cause": event.cause,
                "occurred_at": _iso(event.occurred_at),
            }
        )
    sleeves.sort(
        key=lambda row: (-_RISK_PRIORITY.get(row["state"], 30), row["sleeve_id"])
    )

    risky = [
        row for row in sleeves if row["state"] in {"frozen_data", "dormant", "reduced"}
    ]
    if any(row["state"] == "frozen_data" for row in sleeves):
        risk = {"state": "frozen_data", "label": "数据冻结 / 100% 现金", "tone": "bad"}
    elif any(row["state"] == "dormant" for row in sleeves):
        risk = {"state": "dormant", "label": "Sleeve 休眠", "tone": "warn"}
    elif any(row["state"] == "reduced" for row in sleeves):
        risk = {"state": "reduced", "label": "已触发降权", "tone": "warn"}
    elif any(row["state"] == "probation" for row in sleeves):
        risk = {"state": "probation", "label": "恢复观察期", "tone": "warn"}
    elif sleeves and all(row["state"] == "active" for row in sleeves):
        risk = {"state": "active", "label": "正常暴露", "tone": "good"}
    elif sleeves:
        risk = {"state": "research", "label": "研究 / 影子阶段", "tone": "info"}
    else:
        risk = {"state": "not_initialized", "label": "尚未建立", "tone": "info"}
    risk["affected_sleeves"] = risky
    return sleeves, risk


def _data_health_projection(snapshots: list[Any]) -> dict[str, Any]:
    gold = next(
        (row for row in snapshots if _enum_value(row.reference.tier) == "gold"),
        None,
    )
    latest = snapshots[0] if snapshots else None
    latest_is_blocking = bool(
        latest
        and (
            _enum_value(latest.reference.quality_status) in _BLOCKING_DATA_STATUSES
            or any(
                token in str(label).lower()
                for label in latest.reference.trust_labels
                for token in ("unverified", "disputed", "quarantined")
            )
        )
    )
    # A newer quarantined/disputed partition must remain visible even when an
    # older accepted Gold snapshot exists; otherwise ingestion failures would
    # be hidden behind the last good publication.
    selected = latest if latest_is_blocking else (gold or latest)
    if selected is None:
        return {
            "status": "missing",
            "label": "无已发布快照",
            "tone": "bad",
            "snapshot_id": None,
            "tier": None,
            "as_of": None,
            "promotion_blocked": True,
            "trust_labels": [],
            "blocking_reasons": ["尚未在 Research OS catalog 发布数据快照"],
        }

    reference = selected.reference
    status = _enum_value(reference.quality_status)
    tier = _enum_value(reference.tier)
    trust_labels = [str(item) for item in reference.trust_labels]
    manifest = dict(reference.manifest or {})
    blockers: list[str] = []
    for key in ("blocking_reasons", "blocking_issues", "blockers"):
        values = manifest.get(key)
        if isinstance(values, (list, tuple)):
            blockers.extend(str(item) for item in values if item)
    unverified = [
        label
        for label in trust_labels
        if any(
            token in label.lower()
            for token in ("unverified", "disputed", "quarantined")
        )
    ]
    blockers.extend(unverified)
    if tier != "gold":
        blockers.append("尚未发布 Gold 快照")
    if status in _BLOCKING_DATA_STATUSES:
        blockers.append(f"快照质量状态为 {status}")
    blockers = list(dict.fromkeys(blockers))
    promotion_blocked = bool(blockers) or status != "accepted"
    if promotion_blocked:
        label, tone = "阻断晋级", (
            "bad" if status in {"quarantined", "frozen"} else "warn"
        )
    else:
        label, tone = "快照可研究", "good"
    return {
        "status": status,
        "label": label,
        "tone": tone,
        "snapshot_id": reference.snapshot_id,
        "tier": tier,
        "as_of": _iso(reference.as_of),
        "promotion_blocked": promotion_blocked,
        "trust_labels": trust_labels,
        "blocking_reasons": blockers,
    }


def _recovery_projection(cases: list[Any], now: datetime) -> dict[str, Any]:
    projected: list[dict[str, Any]] = []
    deadlines: list[datetime] = []
    for case in cases:
        status = _enum_value(case.status)
        if status not in _ACTIVE_RECOVERY_STATUSES:
            continue
        if status == "open":
            stage = "5 日漂移登记"
            deadline = case.drift_event_due_at
        elif status == "diagnosing":
            stage = "20 日诊断"
            deadline = case.diagnosis_due_at
        else:
            stage = "60 日影子观察"
            deadline = case.earliest_recovery_review_at
        overdue = now > deadline
        deadlines.append(deadline)
        projected.append(
            {
                "recovery_case_id": case.recovery_case_id,
                "sleeve_id": case.sleeve_id,
                "status": status,
                "lifecycle_state": _enum_value(case.lifecycle_state),
                "stage": stage,
                "deadline": _iso(deadline),
                "overdue": overdue,
                "challenger_count": len(case.challenger_ids),
                "challenger_ids": [str(item) for item in case.challenger_ids],
                "data_integrity_failure": bool(case.data_integrity_failure),
            }
        )
    projected.sort(
        key=lambda row: (not row["overdue"], row["deadline"], row["sleeve_id"])
    )
    overdue_count = sum(1 for row in projected if row["overdue"])
    return {
        "open_count": len(projected),
        "overdue_count": overdue_count,
        "within_sla_count": len(projected) - overdue_count,
        "next_deadline": _iso(min(deadlines)) if deadlines else None,
        "cases": projected,
    }


def _sqlalchemy_database_url(database_url: str) -> str:
    if "://" in database_url:
        return database_url
    return f"sqlite:///{Path(database_url).resolve().as_posix()}"


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _role_weights(metadata: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates: list[Any] = [
        metadata.get("sleeve_weights"),
        metadata.get("weights"),
    ]
    for key in ("allocation", "effective_allocation", "projection"):
        nested = metadata.get(key)
        if isinstance(nested, Mapping):
            candidates.append(nested.get("sleeve_weights"))
            effective = nested.get("effective_allocation")
            if isinstance(effective, Mapping):
                candidates.append(effective.get("sleeve_weights"))
    for raw in candidates:
        if not isinstance(raw, Mapping):
            continue
        rows: list[dict[str, Any]] = []
        for sleeve_id, weight in raw.items():
            try:
                normalized = float(weight)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "sleeve_id": str(sleeve_id),
                    "name": str(sleeve_id),
                    "weight": normalized,
                }
            )
        rows.sort(key=lambda item: (-item["weight"], item["sleeve_id"]))
        if rows:
            return rows
    return []


def _read_production_ledger(
    database_url: str,
    *,
    data_health: Mapping[str, Any],
) -> dict[str, Any]:
    """Read migration-0007 production facts using bounded aggregate queries.

    Every statement is either a grouped count or explicitly limited.  This
    keeps the projection suitable for the 30-second WebUI cache and avoids
    loading event/session histories into Python.
    """

    try:
        from sqlalchemy import create_engine, inspect, text
        from sqlalchemy.pool import NullPool
    except ImportError:
        return _empty_production_projection(reason="SQLAlchemy 运行时不可用")

    engine = create_engine(
        _sqlalchemy_database_url(database_url),
        future=True,
        poolclass=NullPool,
    )
    try:
        with engine.connect() as connection:
            table_names = set(inspect(connection).get_table_names())
            missing = sorted(_PRODUCTION_LEDGER_TABLES - table_names)
            if missing:
                return _empty_production_projection(
                    reason="0007 生产账本尚未升级（缺少 " + "、".join(missing) + "）"
                )

            grouped_partitions = (
                connection.execute(
                    text(
                        """
                    SELECT dataset, status, COUNT(*) AS partition_count,
                           MAX(updated_at) AS last_updated_at
                    FROM ros_partition_runs
                    GROUP BY dataset, status
                    ORDER BY dataset, status
                    """
                    )
                )
                .mappings()
                .all()
            )
            datasets: dict[str, dict[str, Any]] = {}
            by_status = {status: 0 for status in _PARTITION_STATUSES}
            for row in grouped_partitions:
                dataset = str(row["dataset"])
                status = str(row["status"])
                count = int(row["partition_count"] or 0)
                by_status[status] = by_status.get(status, 0) + count
                projected = datasets.setdefault(
                    dataset,
                    {
                        "dataset": dataset,
                        "total_partitions": 0,
                        "succeeded_partitions": 0,
                        "completion_rate": 0.0,
                        "by_status": {item: 0 for item in _PARTITION_STATUSES},
                        "last_updated_at": None,
                    },
                )
                projected["total_partitions"] += count
                projected["by_status"][status] = count
                if status == "succeeded":
                    projected["succeeded_partitions"] += count
                last_updated = _isoish(row["last_updated_at"])
                if last_updated and (
                    projected["last_updated_at"] is None
                    or last_updated > projected["last_updated_at"]
                ):
                    projected["last_updated_at"] = last_updated
            dataset_rows = sorted(datasets.values(), key=lambda row: row["dataset"])
            for row in dataset_rows:
                total = int(row["total_partitions"])
                row["completion_rate"] = (
                    round(100.0 * int(row["succeeded_partitions"]) / total, 2)
                    if total
                    else 0.0
                )
            total_partitions = sum(by_status.values())
            succeeded_partitions = int(by_status.get("succeeded", 0))

            troubled_partitions = [
                {
                    "partition_run_id": str(row["partition_run_id"]),
                    "source_id": str(row["source_id"]),
                    "dataset": str(row["dataset"]),
                    "partition_key": str(row["partition_key"]),
                    "status": str(row["status"]),
                    "error_code": row["error_code"],
                    "error": row["error"],
                    "updated_at": _isoish(row["updated_at"]),
                }
                for row in connection.execute(
                    text(
                        """
                        SELECT partition_run_id, source_id, dataset, partition_key,
                               status, error_code, error, updated_at
                        FROM ros_partition_runs
                        WHERE status IN ('disputed', 'quarantined')
                        ORDER BY updated_at DESC, partition_run_id
                        LIMIT 20
                        """
                    )
                ).mappings()
            ]

            incident_groups = (
                connection.execute(
                    text(
                        """
                    SELECT stage, COUNT(*) AS incident_count
                    FROM ros_data_incidents
                    WHERE status = 'open'
                    GROUP BY stage
                    ORDER BY stage
                    """
                    )
                )
                .mappings()
                .all()
            )
            incidents_by_stage = {
                str(row["stage"]): int(row["incident_count"] or 0)
                for row in incident_groups
            }
            recent_open_incidents = [
                {
                    "incident_id": str(row["incident_id"]),
                    "partition_key": str(row["partition_key"]),
                    "stage": str(row["stage"]),
                    "error_code": str(row["error_code"]),
                    "message": str(row["message"]),
                    "occurred_at": _isoish(row["occurred_at"]),
                }
                for row in connection.execute(
                    text(
                        """
                        SELECT incident_id, partition_key, stage, error_code,
                               message, occurred_at
                        FROM ros_data_incidents
                        WHERE status = 'open'
                        ORDER BY occurred_at DESC, incident_id
                        LIMIT 20
                        """
                    )
                ).mappings()
            ]
            open_incident_count = sum(incidents_by_stage.values())

            epoch_row = (
                connection.execute(
                    text(
                        """
                    SELECT epoch.epoch_id, epoch.frozen_at, epoch.activated_at,
                           epoch.first_forward_session, epoch.evidence_window_hash
                    FROM ros_evidence_epoch_active_pointer pointer
                    JOIN ros_evidence_epochs epoch
                      ON epoch.epoch_id = pointer.epoch_id
                    WHERE pointer.pointer_key = 'research_os'
                      AND epoch.activated_at IS NOT NULL
                      AND epoch.closed_at IS NULL
                    LIMIT 1
                    """
                    )
                )
                .mappings()
                .first()
            )

            role_rows = (
                connection.execute(
                    text(
                        """
                    SELECT b.binding_id, b.role, b.role_key, b.account_id,
                           b.sleeve_id, b.experiment_id, b.epoch_id, b.bound_at,
                           b.metadata_json, a.name AS account_name,
                           a.status AS account_status, a.cash, a.nav,
                           a.benchmark_nav, a.as_of
                    FROM ros_shadow_role_bindings b
                    JOIN ros_shadow_accounts a ON a.account_id = b.account_id
                    WHERE b.active = true
                      AND b.role IN ('champion', 'challenger')
                    ORDER BY CASE b.role WHEN 'champion' THEN 0 ELSE 1 END,
                             b.bound_at, b.binding_id
                    LIMIT 20
                    """
                    )
                )
                .mappings()
                .all()
            )

            session_summaries = {
                str(row["binding_id"]): row
                for row in connection.execute(
                    text(
                        """
                        WITH selected_bindings AS (
                            SELECT binding_id
                            FROM ros_shadow_role_bindings
                            WHERE active = true
                              AND role IN ('champion', 'challenger')
                            ORDER BY CASE role WHEN 'champion' THEN 0 ELSE 1 END,
                                     bound_at, binding_id
                            LIMIT 20
                        )
                        SELECT b.binding_id,
                               COUNT(s.trade_date) AS session_count,
                               MIN(s.trade_date) AS first_session,
                               MAX(s.trade_date) AS last_session,
                               SUM(CASE WHEN s.evidence_class = 'forward' THEN 1 ELSE 0 END)
                                   AS forward_session_count,
                               MIN(CASE WHEN s.evidence_class = 'forward' THEN s.trade_date END)
                                   AS first_forward_session,
                               MAX(CASE WHEN s.evidence_class = 'forward' THEN s.trade_date END)
                                   AS last_forward_session
                        FROM selected_bindings b
                        LEFT JOIN ros_shadow_sessions s
                          ON s.role_binding_id = b.binding_id
                        GROUP BY b.binding_id
                        """
                    )
                ).mappings()
            }
            roles: list[dict[str, Any]] = []
            for row in role_rows:
                binding_id = str(row["binding_id"])
                summary = session_summaries.get(binding_id, {})
                metadata = _json_mapping(row["metadata_json"])
                roles.append(
                    {
                        "binding_id": binding_id,
                        "role": str(row["role"]),
                        "role_key": str(row["role_key"]),
                        "account_id": str(row["account_id"]),
                        "account_name": str(row["account_name"]),
                        "account_status": str(row["account_status"]),
                        "sleeve_id": row["sleeve_id"],
                        "experiment_id": row["experiment_id"],
                        "epoch_id": row["epoch_id"],
                        "bound_at": _isoish(row["bound_at"]),
                        "cash": float(row["cash"]),
                        "nav": float(row["nav"]),
                        "benchmark_nav": float(row["benchmark_nav"]),
                        "as_of": _isoish(row["as_of"]),
                        "session_count": int(summary.get("session_count") or 0),
                        "first_session": _isoish(summary.get("first_session")),
                        "last_session": _isoish(summary.get("last_session")),
                        "forward_session_count": int(
                            summary.get("forward_session_count") or 0
                        ),
                        "first_forward_session": _isoish(
                            summary.get("first_forward_session")
                        ),
                        "last_forward_session": _isoish(
                            summary.get("last_forward_session")
                        ),
                        "metadata": metadata,
                        "sleeve_weights": _role_weights(metadata),
                    }
                )

            champions = [row for row in roles if row["role"] == "champion"]
            challengers = [row for row in roles if row["role"] == "challenger"]
            champion = champions[0] if champions else None
            comparisons: list[dict[str, Any]] = []
            if (
                epoch_row is not None
                and epoch_row["activated_at"] is not None
                and epoch_row["first_forward_session"] is not None
                and epoch_row["evidence_window_hash"] is not None
                and champion is not None
                and str(champion.get("epoch_id")) == str(epoch_row["epoch_id"])
                and challengers
            ):
                comparison_counts = {
                    str(row["challenger_binding_id"]): row
                    for row in connection.execute(
                        text(
                            """
                            WITH active_candidates AS (
                                SELECT binding_id
                                FROM ros_shadow_role_bindings
                                WHERE active = true
                                  AND role = 'challenger'
                                  AND epoch_id = :epoch_id
                                ORDER BY bound_at, binding_id
                                LIMIT 19
                            )
                            SELECT candidate.binding_id AS challenger_binding_id,
                                   COUNT(DISTINCT challenger_session.trade_date)
                                       AS common_session_count,
                                   MIN(challenger_session.trade_date) AS first_common_session,
                                   MAX(challenger_session.trade_date) AS last_common_session
                            FROM active_candidates candidate
                            JOIN ros_shadow_sessions challenger_session
                              ON challenger_session.role_binding_id = candidate.binding_id
                            JOIN ros_shadow_sessions champion_session
                              ON champion_session.role_binding_id = :champion_binding_id
                             AND champion_session.trade_date = challenger_session.trade_date
                             AND champion_session.evidence_class = 'forward'
                             AND champion_session.epoch_id = :epoch_id
                             AND champion_session.evidence_window_hash = :window_hash
                            WHERE challenger_session.evidence_class = 'forward'
                              AND challenger_session.epoch_id = :epoch_id
                              AND challenger_session.evidence_window_hash = :window_hash
                              AND challenger_session.trade_date >= :first_forward_session
                            GROUP BY candidate.binding_id
                            """
                        ),
                        {
                            "champion_binding_id": champion["binding_id"],
                            "epoch_id": str(epoch_row["epoch_id"]),
                            "window_hash": str(epoch_row["evidence_window_hash"]),
                            "first_forward_session": str(
                                epoch_row["first_forward_session"]
                            ),
                        },
                    ).mappings()
                }
                for challenger in challengers:
                    summary = comparison_counts.get(challenger["binding_id"], {})
                    count = int(summary.get("common_session_count") or 0)
                    comparisons.append(
                        {
                            "challenger_binding_id": challenger["binding_id"],
                            "challenger_role_key": challenger["role_key"],
                            "challenger_account_id": challenger["account_id"],
                            "challenger_experiment_id": challenger.get("experiment_id"),
                            "champion_account_id": champion["account_id"],
                            "common_session_count": count,
                            "target_session_count": _FORWARD_EVIDENCE_TARGET,
                            "remaining_session_count": max(
                                0, _FORWARD_EVIDENCE_TARGET - count
                            ),
                            "first_common_session": _isoish(
                                summary.get("first_common_session")
                            ),
                            "last_common_session": _isoish(
                                summary.get("last_common_session")
                            ),
                            "ready_for_probation": count >= _FORWARD_EVIDENCE_TARGET,
                        }
                    )

            common_session_count = max(
                (row["common_session_count"] for row in comparisons), default=0
            )
            common_session_count = min(common_session_count, _FORWARD_EVIDENCE_TARGET)
            ready_challenger_count = sum(
                1 for row in comparisons if row["ready_for_probation"]
            )
            if epoch_row is None:
                epoch_projection = _empty_production_projection()["evidence_epoch"]
            elif epoch_row["activated_at"] is None:
                epoch_projection = {
                    **_empty_production_projection()["evidence_epoch"],
                    "status": "frozen_pending_activation",
                    "label": "epoch 已冻结，等待首个完整交易日",
                    "epoch_id": str(epoch_row["epoch_id"]),
                    "frozen_at": _isoish(epoch_row["frozen_at"]),
                }
            else:
                ready = ready_challenger_count > 0
                epoch_projection = {
                    "status": "ready_for_probation" if ready else "accumulating",
                    "label": (
                        "60/60 共同前瞻交易日已满足"
                        if ready
                        else f"前瞻证据积累中 {common_session_count}/60"
                    ),
                    "tone": "good" if ready else "warn",
                    "epoch_id": str(epoch_row["epoch_id"]),
                    "frozen_at": _isoish(epoch_row["frozen_at"]),
                    "activated_at": _isoish(epoch_row["activated_at"]),
                    "first_forward_session": str(epoch_row["first_forward_session"]),
                    "evidence_window_hash": str(epoch_row["evidence_window_hash"]),
                    "common_session_count": common_session_count,
                    "target_session_count": _FORWARD_EVIDENCE_TARGET,
                    "remaining_session_count": max(
                        0, _FORWARD_EVIDENCE_TARGET - common_session_count
                    ),
                    "progress_percent": round(
                        100.0 * common_session_count / _FORWARD_EVIDENCE_TARGET,
                        2,
                    ),
                    "ready_challenger_count": ready_challenger_count,
                    "comparisons": comparisons,
                }

            blockers: list[str] = []
            accepted_gold = (
                data_health.get("tier") == "gold"
                and data_health.get("status") == "accepted"
                and not bool(data_health.get("promotion_blocked"))
            )
            if not accepted_gold:
                blockers.append("尚无无阻断项的 accepted Gold 快照")
            if by_status.get("disputed", 0):
                blockers.append(f"{by_status['disputed']} 个争议分区")
            if by_status.get("quarantined", 0):
                blockers.append(f"{by_status['quarantined']} 个隔离分区")
            if by_status.get("failed", 0):
                blockers.append(f"{by_status['failed']} 个失败分区")
            unfinished = int(by_status.get("pending", 0)) + int(
                by_status.get("running", 0)
            )
            if unfinished:
                blockers.append(f"{unfinished} 个回填分区尚未闭合")
            if open_incident_count:
                blockers.append(f"{open_incident_count} 个未解决数据事件")
            blockers.extend(
                str(item) for item in data_health.get("blocking_reasons", [])
            )
            blockers = list(dict.fromkeys(item for item in blockers if item))
            ready = not blockers
            gold_readiness = {
                "ready": ready,
                "label": "Gold 已就绪" if ready else "Gold 尚未就绪",
                "tone": (
                    "good"
                    if ready
                    else (
                        "bad"
                        if (
                            by_status.get("disputed", 0)
                            or by_status.get("quarantined", 0)
                            or open_incident_count
                        )
                        else "warn"
                    )
                ),
                "accepted_snapshot_id": (
                    data_health.get("snapshot_id") if accepted_gold else None
                ),
                "as_of": data_health.get("as_of") if accepted_gold else None,
                "blockers": blockers,
            }

            return {
                "backfill": {
                    "available": True,
                    "reason": None,
                    "total_partitions": total_partitions,
                    "succeeded_partitions": succeeded_partitions,
                    "completion_rate": (
                        round(100.0 * succeeded_partitions / total_partitions, 2)
                        if total_partitions
                        else 0.0
                    ),
                    "by_status": by_status,
                    "datasets": dataset_rows,
                    "troubled_partitions": troubled_partitions,
                },
                "data_incidents": {
                    "available": True,
                    "reason": None,
                    "open_count": open_incident_count,
                    "by_stage": incidents_by_stage,
                    "recent_open": recent_open_incidents,
                },
                "gold_readiness": gold_readiness,
                "evidence_epoch": epoch_projection,
                "shadow_roles": {
                    "available": True,
                    "reason": None,
                    "champion": champion,
                    "champions": champions,
                    "challengers": challengers,
                    "active_role_count": len(roles),
                },
            }
    except Exception as exc:
        return _empty_production_projection(
            reason=f"0007 生产账本读取失败（{type(exc).__name__}）"
        )
    finally:
        engine.dispose()


def load_research_os_read_model(
    database_url: str | None = None,
    *,
    allow_sqlite: bool = False,
    now: datetime | None = None,
    connect_timeout_seconds: float = 0.12,
    catalog_factory: Callable[[str], Any] = ResearchCatalog,
) -> dict[str, Any]:
    """Build the WebUI projection without mutating or initializing the catalog."""

    generated_at = now or datetime.now(timezone.utc)
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("now must include a timezone")
    configured_url = database_url or os.environ.get("FACTOR_LAB_DATABASE_URL")
    if not configured_url:
        configured_url = ResearchOSSettings().database_url
    source = _database_kind(configured_url)
    if source == "unsupported":
        return _unavailable_projection(
            source=source,
            reason="不支持的 Research OS 数据库协议",
            generated_at=generated_at,
        )
    if source == "sqlite_test" and not allow_sqlite:
        return _unavailable_projection(
            source=source,
            reason="SQLite 仅允许显式测试覆盖，不能作为 WebUI 生产事实源",
            generated_at=generated_at,
        )
    if source == "postgresql" and not _postgres_available(
        configured_url, connect_timeout_seconds
    ):
        return _unavailable_projection(
            source=source,
            reason="Research OS PostgreSQL 当前不可达",
            generated_at=generated_at,
        )

    try:
        with catalog_factory(configured_url) as catalog:
            summary = catalog.catalog_summary()
            snapshots = catalog.list_snapshots(limit=12)
            experiments = catalog.list_experiments(limit=12)
            lifecycle_events = catalog.list_lifecycle_events(limit=100)
            recovery_cases = catalog.list_recovery_cases(limit=50)
            runs = catalog.list_runs(limit=20)
            # Health samples arrive every five minutes and can legitimately
            # displace the latest readiness audit from the general run feed.
            # Query the authoritative audit stream explicitly so the UI never
            # reconstructs readiness from incidental recent runs.
            readiness_runs = catalog.list_runs(
                limit=5,
                run_type=READINESS_AUDIT_RUN_TYPE,
            )

            champion, champion_run_id = _extract_champion(runs)
            data_health = _data_health_projection(snapshots)
            production = _read_production_ledger(
                configured_url,
                data_health=data_health,
            )
            production["production_readiness"] = _production_readiness_projection(
                readiness_runs
            )
            role_champion = production["shadow_roles"].get("champion")
            if role_champion:
                metadata = dict(role_champion.get("metadata") or {})
                effective = metadata.get("effective_allocation")
                effective = dict(effective) if isinstance(effective, Mapping) else {}
                role_weights = list(role_champion.get("sleeve_weights") or [])
                champion = {
                    "published": True,
                    "name": str(role_champion["account_name"]),
                    "status": str(role_champion["account_status"]),
                    "sleeve_weights": role_weights
                    or list(champion.get("sleeve_weights") or []),
                    "benchmark_weight": metadata.get(
                        "benchmark_weight", effective.get("benchmark_weight")
                    ),
                    "cash_weight": metadata.get(
                        "cash_weight", effective.get("cash_weight")
                    ),
                    "as_of": role_champion.get("as_of"),
                    "nav": role_champion.get("nav"),
                    "benchmark_nav": role_champion.get("benchmark_nav"),
                    "account_id": role_champion.get("account_id"),
                    "role_binding_id": role_champion.get("binding_id"),
                    "epoch_id": role_champion.get("epoch_id"),
                    "first_session": role_champion.get("first_session"),
                    "last_session": role_champion.get("last_session"),
                    "forward_session_count": role_champion.get(
                        "forward_session_count", 0
                    ),
                    "run_id": None,
                    "source": "active_shadow_role_binding",
                }
            else:
                account_id = champion.get("account_id")
                if account_id:
                    account = catalog.get_shadow_account(str(account_id))
                    if account is not None:
                        champion["nav"] = account.nav
                        champion["benchmark_nav"] = account.benchmark_nav
                        champion["as_of"] = _iso(account.as_of)
                        champion["account_status"] = account.status
                        champion["event_chain_valid"] = catalog.verify_shadow_chain(
                            str(account_id)
                        )
                champion["run_id"] = champion_run_id

            sleeves, risk = _lifecycle_projection(lifecycle_events, champion)
            recovery_sla = _recovery_projection(recovery_cases, generated_at)
            comparison_index: dict[str, dict[str, Any]] = {}
            for comparison in production["evidence_epoch"].get("comparisons", []):
                for key in (
                    comparison.get("challenger_binding_id"),
                    comparison.get("challenger_role_key"),
                    comparison.get("challenger_account_id"),
                    comparison.get("challenger_experiment_id"),
                ):
                    if key:
                        comparison_index[str(key)] = comparison
            for case in recovery_sla["cases"]:
                matches = [
                    comparison_index[item]
                    for item in case.get("challenger_ids", [])
                    if item in comparison_index
                ]
                count = max((row["common_session_count"] for row in matches), default=0)
                case["forward_common_session_count"] = count
                case["forward_target_session_count"] = _FORWARD_EVIDENCE_TARGET
                case["forward_remaining_session_count"] = max(
                    0, _FORWARD_EVIDENCE_TARGET - count
                )
            recovery_sla["forward_common_session_count"] = production["evidence_epoch"][
                "common_session_count"
            ]
            recovery_sla["forward_target_session_count"] = _FORWARD_EVIDENCE_TARGET
            experiment_rows = [
                {
                    "experiment_id": row.experiment_id,
                    "candidate_id": row.spec.candidate_id,
                    "candidate_kind": _enum_value(row.spec.candidate_kind),
                    "family": row.spec.family,
                    "hypothesis_id": row.spec.preregistration.hypothesis_id,
                    "economic_mechanism": row.spec.preregistration.economic_mechanism,
                    "direction": _enum_value(row.spec.preregistration.direction),
                    "allowed_variant_count": len(
                        row.spec.preregistration.allowed_variants
                    ),
                    "status": _enum_value(row.status),
                    "registered_at": _iso(row.registered_at),
                }
                for row in experiments
            ]
            hypotheses: list[dict[str, Any]] = []
            seen_hypotheses: set[str] = set()
            for row in experiment_rows:
                hypothesis_id = str(row["hypothesis_id"])
                if hypothesis_id in seen_hypotheses:
                    continue
                seen_hypotheses.add(hypothesis_id)
                related = [
                    item
                    for item in experiment_rows
                    if item["hypothesis_id"] == hypothesis_id
                ]
                hypotheses.append(
                    {
                        "hypothesis_id": hypothesis_id,
                        "economic_mechanism": row["economic_mechanism"],
                        "direction": row["direction"],
                        "families": sorted({str(item["family"]) for item in related}),
                        "candidate_count": len(
                            {str(item["candidate_id"]) for item in related}
                        ),
                        "experiment_count": len(related),
                        "statuses": sorted({str(item["status"]) for item in related}),
                    }
                )
            frozen_budget = (
                experiments[0].spec.validation.statistical_budget
                if experiments
                else StatisticalBudget()
            )
            run_rows = [
                {
                    "run_id": row.run_id,
                    "run_type": row.run_type,
                    "status": row.status,
                    "summary": str((row.metadata or {}).get("summary") or ""),
                    "operation": (row.metadata or {}).get("operation"),
                    "partition_key": (row.metadata or {}).get("partition_key"),
                    "started_at": _iso(row.started_at),
                    "completed_at": _iso(row.completed_at),
                    "error": row.error,
                }
                for row in runs
            ]
            totals = dict(summary.totals)
            has_production_facts = bool(
                production["backfill"]["total_partitions"]
                or production["data_incidents"]["open_count"]
                or production["shadow_roles"]["active_role_count"]
                or production["evidence_epoch"]["epoch_id"]
                or production["production_readiness"]["audit_id"]
            )
            has_facts = has_production_facts or any(
                int(value or 0) > 0 for value in totals.values()
            )
            status = (
                "blocked_data"
                if has_facts and not production["gold_readiness"]["ready"]
                else (
                    "blocked_production"
                    if has_facts and not production["production_readiness"]["ready"]
                    else ("ready" if has_facts else "empty")
                )
            )
            return {
                "available": True,
                "authoritative": source == "postgresql",
                "source": source,
                "status": status,
                "reason": None,
                "generated_at": _iso(generated_at),
                "champion": champion,
                "risk": risk,
                "data_health": data_health,
                "recovery_sla": recovery_sla,
                "research": {
                    "experiment_count": int(totals.get("experiments", 0)),
                    "trial_count": int(totals.get("trials", 0)),
                    "experiment_statuses": dict(summary.experiment_statuses),
                    "recent_experiments": experiment_rows,
                    "recent_hypotheses": hypotheses,
                    "statistical_budget": frozen_budget.model_dump(mode="json"),
                },
                "sleeves": sleeves,
                "runs": run_rows,
                "totals": totals,
                "legacy_evidence_count": int(totals.get("legacy_evidence", 0)),
                **production,
            }
    except Exception as exc:  # The page remains available when infra is starting.
        return _unavailable_projection(
            source=source,
            reason=f"Research OS catalog 读取失败（{type(exc).__name__}）",
            generated_at=generated_at,
        )


__all__ = ["load_research_os_read_model"]
