from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time

from fastapi.testclient import TestClient
import pytest

from factor_lab import webui_app
from factor_lab.research_os.catalog import LifecycleEvent, ResearchCatalog, RunRecord
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    LifecycleState,
    RecoveryCase,
    RecoveryCaseStatus,
    SnapshotTier,
)
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.readiness_audit import (
    READINESS_AUDIT_SCHEMA_VERSION,
    ProductionReadinessAudit,
    ProductionReadinessStatus,
    ReadinessCheck,
)
from factor_lab.webui.services.research_os_read_model import (
    _lifecycle_projection,
    load_research_os_read_model,
)


def _ui_projection() -> dict:
    return {
        "available": True,
        "authoritative": True,
        "source": "postgresql",
        "status": "ready",
        "reason": None,
        "generated_at": "2026-08-22T00:00:00+00:00",
        "champion": {
            "published": True,
            "name": "静态 Champion",
            "status": "published",
            "sleeve_weights": [
                {"sleeve_id": "value_quality", "name": "价值质量", "weight": 0.35}
            ],
            "benchmark_weight": 0.65,
            "cash_weight": 0.0,
            "as_of": "2026-08-22T00:00:00+00:00",
            "nav": 51_000_000.0,
            "benchmark_nav": 50_500_000.0,
        },
        "risk": {
            "state": "reduced",
            "label": "已触发降权",
            "tone": "warn",
            "affected_sleeves": [{"sleeve_id": "value_quality"}],
        },
        "data_health": {
            "status": "accepted",
            "label": "快照可研究",
            "tone": "good",
            "snapshot_id": "gold-20260822",
            "tier": "gold",
            "as_of": "2026-08-22T00:00:00+00:00",
            "promotion_blocked": False,
            "trust_labels": ["pit_verified"],
            "blocking_reasons": [],
        },
        "backfill": {
            "available": True,
            "reason": None,
            "total_partitions": 20,
            "succeeded_partitions": 18,
            "completion_rate": 90.0,
            "by_status": {
                "pending": 1,
                "running": 0,
                "succeeded": 18,
                "disputed": 1,
                "quarantined": 0,
                "failed": 0,
            },
            "datasets": [
                {
                    "dataset": "daily",
                    "total_partitions": 20,
                    "succeeded_partitions": 18,
                    "completion_rate": 90.0,
                    "by_status": {
                        "pending": 1,
                        "running": 0,
                        "succeeded": 18,
                        "disputed": 1,
                        "quarantined": 0,
                        "failed": 0,
                    },
                    "last_updated_at": "2026-08-22T00:00:00+00:00",
                }
            ],
            "troubled_partitions": [],
        },
        "data_incidents": {
            "available": True,
            "reason": None,
            "open_count": 0,
            "by_stage": {},
            "recent_open": [],
        },
        "gold_readiness": {
            "ready": True,
            "label": "Gold 已就绪",
            "tone": "good",
            "accepted_snapshot_id": "gold-20260822",
            "as_of": "2026-08-22T00:00:00+00:00",
            "blockers": [],
        },
        "production_readiness": {
            "ready": False,
            "status": "canary_ready",
            "label": "物理 canary 已闭合，等待全量回填",
            "tone": "warn",
            "audit_id": "readiness-audit-001",
            "audited_at": "2026-08-22T00:00:00+00:00",
            "accepted_session_count": 20,
            "latest_session": "2026-08-22",
            "blockers": ["2016-06 至今的正式分区矩阵尚未闭合"],
            "checks": [],
        },
        "evidence_epoch": {
            "status": "accumulating",
            "label": "前瞻证据积累中 12/60",
            "tone": "warn",
            "epoch_id": "epoch-20260822",
            "frozen_at": "2026-08-22T00:00:00+00:00",
            "activated_at": "2026-08-23T00:00:00+00:00",
            "first_forward_session": "2026-08-24",
            "evidence_window_hash": "e" * 64,
            "common_session_count": 12,
            "target_session_count": 60,
            "remaining_session_count": 48,
            "progress_percent": 20.0,
            "ready_challenger_count": 0,
            "comparisons": [],
        },
        "shadow_roles": {
            "available": True,
            "reason": None,
            "champion": {
                "binding_id": "binding-champion",
                "role": "champion",
                "role_key": "static-core",
                "account_id": "champion-shadow",
                "account_name": "静态 Champion",
                "account_status": "active",
                "nav": 51_000_000.0,
                "benchmark_nav": 50_500_000.0,
                "first_session": "2026-08-01",
                "last_session": "2026-08-22",
                "forward_session_count": 12,
            },
            "champions": [
                {
                    "binding_id": "binding-champion",
                    "role": "champion",
                    "role_key": "static-core",
                    "account_id": "champion-shadow",
                    "account_name": "静态 Champion",
                    "account_status": "active",
                    "nav": 51_000_000.0,
                    "benchmark_nav": 50_500_000.0,
                    "first_session": "2026-08-01",
                    "last_session": "2026-08-22",
                    "forward_session_count": 12,
                }
            ],
            "challengers": [],
            "active_role_count": 1,
        },
        "recovery_sla": {
            "open_count": 1,
            "overdue_count": 0,
            "within_sla_count": 1,
            "next_deadline": "2026-09-01T00:00:00+00:00",
            "cases": [
                {
                    "recovery_case_id": "recovery-value",
                    "sleeve_id": "value_quality",
                    "status": "observing",
                    "lifecycle_state": "reduced",
                    "stage": "60 日影子观察",
                    "deadline": "2026-09-01T00:00:00+00:00",
                    "overdue": False,
                    "challenger_count": 1,
                    "data_integrity_failure": False,
                }
            ],
        },
        "research": {
            "experiment_count": 4,
            "trial_count": 9,
            "experiment_statuses": {"completed": 3, "blocked": 1},
            "recent_experiments": [],
            "recent_hypotheses": [],
            "statistical_budget": {
                "maximum_confirmatory_challengers_per_month": 3,
                "maximum_confirmatory_challengers_per_family_per_month": 1,
                "maximum_diagnostic_branches": 2,
            },
        },
        "sleeves": [
            {
                "sleeve_id": "value_quality",
                "name": "价值质量",
                "state": "reduced",
                "weight": 0.35,
                "cause": "13 周 IR 与成本同时告警",
                "occurred_at": "2026-08-22T00:00:00+00:00",
            }
        ],
        "runs": [
            {
                "run_id": "run-monitor-001",
                "run_type": "monitor_tick",
                "status": "completed",
                "started_at": "2026-08-22T00:00:00+00:00",
                "completed_at": "2026-08-22T00:01:00+00:00",
                "error": None,
            }
        ],
        "totals": {"experiments": 4, "trials": 9},
        "legacy_evidence_count": 2,
    }


def _session_hash(*parts: object) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).hexdigest()


def _readiness_run(now: datetime) -> RunRecord:
    check = ReadinessCheck(
        code="physical_engineering_canary",
        passed=False,
        blockers=("physical_engineering_canary_missing",),
    )
    placeholder = ProductionReadinessAudit(
        audit_id="readiness_" + "0" * 64,
        fingerprint="0" * 64,
        status=ProductionReadinessStatus.CONFIG_VALID_CANARY_PENDING,
        audited_at=now,
        blockers=check.blockers,
        checks=(check,),
    )
    fingerprint = content_fingerprint(
        placeholder._content_payload(),
        domain=READINESS_AUDIT_SCHEMA_VERSION,
    )
    audit = ProductionReadinessAudit(
        audit_id=f"readiness_{fingerprint}",
        fingerprint=fingerprint,
        status=placeholder.status,
        audited_at=now,
        blockers=placeholder.blockers,
        checks=placeholder.checks,
    )
    return RunRecord(
        run_id=audit.audit_id,
        run_type="production_readiness_audit",
        status="completed",
        input_fingerprint=fingerprint,
        started_at=now,
        completed_at=now,
        metadata={"audit": audit.to_dict()},
    )


def _seed_session(
    connection,
    *,
    account_id: str,
    binding_id: str,
    trade_date: str,
    sequence: int,
    epoch_id: str,
    window_hash: str,
) -> None:
    from sqlalchemy import text

    connection.execute(
        text(
            """
            INSERT INTO ros_shadow_sessions(
                account_id, trade_date, role_binding_id, epoch_id,
                evidence_window_hash, evidence_class, decision_snapshot_id,
                execution_snapshot_id, mark_snapshot_id, rebalanced, cash,
                positions_value, nav, benchmark_nav, position_count,
                account_event_hash, account_event_sequence, session_hash, created_at
            ) VALUES (
                :account_id, :trade_date, :binding_id, :epoch_id,
                :window_hash, 'forward', NULL, 'execution-snapshot',
                'mark-snapshot', false, 5000000.0, 45000000.0, 50000000.0,
                50000000.0, 50, :event_hash, :sequence, :session_hash, :created_at
            )
            """
        ),
        {
            "account_id": account_id,
            "trade_date": trade_date,
            "binding_id": binding_id,
            "epoch_id": epoch_id,
            "window_hash": window_hash,
            "event_hash": _session_hash("event", account_id, trade_date, window_hash),
            "sequence": sequence,
            "session_hash": _session_hash(
                "session", account_id, trade_date, window_hash
            ),
            "created_at": datetime(2026, 8, 23, tzinfo=timezone.utc),
        },
    )


def _seed_production_read_model_fixture(database: Path) -> None:
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from factor_lab.research_os import orm

    create_engine = sqlalchemy.create_engine
    text = sqlalchemy.text
    now = datetime(2026, 8, 23, tzinfo=timezone.utc)
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        for snapshot_id, tier, content_hash in (
            ("accepted-gold", SnapshotTier.GOLD, "a" * 64),
            ("execution-snapshot", SnapshotTier.SILVER, "b" * 64),
            ("mark-snapshot", SnapshotTier.SILVER, "c" * 64),
        ):
            catalog.register_snapshot(
                DataSnapshotRef(
                    snapshot_id=snapshot_id,
                    tier=tier,
                    uri=f"s3://factor-lab/{snapshot_id}",
                    content_hash=content_hash,
                    as_of=now,
                    quality_status=DataQualityStatus.ACCEPTED,
                    trust_labels=("pit_verified",),
                )
            )
        for account_id, name in (
            ("champion-account", "静态 Champion"),
            ("challenger-account", "趋势 Challenger"),
        ):
            catalog.create_shadow_account(
                account_id=account_id,
                name=name,
                initial_capital=50_000_000.0,
                opened_at=now,
            )

    engine = create_engine(f"sqlite:///{database.resolve().as_posix()}", future=True)
    assert orm.Base is not None
    orm.Base.metadata.create_all(engine)
    epoch_id = "epoch-formal-001"
    window_hash = "d" * 64
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ros_evidence_epochs(
                    epoch_slot, epoch_id, schema_version, architecture_version,
                    frozen_at, code_hash, configuration_hash,
                    dependency_lock_hash, dirty_patch_hash, epoch_hash,
                    first_forward_session, calendar_snapshot_id,
                    calendar_snapshot_hash, calendar_content_hash,
                    evidence_window_hash, activated_at
                ) VALUES (
                    'research_os', :epoch_id, 'research_os.catalog.v1',
                    'research_os.v1', :frozen_at, :code_hash, :config_hash,
                    :lock_hash, :patch_hash, :epoch_hash, '2026-08-24',
                    'accepted-gold', :calendar_snapshot_hash,
                    :calendar_content_hash, :window_hash, :activated_at
                )
                """
            ),
            {
                "epoch_id": epoch_id,
                "frozen_at": now,
                "code_hash": "1" * 64,
                "config_hash": "2" * 64,
                "lock_hash": "3" * 64,
                "patch_hash": "4" * 64,
                "epoch_hash": "5" * 64,
                "calendar_snapshot_hash": "6" * 64,
                "calendar_content_hash": "7" * 64,
                "window_hash": window_hash,
                "activated_at": now,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO ros_evidence_epoch_active_pointer(
                    pointer_key, epoch_id, updated_at
                ) VALUES ('research_os', :epoch_id, :updated_at)
                """
            ),
            {"epoch_id": epoch_id, "updated_at": now},
        )
        for binding_id, role, role_key, account_id, metadata in (
            (
                "binding-champion",
                "champion",
                "static-core",
                "champion-account",
                {"sleeve_weights": {"value_quality": 0.35, "low_risk": 0.25}},
            ),
            (
                "binding-challenger",
                "challenger",
                "trend-recovery",
                "challenger-account",
                {},
            ),
        ):
            connection.execute(
                text(
                    """
                    INSERT INTO ros_shadow_role_bindings(
                        binding_id, binding_hash, role, role_key, account_id,
                        sleeve_id, experiment_id, epoch_id, active, bound_at,
                        unbound_at, metadata_json
                    ) VALUES (
                        :binding_id, :binding_hash, :role, :role_key, :account_id,
                        NULL, NULL, :epoch_id, true, :bound_at, NULL, :metadata_json
                    )
                    """
                ),
                {
                    "binding_id": binding_id,
                    "binding_hash": _session_hash("binding", binding_id),
                    "role": role,
                    "role_key": role_key,
                    "account_id": account_id,
                    "epoch_id": epoch_id,
                    "bound_at": now,
                    "metadata_json": json.dumps(metadata),
                },
            )
        connection.execute(
            text(
                """
                UPDATE ros_shadow_accounts
                SET cash = 5000000.0, nav = CASE account_id
                        WHEN 'champion-account' THEN 51000000.0 ELSE 50500000.0 END,
                    benchmark_nav = 50250000.0, status = 'active',
                    as_of = :as_of, updated_at = :as_of
                WHERE account_id IN ('champion-account', 'challenger-account')
                """
            ),
            {"as_of": now},
        )
        partition_rows = (
            ("p-daily-1", "tushare", "daily", "2026-08-20", "succeeded"),
            ("p-daily-2", "tushare", "daily", "2026-08-21", "succeeded"),
            ("p-daily-3", "tushare", "daily", "2026-08-22", "disputed"),
            ("p-adj-1", "tushare", "adj_factor", "2026-08-22", "quarantined"),
            ("p-basic-1", "tushare", "daily_basic", "2026-08-22", "pending"),
        )
        for (
            partition_run_id,
            source_id,
            dataset,
            partition_key,
            status,
        ) in partition_rows:
            connection.execute(
                text(
                    """
                    INSERT INTO ros_partition_runs(
                        partition_run_id, source_id, dataset, partition_key,
                        generation, status,
                        lease_owner, lease_token, lease_expires_at, attempts, run_id,
                        output_snapshot_id, input_hash, output_hash, vendor_revision,
                        details_json, error_code, error, created_at, updated_at,
                        started_at, completed_at
                    ) VALUES (
                        :id, :source, :dataset, :partition_key, 'base', :status,
                        NULL, NULL,
                        NULL, 1, NULL, NULL, NULL, NULL, NULL, '{}',
                        :error_code, :error, :created_at, :updated_at, NULL, NULL
                    )
                    """
                ),
                {
                    "id": partition_run_id,
                    "source": source_id,
                    "dataset": dataset,
                    "partition_key": partition_key,
                    "status": status,
                    "error_code": (
                        "reconciliation_conflict"
                        if status in {"disputed", "quarantined"}
                        else None
                    ),
                    "error": (
                        "bounded fixture conflict"
                        if status in {"disputed", "quarantined"}
                        else None
                    ),
                    "created_at": now,
                    "updated_at": now,
                },
            )
        connection.execute(
            text(
                """
                INSERT INTO ros_data_incidents(
                    incident_id, incident_hash, partition_run_id, partition_key,
                    stage, status, error_code, message, source_ids_json,
                    evidence_hashes_json, payload_json, occurred_at, resolved_at,
                    resolution_hash
                ) VALUES (
                    'incident-open', :incident_hash, 'p-daily-3', '2026-08-22',
                    'silver', 'open', 'reconciliation_conflict',
                    'price conflict above tolerance', '["tushare","diemeng"]',
                    '[]', '{}', :occurred_at, NULL, NULL
                )
                """
            ),
            {"incident_hash": "8" * 64, "occurred_at": now},
        )
        first_session = date(2026, 8, 24)
        for index in range(59):
            trade_date = (first_session + timedelta(days=index)).isoformat()
            _seed_session(
                connection,
                account_id="champion-account",
                binding_id="binding-champion",
                trade_date=trade_date,
                sequence=index + 1,
                epoch_id=epoch_id,
                window_hash=window_hash,
            )
            _seed_session(
                connection,
                account_id="challenger-account",
                binding_id="binding-challenger",
                trade_date=trade_date,
                sequence=index + 1,
                epoch_id=epoch_id,
                window_hash=window_hash,
            )
        # A shared date in the wrong evidence window must not become day 60.
        wrong_window_date = (first_session + timedelta(days=59)).isoformat()
        for account_id, binding_id in (
            ("champion-account", "binding-champion"),
            ("challenger-account", "binding-challenger"),
        ):
            _seed_session(
                connection,
                account_id=account_id,
                binding_id=binding_id,
                trade_date=wrong_window_date,
                sequence=60,
                epoch_id=epoch_id,
                window_hash="9" * 64,
            )
    engine.dispose()


def test_sqlite_read_model_requires_explicit_test_opt_in(tmp_path: Path) -> None:
    database = tmp_path / "research-os.db"

    projection = load_research_os_read_model(str(database))

    assert projection["available"] is False
    assert projection["source"] == "sqlite_test"
    assert "显式测试" in projection["reason"]
    assert not database.exists()


def test_read_model_projects_catalog_without_legacy_artifact_scan(
    tmp_path: Path,
) -> None:
    database = tmp_path / "research-os.db"
    now = datetime(2026, 8, 22, tzinfo=timezone.utc)
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(
            DataSnapshotRef(
                snapshot_id="gold-20260822",
                tier=SnapshotTier.GOLD,
                uri="s3://factor-lab/gold/gold-20260822",
                content_hash="a" * 64,
                as_of=now,
                quality_status=DataQualityStatus.ACCEPTED,
                trust_labels=("st_history_unverified",),
            )
        )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="value-quality-active-20260820",
                sleeve_id="value_quality",
                to_state=LifecycleState.ACTIVE,
                cause="fixture active state",
                evidence={"sleeve_name": "价值质量"},
                occurred_at=now - timedelta(days=2),
            )
        )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="value-quality-reduced-20260822",
                sleeve_id="value_quality",
                from_state=LifecycleState.ACTIVE,
                to_state=LifecycleState.REDUCED,
                cause="two simultaneous weekly alerts",
                evidence={"sleeve_name": "价值质量"},
                occurred_at=now - timedelta(days=1),
            )
        )
        catalog.save_recovery_case(
            RecoveryCase(
                recovery_case_id="recovery-value-quality",
                sleeve_id="value_quality",
                status=RecoveryCaseStatus.DIAGNOSING,
                lifecycle_state=LifecycleState.REDUCED,
                triggered_at=now - timedelta(days=30),
                drift_event_due_at=now - timedelta(days=25),
                diagnosis_due_at=now - timedelta(days=10),
                earliest_recovery_review_at=now + timedelta(days=30),
            )
        )
        catalog.save_run(
            RunRecord(
                run_id="run_champion_publish_001",
                run_type="champion_publish",
                status="completed",
                input_fingerprint="b" * 64,
                started_at=now - timedelta(minutes=2),
                completed_at=now - timedelta(minutes=1),
                metadata={
                    "champion": {
                        "name": "静态 Champion",
                        "sleeve_weights": {"value_quality": 0.35},
                        "benchmark_weight": 0.65,
                        "cash_weight": 0.0,
                    }
                },
            )
        )

    with sqlite3.connect(database) as connection:
        tables_before = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    projection = load_research_os_read_model(str(database), allow_sqlite=True, now=now)
    with sqlite3.connect(database) as connection:
        tables_after = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert projection["available"] is True
    assert projection["authoritative"] is False
    assert projection["champion"]["name"] == "静态 Champion"
    assert projection["champion"]["sleeve_weights"][0]["weight"] == 0.35
    assert projection["risk"]["state"] == "reduced"
    assert projection["data_health"]["promotion_blocked"] is True
    assert "st_history_unverified" in projection["data_health"]["blocking_reasons"]
    assert projection["recovery_sla"]["overdue_count"] == 1
    assert projection["runs"][0]["run_id"] == "run_champion_publish_001"
    assert projection["research"]["recent_hypotheses"] == []
    assert (
        projection["research"]["statistical_budget"][
            "maximum_confirmatory_challengers_per_month"
        ]
        == 3
    )
    assert projection["backfill"]["available"] is False
    assert "0007" in projection["backfill"]["reason"]
    assert tables_after == tables_before
    assert "ros_partition_runs" not in tables_after


def test_read_model_projects_0007_backfill_incidents_roles_and_exact_59_60(
    tmp_path: Path,
) -> None:
    database = tmp_path / "research-os-0007.db"
    _seed_production_read_model_fixture(database)
    now = datetime(2026, 11, 1, tzinfo=timezone.utc)

    started = time.monotonic()
    projection = load_research_os_read_model(str(database), allow_sqlite=True, now=now)
    elapsed = time.monotonic() - started

    assert elapsed < 1.0
    assert projection["backfill"]["total_partitions"] == 5
    assert projection["backfill"]["succeeded_partitions"] == 2
    assert projection["backfill"]["by_status"]["disputed"] == 1
    assert projection["backfill"]["by_status"]["quarantined"] == 1
    assert projection["backfill"]["by_status"]["pending"] == 1
    assert projection["data_incidents"]["open_count"] == 1
    assert projection["data_incidents"]["by_stage"] == {"silver": 1}
    assert projection["gold_readiness"]["ready"] is False
    assert "1 个争议分区" in projection["gold_readiness"]["blockers"]

    assert projection["champion"]["source"] == "active_shadow_role_binding"
    assert projection["champion"]["account_id"] == "champion-account"
    assert projection["champion"]["nav"] == 51_000_000.0
    assert projection["champion"]["sleeve_weights"] == [
        {"sleeve_id": "value_quality", "name": "value_quality", "weight": 0.35},
        {"sleeve_id": "low_risk", "name": "low_risk", "weight": 0.25},
    ]
    assert projection["shadow_roles"]["active_role_count"] == 2
    assert projection["shadow_roles"]["champion"]["first_session"] == "2026-08-24"
    assert projection["shadow_roles"]["challengers"][0]["last_session"] == "2026-10-22"

    epoch = projection["evidence_epoch"]
    assert epoch["status"] == "accumulating"
    assert epoch["common_session_count"] == 59
    assert epoch["remaining_session_count"] == 1
    assert epoch["comparisons"][0]["common_session_count"] == 59
    assert epoch["comparisons"][0]["last_common_session"] == "2026-10-21"

    sqlalchemy = pytest.importorskip("sqlalchemy")
    engine = sqlalchemy.create_engine(
        f"sqlite:///{database.resolve().as_posix()}", future=True
    )
    with engine.begin() as connection:
        valid_day_60 = (date(2026, 8, 24) + timedelta(days=60)).isoformat()
        _seed_session(
            connection,
            account_id="champion-account",
            binding_id="binding-champion",
            trade_date=valid_day_60,
            sequence=61,
            epoch_id="epoch-formal-001",
            window_hash="d" * 64,
        )
        _seed_session(
            connection,
            account_id="challenger-account",
            binding_id="binding-challenger",
            trade_date=valid_day_60,
            sequence=61,
            epoch_id="epoch-formal-001",
            window_hash="d" * 64,
        )
    engine.dispose()

    ready = load_research_os_read_model(str(database), allow_sqlite=True, now=now)[
        "evidence_epoch"
    ]
    assert ready["common_session_count"] == 60
    assert ready["remaining_session_count"] == 0
    assert ready["status"] == "ready_for_probation"
    assert ready["ready_challenger_count"] == 1


def test_open_data_incident_fail_closes_effective_lifecycle_projection(
    tmp_path: Path,
) -> None:
    database = tmp_path / "research-os-open-incident.db"
    _seed_production_read_model_fixture(database)
    occurred_at = datetime(2026, 8, 24, tzinfo=timezone.utc)
    with ResearchCatalog(database) as catalog:
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="value-quality-reduced-before-data-incident-overlay",
                sleeve_id="value_quality",
                to_state=LifecycleState.REDUCED,
                cause="fixture reduced state",
                evidence={"sleeve_name": "价值质量"},
                occurred_at=occurred_at - timedelta(microseconds=1),
            )
        )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="value-quality-dormant-before-data-incident-overlay",
                sleeve_id="value_quality",
                from_state=LifecycleState.REDUCED,
                to_state=LifecycleState.DORMANT,
                cause="negative 26-week IR",
                evidence={"sleeve_name": "价值质量"},
                occurred_at=occurred_at,
            )
        )
        for index in range(101):
            catalog.append_lifecycle_event(
                LifecycleEvent(
                idempotency_key=f"value-quality-newer-noise-{index:03d}",
                sleeve_id="value_quality",
                from_state=LifecycleState.DORMANT,
                to_state=LifecycleState.DORMANT,
                    cause="high-frequency lifecycle audit noise",
                    evidence={"fixture_index": index},
                    occurred_at=occurred_at + timedelta(seconds=index + 1),
                )
            )
        for index in range(1_005):
            catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=f"ephemeral-canary-{index:04d}",
                    sleeve_id=f"engineering-canary-{index:04d}",
                    to_state=LifecycleState.CANARY,
                    cause="ephemeral physical canary",
                    evidence={"fixture_index": index},
                    occurred_at=occurred_at + timedelta(minutes=10, seconds=index),
                )
            )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="low-risk-probation-before-active",
                sleeve_id="low_risk",
                to_state=LifecycleState.PROBATION,
                cause="fixture probation state",
                evidence={"sleeve_name": "低风险防御"},
                occurred_at=occurred_at - timedelta(microseconds=1),
            )
        )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="low-risk-active-before-data-incident-overlay",
                sleeve_id="low_risk",
                from_state=LifecycleState.PROBATION,
                to_state=LifecycleState.ACTIVE,
                cause="probation completed",
                evidence={"sleeve_name": "低风险防御"},
                occurred_at=occurred_at,
            )
        )

    projection = load_research_os_read_model(
        str(database),
        allow_sqlite=True,
        now=datetime(2026, 8, 25, tzinfo=timezone.utc),
    )

    sleeves = {row["sleeve_id"]: row for row in projection["sleeves"]}
    assert projection["data_incidents"]["open_count"] == 1
    assert set(sleeves) == {"low_risk", "value_quality"}
    assert {row["state"] for row in sleeves.values()} == {"frozen_data"}
    assert sleeves["value_quality"]["catalog_state"] == "dormant"
    assert sleeves["low_risk"]["catalog_state"] == "active"
    assert {
        row["cause"] for row in sleeves.values()
    } == {"open_data_incident_override"}
    assert projection["risk"]["state"] == "frozen_data"
    assert projection["risk"]["tone"] == "bad"
    assert projection["risk"]["cause"] == "open_data_incident_override"
    assert projection["risk"]["open_data_incident_count"] == 1
    assert {
        row["sleeve_id"] for row in projection["risk"]["affected_sleeves"]
    } == {"low_risk", "value_quality"}


def test_incident_override_is_frozen_even_without_any_sleeve_projection() -> None:
    sleeves, risk = _lifecycle_projection(
        [],
        {},
        open_data_incident_count=1,
    )
    assert sleeves == []
    assert risk == {
        "state": "frozen_data",
        "label": "数据冻结 / 100% 现金",
        "tone": "bad",
        "cause": "open_data_incident_override",
        "open_data_incident_count": 1,
        "affected_sleeves": [],
    }


def test_missing_production_incident_authority_fails_closed() -> None:
    sleeves, risk = _lifecycle_projection(
        [],
        {},
        incident_authority_available=False,
        require_incident_authority=True,
    )
    assert sleeves == []
    assert risk["state"] == "frozen_data"
    assert risk["tone"] == "bad"
    assert risk["cause"] == "incident_authority_unavailable_override"


def test_unavailable_local_postgres_fails_fast() -> None:
    started = time.monotonic()

    projection = load_research_os_read_model(
        "postgresql+psycopg://factor_lab:secret@127.0.0.1:1/factor_lab",
        connect_timeout_seconds=0.05,
    )

    assert projection["available"] is False
    assert projection["source"] == "postgresql"
    assert "secret" not in str(projection)
    assert time.monotonic() - started < 0.5


def test_read_model_projects_standard_weight_reestimation_output(
    tmp_path: Path,
) -> None:
    database = tmp_path / "research-os.db"
    now = datetime(2026, 8, 22, tzinfo=timezone.utc)
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        catalog.save_run(
            RunRecord(
                run_id="run_weight_reestimation_001",
                run_type="dagster:monthly_research:weight_reestimation",
                status="completed",
                input_fingerprint="c" * 64,
                started_at=now - timedelta(minutes=2),
                completed_at=now - timedelta(minutes=1),
                metadata={
                    "operation": "weight_reestimation",
                    "outputs": {
                        "static_champion": {
                            "sleeve_weights": {
                                "value_quality": 0.35,
                                "low_risk": 0.25,
                            }
                        }
                    },
                },
            )
        )

    projection = load_research_os_read_model(str(database), allow_sqlite=True, now=now)

    assert projection["champion"]["published"] is True
    assert projection["champion"]["run_id"] == "run_weight_reestimation_001"
    assert {row["sleeve_id"] for row in projection["champion"]["sleeve_weights"]} == {
        "value_quality",
        "low_risk",
    }


def test_read_model_projects_authoritative_champion_control_allocation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "research-os.db"
    now = datetime(2026, 8, 22, tzinfo=timezone.utc)
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        catalog.save_run(
            RunRecord(
                run_id="champ_001",
                run_type="champion_projection",
                status="completed",
                input_fingerprint="d" * 64,
                started_at=now,
                completed_at=now,
                metadata={
                    "projection": {
                        "projection_id": "d" * 64,
                        "generated_at": now.isoformat(),
                        "effective_allocation": {
                            "sleeve_weights": {"value_quality": 0.35},
                            "benchmark_weight": 0.65,
                            "cash_weight": 0.0,
                            "reason": "lifecycle_degradation_moves_removed_weight_to_benchmark",
                        },
                    }
                },
            )
        )

    projection = load_research_os_read_model(str(database), allow_sqlite=True, now=now)

    assert projection["champion"]["published"] is True
    assert projection["champion"]["run_id"] == "champ_001"
    assert projection["champion"]["benchmark_weight"] == 0.65
    assert projection["champion"]["cash_weight"] == 0.0
    assert projection["champion"]["sleeve_weights"] == [
        {"sleeve_id": "value_quality", "name": "value_quality", "weight": 0.35}
    ]


def test_read_model_queries_readiness_stream_separately_from_recent_health_samples(
    tmp_path: Path,
) -> None:
    database = tmp_path / "research-os.db"
    now = datetime(2026, 8, 22, 12, tzinfo=timezone.utc)
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        catalog.save_run(_readiness_run(now - timedelta(hours=1)))
        for index in range(25):
            observed = now + timedelta(minutes=index)
            catalog.save_run(
                RunRecord(
                    run_id=f"health-{index:02d}",
                    run_type="dagster_code_location_health_sample",
                    status="succeeded",
                    input_fingerprint=f"{index:064x}",
                    started_at=observed,
                    completed_at=observed,
                    metadata={},
                )
            )

    projection = load_research_os_read_model(
        str(database), allow_sqlite=True, now=now
    )

    assert projection["production_readiness"]["status"] == (
        "config_valid_canary_pending"
    )
    assert projection["production_readiness"]["audit_id"].startswith("readiness_")
    assert projection["production_readiness"]["blockers"] == [
        "physical_engineering_canary_missing"
    ]


def test_overview_uses_research_os_without_scanning_legacy_artifacts(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        webui_app, "get_research_os_read_model", lambda: _ui_projection()
    )
    monkeypatch.setattr(
        webui_app,
        "_build_legacy_overview_snapshot",
        lambda: (_ for _ in ()).throw(AssertionError("legacy artifacts were scanned")),
    )
    webui_app._OVERVIEW_CACHE.update({"key": None, "created_at": 0.0, "payload": None})

    response = TestClient(webui_app.app).get("/")

    assert response.status_code == 200
    assert "Champion 与当前暴露" in response.text
    assert "已触发降权" in response.text
    assert "gold-20260822" in response.text
    assert "60 日影子观察" in response.text
    assert "Champion / Challenger 影子账户" in response.text
    assert "champion-shadow" in response.text
    assert "12/60" in response.text
    assert "Legacy 研究参考" not in response.text
    assert "legacy_factor" not in response.text


def test_group_pages_include_research_os_summaries(monkeypatch) -> None:
    monkeypatch.setattr(
        webui_app, "get_research_os_read_model", lambda: _ui_projection()
    )
    client = TestClient(webui_app.app)

    research = client.get("/research")
    portfolios = client.get("/portfolios")
    runs = client.get("/runs")

    assert research.status_code == portfolios.status_code == runs.status_code == 200
    assert "Research OS 实验与否证" in research.text
    assert "最近假设谱系" in research.text
    assert "统计预算" in research.text
    assert "Lifetime 试验" in research.text
    assert "Champion / Sleeve 权重" in portfolios.text
    assert "价值质量" in portfolios.text
    assert "Research OS 运行记录" in runs.text
    assert "run-monitor-001" in runs.text
    assert "回填进度" in runs.text
    assert "12/60" in runs.text


def test_five_primary_pages_never_call_legacy_sqlite_or_json_helpers(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        webui_app, "get_research_os_read_model", lambda: _ui_projection()
    )

    def legacy_access_forbidden(*_args, **_kwargs):
        raise AssertionError("active WebUI path attempted legacy SQLite/JSON access")

    for name in (
        "_build_legacy_overview_snapshot",
        "_build_legacy_research_page_context",
        "_load_legacy_portfolios_page_context",
        "_load_legacy_runs_page_context",
        "_artifact_run_snapshots",
        "_latest_db_run",
        "_quick_latest_runs",
        "_read_json_safely",
        "fetch_all",
        "fetch_one",
    ):
        monkeypatch.setattr(webui_app, name, legacy_access_forbidden)
    monkeypatch.setattr(webui_app, "ExperimentStore", legacy_access_forbidden)
    webui_app._OVERVIEW_CACHE.update({"key": None, "created_at": 0.0, "payload": None})

    client = TestClient(webui_app.app)
    responses = {
        path: client.get(path)
        for path in ("/", "/research", "/portfolios", "/runs", "/data-sources")
    }

    assert all(response.status_code == 200 for response in responses.values())
    assert "Research OS 实验与否证" in responses["/research"].text
    assert "Champion / Sleeve 权重" in responses["/portfolios"].text
    assert "run-monitor-001" in responses["/runs"].text
    assert "Research OS 当前事实源：PostgreSQL" in responses["/data-sources"].text
    assert "生产数据闭合状态" in responses["/data-sources"].text
    assert "Gold 已就绪" in responses["/data-sources"].text
    for path in ("/research", "/portfolios", "/runs"):
        assert "Legacy" not in responses[path].text


def test_every_user_get_route_is_inside_research_os_boundary(monkeypatch) -> None:
    """New GET routes must be projected, redirected, or explicitly framework-owned."""

    monkeypatch.setattr(
        webui_app, "get_research_os_read_model", lambda: _ui_projection()
    )

    def legacy_access_forbidden(*_args, **_kwargs):
        raise AssertionError(
            "GET route attempted legacy artifact/SQLite/evaluator access"
        )

    for name in (
        "_build_legacy_overview_snapshot",
        "_build_legacy_research_page_context",
        "_load_legacy_portfolios_page_context",
        "_load_legacy_runs_page_context",
        "_artifact_run_snapshots",
        "_latest_db_run",
        "_quick_latest_runs",
        "_read_json_safely",
        "read_jsonl",
        "fetch_all",
        "fetch_one",
        "get_conn",
        "get_cached_health_metrics",
        "compute_weekly_report",
        "build_research_quality_summary",
        "build_harvest_report",
        "build_autonomous_strategy_lab_report",
        "build_promotion_scorecard",
        "build_candidate_detail_context",
        "latest_task_states",
        "_quick_research_queue_snapshot",
        "_quick_heartbeat",
        "_load_llm_usage_rows",
        "_latest_llm_usage_rows",
    ):
        monkeypatch.setattr(webui_app, name, legacy_access_forbidden)
    monkeypatch.setattr(webui_app, "ExperimentStore", legacy_access_forbidden)
    monkeypatch.setattr(webui_app.sqlite3, "connect", legacy_access_forbidden)

    original_read_text = Path.read_text
    original_glob = Path.glob

    def guarded_read_text(path: Path, *args, **kwargs):
        normalized = path.as_posix().lower()
        if "/artifacts/" in normalized:
            legacy_access_forbidden(path)
        return original_read_text(path, *args, **kwargs)

    def guarded_glob(path: Path, pattern: str):
        if "/artifacts" in path.as_posix().lower():
            legacy_access_forbidden(path, pattern)
        return original_glob(path, pattern)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    monkeypatch.setattr(Path, "glob", guarded_glob)
    webui_app._OVERVIEW_CACHE.update({"key": None, "created_at": 0.0, "payload": None})

    framework_get_routes = {
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
    }
    projected_get_routes = {
        "/",
        "/research",
        "/portfolios",
        "/runs",
        "/data-sources",
        "/settings",
    }
    redirected_route_patterns = set(webui_app.RESEARCH_OS_GET_REDIRECTS) | {
        "/runs/{run_id}",
        "/candidates/{candidate_id}",
    }
    declared_get_routes = {
        route.path
        for route in webui_app.app.routes
        if "GET" in (getattr(route, "methods", None) or set())
    }
    assert (
        declared_get_routes
        == framework_get_routes | projected_get_routes | redirected_route_patterns
    )

    client = TestClient(webui_app.app)
    for path in projected_get_routes | framework_get_routes:
        assert client.get(path, follow_redirects=False).status_code == 200, path
    assert client.get("/static/webui.css", follow_redirects=False).status_code == 200

    for path, target in webui_app.RESEARCH_OS_GET_REDIRECTS.items():
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 307, path
        assert response.headers["location"] == target
    for path, target in (
        ("/runs/example", "/runs"),
        ("/candidates/example", "/research"),
    ):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == target

    assert client.get("/ops/run/workflow", follow_redirects=False).status_code == 405
    assert client.get("/hermes", follow_redirects=False).status_code == 404
    assert client.get("/agents", follow_redirects=False).status_code == 404


def test_rendered_secondary_navigation_excludes_redirect_only_links(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        webui_app, "get_research_os_read_model", lambda: _ui_projection()
    )
    client = TestClient(webui_app.app)
    expected = {
        "/research": {
            "/research": "研究总览",
            "/research#hypothesis-lineage": "假设谱系",
            "/research#trial-budget": "统计预算",
            "/research#experiments": "实验与否证",
            "/research#recovery": "恢复流程",
        },
        "/portfolios": {"/portfolios": "Champion / Sleeve"},
        "/runs": {"/runs": "Research OS 运行"},
        "/data-sources": {
            "/data-sources": "数据源与快照",
            "/settings": "模型配置",
        },
        "/settings": {
            "/data-sources": "数据源与快照",
            "/settings": "模型配置",
        },
    }

    for page, expected_links in expected.items():
        response = client.get(page)
        assert response.status_code == 200
        navigation = re.search(
            r'<nav class="secondary-nav"[\s\S]*?</nav>', response.text
        )
        assert navigation is not None, page
        rendered = navigation.group(0)
        hrefs = set(re.findall(r'href="([^"]+)"', rendered))
        assert hrefs == set(expected_links), page
        for href, label in expected_links.items():
            assert f'href="{href}"' in rendered
            assert label in rendered
        assert hrefs.isdisjoint(webui_app.RESEARCH_OS_GET_REDIRECTS)


def test_active_webui_boundary_cannot_enable_sqlite_via_environment(
    monkeypatch,
) -> None:
    calls: list[dict] = []

    def fake_load(**kwargs):
        calls.append(kwargs)
        return _ui_projection()

    monkeypatch.setenv("FACTOR_LAB_DATABASE_URL", "sqlite:///legacy-factor-lab.db")
    monkeypatch.setenv("FACTOR_LAB_WEBUI_ALLOW_SQLITE_READ_MODEL", "1")
    monkeypatch.setattr(webui_app, "_load_research_os_read_model", fake_load)
    webui_app._RESEARCH_OS_READ_MODEL_CACHE.update(
        {"key": None, "created_at": 0.0, "payload": None}
    )

    projection = webui_app.get_research_os_read_model(max_age_seconds=0)

    assert projection["source"] == "postgresql"
    assert calls == [{"allow_sqlite": False}]
