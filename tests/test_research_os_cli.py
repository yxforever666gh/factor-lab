from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from factor_lab.research_os import cli as cli_module
from factor_lab.research_os.build_provenance import EpochBuildProvenance
from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.cli import build_parser, main
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    RecoveryCaseStatus,
    SnapshotTier,
)
from factor_lab.research_os.snapshots import build_immutable_snapshot_manifest


def test_catalog_uses_password_file_value_only_as_driver_connect_arg(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class FakeCatalog:
        def __init__(self, database_url: str, **kwargs: object) -> None:
            captured["database_url"] = database_url
            captured.update(kwargs)

        def initialize_schema(self) -> None:
            captured["initialized"] = True

    monkeypatch.setattr(cli_module, "ResearchCatalog", FakeCatalog)
    password_file = tmp_path / "postgres-password"
    password_file.write_text("driver-only-secret\n", encoding="utf-8")
    settings = cli_module.ResearchOSSettings.from_env(
        {
            "FACTOR_LAB_ENVIRONMENT": "production",
            "FACTOR_LAB_DATABASE_URL": (
                "postgresql+psycopg://factor_lab@127.0.0.1:5433/factor_lab"
            ),
            "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(password_file),
        }
    )

    cli_module._catalog(settings)

    assert captured == {
        "database_url": "postgresql+psycopg://factor_lab@127.0.0.1:5433/factor_lab",
        "connect_args": {"password": "driver-only-secret"},
        "initialized": True,
    }


def test_cli_exposes_exact_research_os_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["data", "sync", "--spec", "source.json"]).data_command == "sync"
    assert parser.parse_args(["snapshot", "publish", "--spec", "snapshot.json"]).snapshot_command == "publish"
    assert parser.parse_args(
        ["research", "cycle", "--experiment", "e.json", "--data", "d.parquet"]
    ).research_command == "cycle"
    assert parser.parse_args(["monitor", "tick", "--input", "tick.json"]).monitor_command == "tick"
    assert parser.parse_args(["shadow", "step", "--input", "step.json"]).shadow_command == "step"
    assert parser.parse_args(["legacy", "import"]).legacy_command == "import"
    assert parser.parse_args(["doctor"]).command == "doctor"
    assert parser.parse_args(
        ["research", "epoch", "status", "--config", "production.json"]
    ).epoch_command == "status"
    assert parser.parse_args(
        ["research", "epoch", "freeze", "--config", "production.json"]
    ).epoch_command == "freeze"
    assert parser.parse_args(
        ["data", "sync", "--from", "2026-08-01", "--to", "2026-08-21", "--resume"]
    ).date_from == "2026-08-01"
    assert parser.parse_args(
        ["snapshot", "publish", "--as-of", "2026-08-21"]
    ).as_of == "2026-08-21"
    assert parser.parse_args(
        [
            "research",
            "submit",
            "--family",
            "value_quality_v1",
            "--proposal",
            "-",
        ]
    ).research_command == "submit"
    assert parser.parse_args(
        ["research", "status", "--submission-id", "submission_1"]
    ).research_command == "status"
    assert parser.parse_args(
        ["research", "resume", "--worker-id", "worker-1"]
    ).research_command == "resume"
    assert parser.parse_args(
        ["epoch", "status", "--config", "production.json"]
    ).epoch_command == "status"
    assert parser.parse_args(["readiness", "status"]).readiness_command == "status"
    assert parser.parse_args(
        ["readiness", "audit", "--config", "production.json"]
    ).readiness_command == "audit"
    assert parser.parse_args(
        ["readiness", "attest-credential-use"]
    ).readiness_command == "attest-credential-use"
    assert parser.parse_args(
        ["canary", "run", "--as-of", "2026-08-21"]
    ).canary_command == "run"
    assert parser.parse_args(["canary", "restore"]).canary_command == "restore"
    assert parser.parse_args(["monitor", "tick"]).input is None
    assert parser.parse_args(["shadow", "step", "--date", "2026-08-21"]).input is None


def test_monitor_cli_sees_older_active_recovery_beyond_terminal_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload_path = tmp_path / "monitor.json"
    payload_path.write_text(
        json.dumps(
            {
                "record": {
                    "sleeve_id": "value_quality",
                    "state": "active",
                    "target_weight": 0.25,
                    "effective_weight": 0.25,
                },
                "observation": {
                    "as_of_date": "2026-08-22",
                    "active_ir_13w": 0.1,
                    "active_ir_26w": 0.1,
                    "ic_26w": 0.01,
                },
                "snapshot_id": "snapshot-monitor-cli",
            }
        ),
        encoding="utf-8",
    )
    active = SimpleNamespace(
        recovery_case_id="older-active-cli",
        sleeve_id="value_quality",
        status=RecoveryCaseStatus.OPEN,
    )
    cases = [
        *(
            SimpleNamespace(
                recovery_case_id=f"newer-terminal-cli-{index}",
                sleeve_id="value_quality",
                status=RecoveryCaseStatus.CLOSED,
            )
            for index in range(101)
        ),
        active,
    ]

    class FakeCatalog:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def list_lifecycle_events(self, **_kwargs):
            return []

        def append_lifecycle_event(self, event):
            return event

        def list_recovery_cases(self, *, limit, **_kwargs):
            return cases[:limit]

        def iter_recovery_cases(
            self, *, statuses, sleeve_id=None, batch_size
        ):
            assert batch_size == 1_000
            allowed = set(statuses)
            return iter(
                case
                for case in cases
                if case.status in allowed
                and (sleeve_id is None or case.sleeve_id == sleeve_id)
            )

    captured: dict[str, object] = {}

    class Monitor:
        def __init__(self, _catalog):
            pass

        def tick(self, *_args, active_recovery_case=None, **_kwargs):
            captured["active_recovery_case"] = active_recovery_case
            return {"status": "completed"}

    catalog = FakeCatalog()
    assert active not in catalog.list_recovery_cases(limit=100)
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda _args: SimpleNamespace(
            environment="local",
            uses_postgresql=False,
        ),
    )
    monkeypatch.setattr(cli_module, "_catalog", lambda _settings: catalog)
    monkeypatch.setattr(cli_module, "LifecycleMonitor", Monitor)
    monkeypatch.setattr(
        cli_module,
        "_coordinated",
        lambda _catalog, _run_type, _inputs, action: action(),
    )
    monkeypatch.setattr(cli_module, "_emit", lambda result: captured.setdefault("result", result))

    assert cli_module._monitor_tick(SimpleNamespace(input=str(payload_path))) == 0
    assert captured["active_recovery_case"] is active


@pytest.mark.parametrize(
    "argv",
    [
        ["data", "sync", "--spec", "source.json"],
        ["snapshot", "publish", "--spec", "snapshot.json"],
        ["research", "cycle", "--experiment", "e.json", "--data", "d.parquet"],
        ["monitor", "tick", "--input", "tick.json"],
        ["shadow", "step", "--input", "step.json"],
        ["legacy", "import", "--root", "artifacts"],
    ],
)
def test_production_rejects_legacy_file_fact_inputs(
    argv, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")

    assert main(argv) == 1
    assert "rejects caller-supplied" in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv",
    [
        ["data", "sync", "--spec", "source.json"],
        ["snapshot", "publish", "--spec", "snapshot.json"],
        ["research", "cycle", "--experiment", "e.json", "--data", "d.parquet"],
        ["monitor", "tick", "--input", "tick.json"],
        ["shadow", "step", "--input", "step.json"],
        ["legacy", "import", "--root", "artifacts"],
    ],
)
def test_database_production_marker_overrides_local_env_before_file_or_write(
    argv, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    database_url = "postgresql+psycopg://authority.invalid/factor_lab"
    marker_reads: list[str] = []
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "local")
    monkeypatch.setattr(
        cli_module,
        "load_runtime_authority_marker",
        lambda database: (
            marker_reads.append(database)
            or SimpleNamespace(is_production=True, environment="production")
        ),
    )

    def forbidden(*_args, **_kwargs):
        pytest.fail("file-driven production command reached a read/write seam")

    monkeypatch.setattr(cli_module, "_json", forbidden)
    monkeypatch.setattr(cli_module, "_catalog", forbidden)
    monkeypatch.setattr(cli_module, "_authoritative_services", forbidden)
    monkeypatch.setattr(cli_module, "import_legacy_evidence", forbidden)

    assert main(["--database-url", database_url, *argv]) == 1
    assert marker_reads == [database_url]
    assert "rejects caller-supplied" in capsys.readouterr().err


def test_read_only_readiness_status_honors_database_production_marker(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    database_url = "postgresql+psycopg://authority.invalid/factor_lab"
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "local")
    monkeypatch.setattr(
        cli_module,
        "load_runtime_authority_marker",
        lambda _database: SimpleNamespace(
            is_production=True, environment="production"
        ),
    )
    monkeypatch.setattr(cli_module, "_latest_verified_readiness", lambda _settings: None)

    assert main(
        ["--database-url", database_url, "readiness", "status"]
    ) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["ready"] is False
    assert payload["blockers"] == ["persisted_production_readiness_audit_missing"]


def test_credential_use_attestation_emits_only_public_persisted_evidence(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    dataset = "tushare_token_retention_" + "a" * 52
    authority = SimpleNamespace(
        rotation_capability_identity=("security", dataset),
        migrate_credential_use_evidence=lambda: SimpleNamespace(
            credential="tushare_token",
            disposition="operator_accepted_unrotated_retention",
            evidence_hash="a" * 64,
            confirmed_at=datetime(2026, 8, 25, 10, 30, tzinfo=timezone.utc),
        ),
    )
    services = SimpleNamespace(
        _production_execution_snapshot_authority=lambda: authority
    )
    closed: list[object] = []
    monkeypatch.setattr(cli_module, "_authoritative_services", lambda: services)
    monkeypatch.setattr(cli_module, "_close_services", closed.append)

    assert main(["readiness", "attest-credential-use"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "status": "completed",
        "credential": "tushare_token",
        "disposition": "operator_accepted_unrotated_retention",
        "evidence_hash": "a" * 64,
        "recorded_at": "2026-08-25T10:30:00+00:00",
        "capability": {
            "source_id": "security",
            "dataset": dataset,
        },
        "credential_material_recorded": False,
    }
    assert closed == [services]


def test_credential_use_attestation_rejects_database_override_before_services(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setattr(
        cli_module,
        "_authoritative_services",
        lambda: pytest.fail("database override reached authoritative services"),
    )

    assert main(
        [
            "--database-url",
            "postgresql+psycopg://other.invalid/factor_lab",
            "readiness",
            "attest-credential-use",
        ]
    ) == 1
    assert "rejects --database-url overrides" in capsys.readouterr().err


def test_authoritative_backfill_is_rejected_before_services_when_rotation_pending(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setenv(
        "FACTOR_LAB_ORCHESTRATION_CONFIG",
        "research_os_orchestration.production.json",
    )
    monkeypatch.setattr(
        cli_module,
        "validate_production_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            historical_backfill_allowed=False,
            provenance=SimpleNamespace(formal_epoch_eligible=False),
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "_authoritative_services",
        lambda: pytest.fail("blocked backfill must not initialize services"),
    )

    assert main(
        [
            "data",
            "sync",
            "--from",
            "2016-06-01",
            "--to",
            "2016-06-30",
            "--resume",
        ]
    ) == 1
    assert "tushare_token_post_exposure_rotation_pending" in capsys.readouterr().err


def test_authoritative_data_sync_bootstraps_calendar_and_uses_backfill_executor(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    calls: list[tuple[str, object]] = []

    class Ledger:
        @staticmethod
        def accepted_calendar_partitions():
            return ("2026-08-20", "2026-08-21")

    class Services:
        production_ledger = Ledger()
        catalog = None

        @staticmethod
        def bootstrap_accepted_calendar(**kwargs):
            calls.append(("calendar", kwargs))
            return SimpleNamespace(message="2 accepted sessions", cursor="2026-08-21")

        @staticmethod
        def execute_authoritative_backfill(request):
            calls.append(("backfill", request))
            return SimpleNamespace(
                operation=request.operation,
                status="completed",
                summary="persisted",
                outputs={"snapshot_id": request.partition_key},
            )

        @staticmethod
        def execute(_request):
            pytest.fail("historical sync must not use the formal-forward executor")

    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setenv(
        "FACTOR_LAB_ORCHESTRATION_CONFIG",
        "research_os_orchestration.production.json",
    )
    monkeypatch.setattr(
        cli_module,
        "validate_production_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            historical_backfill_allowed=True,
            provenance=SimpleNamespace(formal_epoch_eligible=False),
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "_prepare_legacy_bronze_seed",
        lambda _services, *, through: (
            calls.append(("seed", through)),
            {
                "status": "seed_absent_vendor_backfill_pending",
                "promotion_allowed": False,
            },
        )[1],
    )
    monkeypatch.setattr(cli_module, "_authoritative_services", Services)

    assert main(
        [
            "data",
            "sync",
            "--from",
            "2026-08-20",
            "--to",
            "2026-08-21",
            "--resume",
        ]
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["partition_count"] == 2
    assert payload["calendar_bootstrap"]["cursor"] == "2026-08-21"
    assert payload["legacy_bronze_seed"]["promotion_allowed"] is False
    assert [name for name, _ in calls] == [
        "calendar",
        "seed",
        "backfill",
        "backfill",
    ]


def test_production_submit_calls_only_deterministic_coordinator_bridge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    proposal = tmp_path / "proposal.json"
    proposal.write_text(
        json.dumps(
            {
                "preregistration": {"hypothesis_id": "hypothesis_1"},
                "factor": {"factor_id": "factor_1"},
            }
        ),
        encoding="utf-8",
    )
    captured = {}

    class FakeCatalog:
        closed = False

        def close(self):
            self.closed = True

    fake = SimpleNamespace(monthly_research=object(), catalog=FakeCatalog())
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setattr(cli_module, "_authoritative_services", lambda: fake)

    def submit(coordinator, payload, **kwargs):
        captured.update(
            {
                "coordinator": coordinator,
                "payload": payload,
                **kwargs,
            }
        )
        return {"accepted": True, "submission": {"status": "reviewed"}}

    monkeypatch.setattr(cli_module, "coordinator_submit", submit)

    assert main(
        [
            "research",
            "submit",
            "--family",
            "value_quality_v1",
            "--proposal",
            str(proposal),
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["accepted"] is True
    assert captured["coordinator"] is fake.monthly_research
    assert captured["family_id"] == "value_quality_v1"
    assert set(captured["payload"]) == {"preregistration", "factor"}
    assert fake.catalog.closed is True


def test_epoch_cli_does_not_accept_self_reported_hashes() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "research",
                "epoch",
                "freeze",
                "--code-hash",
                "a" * 64,
            ]
        )


def test_epoch_freeze_and_status_bind_current_measured_provenance(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    database = tmp_path / "epoch.db"
    measured = EpochBuildProvenance(
        architecture_version="research-os/application-services/v1",
        code_hash="a" * 64,
        configuration_hash="b" * 64,
        dependency_lock_hash="c" * 64,
        dirty_patch_hash="d" * 64,
        provenance_kind="immutable_source_bundle",
        build_identity_hash="e" * 64,
        image_source_digest="f" * 64,
    )
    monkeypatch.setattr(
        cli_module,
        "_measured_epoch_context",
        lambda _args, _settings: (measured, None),
    )

    assert main(
        [
            "--database-url",
            str(database),
            "research",
            "epoch",
            "freeze",
        ]
    ) == 0
    frozen = json.loads(capsys.readouterr().out)
    assert frozen["current_provenance_matches"] is True
    with ResearchCatalog(database) as catalog:
        epoch = catalog.get_evidence_epoch()
        assert epoch is not None
        assert epoch.code_hash == measured.code_hash
        assert epoch.dirty_patch_hash == measured.dirty_patch_hash

    changed = EpochBuildProvenance(
        **{**measured.__dict__, "code_hash": "0" * 64}
    )
    monkeypatch.setattr(
        cli_module,
        "_measured_epoch_context",
        lambda _args, _settings: (changed, None),
    )
    assert main(
        ["--database-url", str(database), "research", "epoch", "status"]
    ) == 2
    status = json.loads(capsys.readouterr().out)
    assert status["status"] == "provenance_mismatch"

    assert main(
        ["--database-url", str(database), "research", "epoch", "freeze"]
    ) == 0
    upgraded = json.loads(capsys.readouterr().out)
    assert upgraded["current_provenance_matches"] is True
    with ResearchCatalog(database) as catalog:
        versions = catalog.list_evidence_epochs()
        assert len(versions) == 2
        assert versions[0].code_hash == changed.code_hash
        assert versions[0].lifecycle_status == "pending"
        assert versions[1].lifecycle_status == "closed"
        assert versions[1].superseded_by_epoch_id == versions[0].epoch_id


def test_epoch_cli_calendar_horizon_revision_appends_and_moves_active_pointer(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    database = tmp_path / "calendar-revision.db"
    measured = EpochBuildProvenance(
        architecture_version="research-os/application-services/v1",
        code_hash="a" * 64,
        configuration_hash="b" * 64,
        dependency_lock_hash="c" * 64,
        dirty_patch_hash="d" * 64,
        provenance_kind="immutable_source_bundle",
        build_identity_hash="e" * 64,
        image_source_digest="f" * 64,
    )
    monkeypatch.setattr(
        cli_module,
        "_measured_epoch_context",
        lambda _args, _settings: (measured, None),
    )
    now = datetime.now(timezone.utc)
    shanghai_day = now.astimezone(timezone(timedelta(hours=8))).date()
    first_session = (pd.Timestamp(shanghai_day) + pd.offsets.BDay(1)).date()

    def calendar_snapshot(revision: str, count: int) -> DataSnapshotRef:
        root = tmp_path / revision
        root.mkdir()
        source = root / "calendar.csv"
        source.write_text(f"revision\n{revision}\n", encoding="utf-8")
        sessions = tuple(
            item.date().isoformat()
            for item in pd.bdate_range(first_session, periods=count)
        )
        calendar_hash = hashlib.sha256(
            "\n".join(sessions).encode("ascii")
        ).hexdigest()
        manifest = build_immutable_snapshot_manifest(
            (source,),
            base_dir=root,
                tier="silver",
            as_of=now,
            parent_snapshot_ids=("9" * 64,),
            environment_hashes={
                "config_hash": "1" * 64,
                "code_hash": "2" * 64,
                "dirty_patch_hash": "3" * 64,
                "dependency_lock_hash": "4" * 64,
            },
            quality_report={"status": "pass"},
            trust_labels=("point_in_time",),
            trading_calendar={
                "source": "test-exchange-authority",
                "quality_status": "accepted",
                "revision": revision,
                "sessions": sessions,
                "content_hash": calendar_hash,
            },
        )
        return manifest.to_snapshot_ref(
            uri=f"s3://factorlab/calendar/{manifest.snapshot_id}"
        )

    short = calendar_snapshot("r1", 80)
    extended = calendar_snapshot("r2", 120)
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(short)
        catalog.register_snapshot(extended)

    command = [
        "--database-url",
        str(database),
        "research",
        "epoch",
        "freeze",
        "--first-forward-session",
        first_session.isoformat(),
    ]
    assert main([*command, "--calendar-snapshot-id", short.snapshot_id]) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["lifecycle_status"] == "active"

    assert main([*command, "--calendar-snapshot-id", extended.snapshot_id]) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["epoch_id"] != first["epoch_id"]
    assert second["calendar_snapshot_id"] == extended.snapshot_id
    with ResearchCatalog(database) as catalog:
        versions = catalog.list_evidence_epochs()
        assert [item.lifecycle_status for item in versions] == ["active", "closed"]
        assert versions[1].superseded_by_epoch_id == versions[0].epoch_id
        assert catalog.get_evidence_epoch() == versions[0]


def test_doctor_command_returns_nonzero_when_required_stack_is_absent(capsys) -> None:
    code = main(["--database-url", "sqlite:///:memory:", "doctor", "--no-network"])
    payload = capsys.readouterr().out
    assert code in {0, 2}
    assert '"status"' in payload


def test_doctor_production_reports_bootstrap_validation(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setattr(
        cli_module,
        "validate_production_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            path=Path("research_os_orchestration.production.json"),
            status="formal_epoch_ready",
            formal_execution_capable=True,
            historical_backfill_allowed=True,
            formal_forward_evidence=True,
            readiness_blockers=(),
            provenance=SimpleNamespace(
                build_identity_hash="a" * 64,
                formal_epoch_eligible=True,
            ),
        ),
    )

    code = main(
        [
            "--database-url",
            "sqlite:///:memory:",
            "doctor",
            "--no-network",
            "--production",
            "--config",
            "research_os_orchestration.production.json",
            "--image",
            "factor-lab-research-os:local",
            "--no-mount-check",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code in {0, 2}
    production = next(
        item for item in payload["checks"] if item["name"] == "production_bootstrap"
    )
    assert production["status"] == "pass"
    assert next(
        item for item in payload["checks"] if item["name"] == "formal_forward_evidence"
    )["status"] == "fail"
    assert next(
        item
        for item in payload["checks"]
        if item["name"] == "persisted_production_readiness"
    )["status"] == "fail"


def test_production_epoch_cannot_freeze_without_matching_ready_audit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    database = tmp_path / "engineering-epoch.db"
    measured = EpochBuildProvenance(
        architecture_version="research-os/application-services/v1",
        code_hash="a" * 64,
        configuration_hash="b" * 64,
        dependency_lock_hash="c" * 64,
        dirty_patch_hash="d" * 64,
        provenance_kind="daemon_inspected_oci_image",
        build_identity_hash="e" * 64,
        image_source_digest="f" * 64,
        oci_image_id="sha256:" + "1" * 64,
        oci_repo_digests=("factor-lab@sha256:" + "2" * 64,),
        oci_base_digests=("python@sha256:" + "3" * 64,),
    )
    evidence = SimpleNamespace(
        status="config_valid_canary_pending",
        formal_execution_capable=False,
        historical_backfill_allowed=False,
        formal_forward_evidence=False,
        readiness_blockers=(
            "formal_execution_adapter_insufficient",
            "tushare_token_post_exposure_rotation_pending",
        ),
    )
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setattr(
        cli_module,
        "_measured_epoch_context",
        lambda _args, _settings: (measured, evidence),
    )
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()

    assert main(
        [
            "--database-url",
            str(database),
            "epoch",
            "freeze",
        ]
    ) == 1
    assert "formal production epoch freeze is blocked" in capsys.readouterr().err
    with ResearchCatalog(database) as catalog:
        assert catalog.get_evidence_epoch() is None


def test_production_readiness_status_fails_closed_without_audit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    database = tmp_path / "readiness.db"
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")

    assert main(
        ["--database-url", str(database), "readiness", "status"]
    ) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["ready"] is False
    assert payload["blockers"] == [
        "persisted_production_readiness_audit_missing"
    ]


def test_physical_canary_cli_accepts_only_a_date_and_authoritative_service(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    calls: list[object] = []

    class Services:
        catalog = None

        @staticmethod
        def run_physical_engineering_canary(*, as_of=None):
            calls.append(as_of)
            return {"run_type": "physical_engineering_canary", "formal": False}

    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setattr(cli_module, "_authoritative_services", Services)

    assert main(["canary", "run", "--as-of", "2026-08-21"]) == 0
    assert calls == [datetime(2026, 8, 21).date()]
    assert json.loads(capsys.readouterr().out)["formal"] is False


def test_minio_restore_cli_has_no_object_arguments_and_uses_authoritative_service(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    calls: list[str] = []

    class Services:
        catalog = None

        @staticmethod
        def run_physical_minio_restore_drill():
            calls.append("restore")
            return {
                "run_type": "minio_restore_drill",
                "physical": True,
            }

    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setattr(cli_module, "_authoritative_services", Services)

    assert main(["canary", "restore"]) == 0
    assert calls == ["restore"]
    assert json.loads(capsys.readouterr().out) == {
        "physical": True,
        "run_type": "minio_restore_drill",
    }

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["canary", "restore", "--uri", "s3://caller/object"]
        )


def test_doctor_production_exposes_engineering_only_blockers(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("FACTOR_LAB_ENVIRONMENT", "production")
    monkeypatch.setattr(
        cli_module,
        "validate_production_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            path=Path("research_os_orchestration.production.json"),
            status="config_valid_canary_pending",
            formal_execution_capable=False,
            historical_backfill_allowed=False,
            formal_forward_evidence=False,
            readiness_blockers=(
                "formal_execution_adapter_insufficient",
                "tushare_token_post_exposure_rotation_pending",
            ),
            provenance=SimpleNamespace(build_identity_hash="a" * 64),
        ),
    )

    assert main(
        [
            "--database-url",
            "sqlite:///:memory:",
            "doctor",
            "--no-network",
            "--production",
            "--config",
            "research_os_orchestration.production.json",
            "--no-mount-check",
        ]
    ) == 2
    payload = json.loads(capsys.readouterr().out)
    checks = {item["name"]: item for item in payload["checks"]}
    assert checks["production_bootstrap"]["status"] == "pass"
    assert checks["historical_backfill_authority"]["status"] == "fail"
    assert checks["formal_execution_capability"]["status"] == "fail"
    assert checks["formal_forward_evidence"]["status"] == "fail"
    assert "formal_execution_adapter_insufficient" in checks[
        "formal_forward_evidence"
    ]["detail"]


def test_shadow_step_command_uses_transactional_catalog_bridge(tmp_path, capsys) -> None:
    database = tmp_path / "catalog.sqlite"
    sessions = ("2026-01-02", "2026-01-05")
    calendar_hash = hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest()
    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(
            DataSnapshotRef(
                snapshot_id="gold-cli",
                tier=SnapshotTier.GOLD,
                uri="s3://factor-lab/gold/gold-cli",
                content_hash="c" * 64,
                as_of=datetime(2026, 1, 2, 7, tzinfo=timezone.utc),
                quality_status=DataQualityStatus.ACCEPTED,
                manifest={
                    "trading_calendar": {
                        "source": "test-exchange-calendar",
                        "quality_status": "accepted",
                        "sessions": list(sessions),
                        "content_hash": calendar_hash,
                    }
                },
            )
        )

    bars = tmp_path / "bars.csv"
    pd.DataFrame(
        [
            {
                    "ticker": "000001.SZ",
                    "gold_snapshot_id": "gold-cli",
                    "trade_date": "2026-01-05",
                    "execution_event_time": "2026-01-05T09:30:00+08:00",
                    "execution_available_at": "2026-01-05T09:30:00+08:00",
                    "mark_event_time": "2026-01-05T15:00:00+08:00",
                    "mark_available_at": "2026-01-05T15:01:00+08:00",
                "open_adj": 10.0,
                "close_adj": 11.0,
                "adv_20": 100_000_000.0,
                "volatility_20": 0.02,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "is_suspended": False,
                "is_delisted": False,
            }
        ]
    ).to_csv(bars, index=False)
    request = tmp_path / "shadow-step.json"
    request.write_text(
        json.dumps(
            {
                "account_id": "paper-cli",
                "decision_date": "2026-01-02",
                "trade_date": "2026-01-05",
                "expected_next_session": "2026-01-05",
                "target_weights": {"000001.SZ": 0.02},
                "market_bars": str(bars),
                "snapshot_id": "gold-cli",
                "model_version": "champion-v1",
                "initial_capital": 50_000_000,
            }
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "--database-url",
            str(database),
            "shadow",
            "step",
            "--input",
            str(request),
        ]
    )

    assert code == 0
    result = json.loads(capsys.readouterr().out)
    assert result["account_id"] == "paper-cli"
    assert result["chain_verified"] is True
    with ResearchCatalog(database) as catalog:
        assert catalog.verify_shadow_chain("paper-cli") is True
        account = catalog.get_shadow_account("paper-cli")
        assert account is not None and account.last_event_sequence > 1
