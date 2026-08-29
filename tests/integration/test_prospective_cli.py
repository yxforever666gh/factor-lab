from __future__ import annotations

import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest

import factor_lab
from factor_lab.cli import (
    _activation_payload,
    _published_tag_oids,
    build_parser,
    main,
)


RUN_ID = "c" * 16
AUTHORITATIVE_RUN = {
    "authoritative_run_id": RUN_ID,
    "run_fingerprint": RUN_ID + "d" * 48,
    "manifest_sha256": "e" * 64,
    "manifest_self_sha256": "f" * 64,
    "adaptive_summary_sha256": "1" * 64,
    "frozen_route": "fixed_core_full",
    "integrity_valid": True,
}
ROOT = Path(__file__).resolve().parents[2]


def _install_real_suspension_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client: object,
) -> None:
    top500_root = tmp_path / "runtime/data/top500"

    def ensure_directories() -> None:
        top500_root.mkdir(parents=True, exist_ok=True)

    layout = SimpleNamespace(
        repo_root=tmp_path,
        top500_root=top500_root,
        ensure_directories=ensure_directories,
    )
    config_path = tmp_path / "configs/data.json"
    monkeypatch.setattr(
        "factor_lab.cli._data_layout",
        lambda _root: ({}, layout, config_path),
    )
    monkeypatch.setattr(
        "factor_lab.data.suspensions.load_data_config",
        lambda _path: {"sync": {"request_rate_per_minute": 0}},
    )
    monkeypatch.setattr(
        "factor_lab.data.suspensions._configured_tushare_client",
        lambda _config, _layout: client,
    )


def _suspensions_argv(tmp_path: Path) -> list[str]:
    return [
        "--root",
        str(tmp_path),
        "data",
        "suspensions",
        "--from",
        "2017-01-01",
        "--to",
        "2026-08-31",
        "--no-resume",
    ]


def test_data_sync_routes_calendar_extension_only_to_market_sync(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    calls: list[tuple[str, tuple, dict]] = []
    fake_layout = SimpleNamespace()
    monkeypatch.setattr(
        "factor_lab.cli._data_layout",
        lambda _root: ({}, fake_layout, tmp_path / "configs/data.json"),
    )

    def fake_sync(*args, **kwargs):
        calls.append(("sync", args, kwargs))
        return {"status": "complete"}

    def fake_suspensions(*args, **kwargs):
        calls.append(("suspensions", args, kwargs))
        return {"status": "complete"}

    monkeypatch.setattr("factor_lab.cli.sync_data", fake_sync)
    monkeypatch.setattr("factor_lab.cli.sync_suspensions", fake_suspensions)
    assert main(
        [
            "--root",
            str(tmp_path),
            "data",
            "sync",
            "--from",
            "2026-08-24",
            "--to",
            "2026-08-28",
            "--calendar-to",
            "2026-09-15",
        ]
    ) == 0
    capsys.readouterr()
    assert calls[0][2]["calendar_end_date"] == "2026-09-15"

    assert main(
        [
            "--root",
            str(tmp_path),
            "data",
            "suspensions",
            "--from",
            "2026-08-24",
            "--to",
            "2026-08-28",
        ]
    ) == 0
    capsys.readouterr()
    assert "calendar_end_date" not in calls[1][2]


@pytest.mark.parametrize(
    ("status", "expected_exit"),
    (("complete", 0), ("partial", 0), ("waiting", 2), ("blocked", 3)),
)
def test_data_sync_preserves_the_retryable_exit_contract(
    tmp_path: Path,
    monkeypatch,
    capsys,
    status: str,
    expected_exit: int,
) -> None:
    fake_layout = SimpleNamespace()
    monkeypatch.setattr(
        "factor_lab.cli._data_layout",
        lambda _root: ({}, fake_layout, tmp_path / "configs/data.json"),
    )
    monkeypatch.setattr(
        "factor_lab.cli.sync_data",
        lambda *_args, **_kwargs: {
            "schema_version": 1,
            "status": status,
        },
    )

    exit_code = main(
        [
            "--root",
            str(tmp_path),
            "data",
            "sync",
            "--from",
            "2026-08-31",
            "--to",
            "2026-08-31",
        ]
    )

    assert exit_code == expected_exit
    assert json.loads(capsys.readouterr().out)["status"] == status


def test_data_suspensions_transport_failure_is_retryable_exit_2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    class TimeoutClient:
        def query(self, _endpoint: str, **_kwargs: object) -> pd.DataFrame:
            raise TimeoutError("provider timed out")

    _install_real_suspension_cli(tmp_path, monkeypatch, TimeoutClient())

    assert main(_suspensions_argv(tmp_path)) == 2
    assert json.loads(capsys.readouterr().out)["reason"] == (
        "provider_temporarily_unavailable"
    )


def test_data_suspensions_configuration_failure_is_blocked_exit_3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    _install_real_suspension_cli(tmp_path, monkeypatch, object())

    def missing_token(_config, _layout):
        raise RuntimeError("missing Tushare token")

    monkeypatch.setattr(
        "factor_lab.data.suspensions._configured_tushare_client",
        missing_token,
    )

    assert main(_suspensions_argv(tmp_path)) == 3
    assert json.loads(capsys.readouterr().out)["reason"] == (
        "suspension_evidence_invalid"
    )


def test_data_suspensions_malformed_payload_is_blocked_exit_3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    class MalformedClient:
        def query(self, _endpoint: str, **_kwargs: object) -> pd.DataFrame:
            return pd.DataFrame([{"ts_code": "000001.SZ"}])

    _install_real_suspension_cli(tmp_path, monkeypatch, MalformedClient())

    assert main(_suspensions_argv(tmp_path)) == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["reason"] == "suspension_evidence_invalid"
    assert "missing columns" in payload["error"]


def test_data_suspensions_repeated_page_is_blocked_exit_3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    class RepeatingClient:
        def query(self, _endpoint: str, **_kwargs: object) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20170103",
                        "suspend_type": "S",
                        "suspend_timing": "09:30",
                    }
                ]
            )

    _install_real_suspension_cli(tmp_path, monkeypatch, RepeatingClient())
    monkeypatch.setattr("factor_lab.data.suspensions.SUSPENSION_PAGE_SIZE", 1)

    assert main(_suspensions_argv(tmp_path)) == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["reason"] == "suspension_evidence_invalid"
    assert "repeated a page" in payload["error"]


@pytest.mark.parametrize(
    ("status", "expected_exit"),
    (("complete", 0), ("waiting", 2), ("blocked", 3)),
)
def test_data_reference_preserves_the_capture_exit_contract(
    tmp_path: Path,
    monkeypatch,
    capsys,
    status: str,
    expected_exit: int,
) -> None:
    fake_layout = SimpleNamespace()
    config_path = tmp_path / "configs/data.json"
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        "factor_lab.cli._data_layout",
        lambda _root: ({}, fake_layout, config_path),
    )

    def reference(trade_date, **kwargs):
        calls.append((trade_date, dict(kwargs)))
        return {
            "schema_version": 1,
            "status": status,
            "trade_date": trade_date,
        }

    monkeypatch.setattr("factor_lab.cli.sync_exact_reference", reference)

    exit_code = main(
        [
            "--root",
            str(tmp_path),
            "data",
            "reference",
            "--trade-date",
            "2026-08-31",
        ]
    )

    assert exit_code == expected_exit
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": 1,
        "status": status,
        "trade_date": "2026-08-31",
    }
    assert calls == [
        (
            "2026-08-31",
            {"config_path": config_path, "layout": fake_layout},
        )
    ]


def test_data_reference_value_error_fails_closed(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    fake_layout = SimpleNamespace()
    config_path = tmp_path / "configs/data.json"
    monkeypatch.setattr(
        "factor_lab.cli._data_layout",
        lambda _root: ({}, fake_layout, config_path),
    )

    def invalid_reference(_trade_date, **_kwargs):
        raise ValueError("exact reference samples disagree")

    monkeypatch.setattr(
        "factor_lab.cli.sync_exact_reference",
        invalid_reference,
    )

    exit_code = main(
        [
            "--root",
            str(tmp_path),
            "data",
            "reference",
            "--trade-date",
            "2026-08-31",
        ]
    )

    assert exit_code == 3
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": 1,
        "status": "blocked",
        "reason": "reference_evidence_invalid",
        "error": "exact reference samples disagree",
    }


def test_parser_exposes_prospective_lifecycle_commands() -> None:
    parser = build_parser()
    arguments = {
        "activate": ["--run", RUN_ID],
        "upgrade": [],
        "abandon-upgrade": ["--upgrade", "1" * 64, "--reason", "canary failed"],
        "membership": ["--month", "2026-09"],
        "input": ["--signal-date", "2026-08-24"],
        "plan": ["--input", "snapshot"],
        "admit": ["--input", "snapshot"],
        "seal": ["--plan", "plan.json"],
        "execution": ["--decision", "1" * 64],
        "outcome": ["--decision", "1" * 64, "--execution", "2" * 64],
        "correct": ["--input", "correction.json"],
        "attest": ["--purpose", "activation_canary"],
        "audit": [],
        "repair-snapshots": [],
        "evaluate": [],
        "status": [],
        "readiness": [],
    }

    for command, values in arguments.items():
        parsed = parser.parse_args(["prospective", command, *values])
        assert parsed.command == "prospective"
        assert parsed.prospective_command == command

    upgrade = parser.parse_args(["prospective", "upgrade"])
    assert upgrade.release_tag == "5.9"

    admit = parser.parse_args(
        ["prospective", "admit", "--input", "snapshot"]
    )
    assert admit.input == Path("snapshot")

    attest = parser.parse_args(
        ["prospective", "attest", "--purpose", "activation_canary"]
    )
    assert attest.snapshot == "latest"
    assert attest.release_tag == "5.0"
    assert attest.repository == "yxforever666gh/factor-lab"
    assert attest.workflow_run_id is None
    with pytest.raises(SystemExit):
        parser.parse_args(["prospective", "activate"])


def test_repair_snapshots_command_forwards_the_canonical_ledger_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[Path] = []

    def repair(ledger_root: Path) -> dict[str, Any]:
        calls.append(Path(ledger_root))
        return {"schema_version": 1, "repaired_count": 1}

    monkeypatch.setattr("factor_lab.cli.repair_snapshots", repair)

    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "repair-snapshots",
        ]
    ) == 0
    assert calls == [tmp_path / "runtime/prospective/5.0"]
    assert json.loads(capsys.readouterr().out)["repaired_count"] == 1


def test_current_release_metadata_and_upgrade_default_are_consistent() -> None:
    declared = next(
        line.split("=", 1)[1].strip().strip('"')
        for line in (ROOT / "pyproject.toml").read_text(encoding="utf-8").splitlines()
        if line.startswith("version = ")
    )
    manifest = json.loads(
        (ROOT / "protocols/5.2-target-generator.json").read_text(encoding="utf-8")
    )
    release = str(manifest["implementation_release"])
    parsed = build_parser().parse_args(["prospective", "upgrade"])

    assert declared == factor_lab.__version__ == f"{release}.0"
    assert parsed.release_tag == release


@pytest.mark.parametrize(
    ("status", "expected_exit"),
    (("ready", 0), ("waiting", 2), ("blocked", 3), ("terminal", 4)),
)
def test_readiness_command_preserves_the_observer_exit_contract(
    tmp_path: Path,
    monkeypatch,
    capsys,
    status: str,
    expected_exit: int,
) -> None:
    calls: list[dict[str, object]] = []

    def readiness(ledger_root, **kwargs):
        calls.append({"ledger_root": Path(ledger_root), **kwargs})
        return {"status": status, "ready": status == "ready"}

    monkeypatch.setattr("factor_lab.cli.prospective_readiness", readiness)
    exit_code = main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "readiness",
            "--observed-at-utc",
            "2026-08-31T06:59:00Z",
        ]
    )

    assert exit_code == expected_exit
    assert json.loads(capsys.readouterr().out)["status"] == status
    assert calls == [
        {
            "ledger_root": tmp_path / "runtime/prospective/5.0",
            "project_root": tmp_path,
            "observed_at_utc": "2026-08-31T06:59:00Z",
        }
    ]


def test_status_is_read_only_and_plan_uses_only_verified_snapshot(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    protocol = Path(__file__).resolve().parents[2] / "protocols" / "5.0.json"
    monkeypatch.setattr(
        "factor_lab.cli._published_tag_oids",
        lambda _root, _tag: ("a" * 40, "b" * 40),
    )
    monkeypatch.setattr(
        "factor_lab.cli.verify_authoritative_run",
        lambda *_args, **_kwargs: dict(AUTHORITATIVE_RUN),
    )
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "activate",
            "--run",
            RUN_ID,
            "--protocol",
            str(protocol),
            "--release-tag",
            "5.0",
        ]
    ) == 0
    activation = json.loads(capsys.readouterr().out)
    assert activation["created"] is True

    snapshot_path = tmp_path / "runtime/prospective/5.0/inputs" / ("1" * 64)
    snapshot_path.mkdir(parents=True)
    calls: list[dict] = []

    def fake_build(ledger_root, **kwargs):
        calls.append({"ledger_root": Path(ledger_root), **kwargs})
        return {
            "schema_version": 2,
            "decision_session": "2026-08-25",
            "source_data_snapshot_sha256": kwargs[
                "source_data_snapshot_sha256"
            ],
        }

    monkeypatch.setattr("factor_lab.cli.build_decision_plan", fake_build)
    monkeypatch.setattr(
        "factor_lab.cli.store_decision_plan",
        lambda _root, _plan: {"plan_sha256": "2" * 64, "created": True},
    )
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "plan",
            "--input",
            str(snapshot_path),
        ]
    ) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["plan"]["decision_session"] == "2026-08-25"
    assert planned["plan"]["source_data_snapshot_sha256"] == "1" * 64
    assert calls == [
        {
            "ledger_root": tmp_path / "runtime/prospective/5.0",
            "source_data_snapshot_sha256": "1" * 64,
        }
    ]

    assert main(["--root", str(tmp_path), "prospective", "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["record_count"] == 1
    assert status["status"] == "awaiting_new_data"


def test_input_command_rejects_backfill_and_builds_future_snapshot(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    with pytest.raises(ValueError, match="after the frozen 2026-08-21 cutoff"):
        main(
            [
                "--root",
                str(tmp_path),
                "prospective",
                "input",
                "--signal-date",
                "2026-08-21",
            ]
        )

    snapshot_path = tmp_path / "runtime/prospective/5.0/inputs" / ("3" * 64)
    built = {
        "signal_date": "2026-08-24",
        "trade_date": "2026-08-25",
        "source_data_snapshot_sha256": "3" * 64,
        "directory": str(snapshot_path),
        "manifest_path": str(snapshot_path / "manifest.json"),
        "rows_path": str(snapshot_path / "rows.json"),
        "build_receipt_path": str(snapshot_path / "build-receipt.json"),
        "inputs_available_at_utc": "2026-08-24T07:01:00Z",
        "build_completed_at_utc": "2026-08-24T07:02:00Z",
    }
    calls: list[tuple] = []

    def fake_build(root, *, signal_date, **kwargs):
        calls.append((Path(root), signal_date, kwargs))
        return built

    monkeypatch.setattr(
        "factor_lab.cli.build_signal_input_evidence", fake_build
    )
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "input",
            "--signal-date",
            "2026-08-24",
            "--available-at-utc",
            "2026-08-24T07:05:00Z",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["source_data_snapshot_sha256"] == "3" * 64
    assert calls == [
        (
            tmp_path / "runtime/prospective/5.0",
            "2026-08-24",
            {
                "available_at_utc": "2026-08-24T07:05:00Z",
                "membership_snapshot_path": None,
            },
        )
    ]


def test_execution_and_outcome_commands_accept_only_replayable_identifiers(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    decision_sha = "4" * 64
    execution_sha = "5" * 64
    calls: list[tuple[str, tuple, dict]] = []

    def fake_execution(*args, **kwargs):
        calls.append(("execution", args, kwargs))
        return {"execution_snapshot_sha256": execution_sha}

    def fake_outcome(*args, **kwargs):
        calls.append(("build_outcome", args, kwargs))
        return {
            "schema_version": 2,
            "decision_record_sha256": decision_sha,
            "execution_snapshot_sha256": execution_sha,
        }

    def fake_append(*args, **kwargs):
        calls.append(("append_outcome", args, kwargs))
        return {"record_sha256": "6" * 64}

    monkeypatch.setattr("factor_lab.cli.build_execution_evidence", fake_execution)
    monkeypatch.setattr("factor_lab.cli.build_outcome_payload", fake_outcome)
    monkeypatch.setattr("factor_lab.cli.append_outcome", fake_append)

    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "execution",
            "--decision",
            decision_sha,
            "--available-at-utc",
            "2026-09-08T12:05:00Z",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["execution_snapshot_sha256"] == execution_sha

    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "outcome",
            "--decision",
            decision_sha,
            "--execution",
            execution_sha,
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["record_sha256"] == "6" * 64
    ledger_root = tmp_path / "runtime/prospective/5.0"
    assert calls[0] == (
        "execution",
        (ledger_root,),
        {
            "decision_record_sha256": decision_sha,
            "available_at_utc": "2026-09-08T12:05:00Z",
        },
    )
    assert calls[1] == (
        "build_outcome",
        (ledger_root,),
        {
            "decision_record_sha256": decision_sha,
            "execution_snapshot_sha256": execution_sha,
        },
    )
    assert calls[2][0] == "append_outcome"

    with pytest.raises(SystemExit):
        main(
            [
                "--root",
                str(tmp_path),
                "prospective",
                "outcome",
                "--decision",
                decision_sha,
                "--execution",
                execution_sha,
                "--status",
                "not_executed",
            ]
        )

    with pytest.raises(ValueError, match="lowercase SHA-256"):
        main(
            [
                "--root",
                str(tmp_path),
                "prospective",
                "outcome",
                "--decision",
                decision_sha,
                "--execution",
                "manual.json",
            ]
        )


def test_membership_and_evaluate_use_only_the_verified_ledger_boundary(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    ledger_root = tmp_path / "runtime/prospective/5.0"
    calls: list[tuple] = []

    def fake_membership(root, **kwargs):
        calls.append(("membership", Path(root), kwargs))
        return {"membership_month": kwargs["membership_month"], "artifact_sha256": "7" * 64}

    def fake_evaluate(root):
        calls.append(("evaluate", Path(root)))
        return {"status": "accumulating", "evaluation_sha256": "8" * 64}

    monkeypatch.setattr("factor_lab.cli.build_membership_evidence", fake_membership)
    monkeypatch.setattr("factor_lab.cli.checkpoint_evaluation", fake_evaluate)
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "membership",
            "--month",
            "2026-09",
            "--available-at-utc",
            "2026-08-31T10:00:00Z",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["artifact_sha256"] == "7" * 64
    assert main(["--root", str(tmp_path), "prospective", "evaluate"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "accumulating"
    assert calls == [
        (
            "membership",
            ledger_root,
            {
                "membership_month": "2026-09",
                "available_at_utc": "2026-08-31T10:00:00Z",
            },
        ),
        ("evaluate", ledger_root),
    ]


def test_upgrade_command_uses_published_manifest_binding(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    manifest = tmp_path / "protocols/5.2-target-generator.json"
    expected = {
        "schema_version": 1,
        "implementation_release_tag": "5.2",
    }
    calls: list[tuple] = []

    def fake_payload(root, ledger_root, requested, release_tag):
        calls.append(
            (
                "payload",
                Path(root),
                Path(ledger_root),
                Path(requested),
                release_tag,
            )
        )
        return dict(expected)

    def fake_append(ledger_root, payload):
        calls.append(("append", Path(ledger_root), payload))
        return {"record_sha256": "4" * 64}

    monkeypatch.setattr(
        "factor_lab.cli._implementation_upgrade_payload", fake_payload
    )
    monkeypatch.setattr(
        "factor_lab.cli.append_implementation_upgrade", fake_append
    )
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "upgrade",
            "--manifest",
            str(manifest),
            "--release-tag",
            "5.2",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["record_sha256"] == "4" * 64
    assert calls == [
        (
            "payload",
            tmp_path,
            tmp_path / "runtime/prospective/5.0",
            manifest,
            "5.2",
        ),
        ("append", tmp_path / "runtime/prospective/5.0", expected),
    ]

    def fake_abandon(ledger_root, **kwargs):
        calls.append(("abandon", Path(ledger_root), kwargs))
        return {"record_sha256": "5" * 64}

    monkeypatch.setattr(
        "factor_lab.cli.abandon_implementation_upgrade", fake_abandon
    )
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "abandon-upgrade",
            "--upgrade",
            "6" * 64,
            "--reason",
            "canary failed",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["record_sha256"] == "5" * 64
    assert calls[-1] == (
        "abandon",
        tmp_path / "runtime/prospective/5.0",
        {
            "implementation_upgrade_record_sha256": "6" * 64,
            "reason": "canary failed",
        },
    )


def test_upgrade_payload_reads_the_decision_free_transition_view(
    tmp_path: Path,
    monkeypatch,
) -> None:
    record_path = tmp_path / "activation.json"
    record_path.write_bytes(b"observed")
    state = {
        "activation_record_sha256": "1" * 64,
        "latest_implementation_upgrade_record_sha256": "2" * 64,
    }
    calls: list[Path] = []

    def transition(root):
        calls.append(Path(root))
        return {
            "valid": True,
            "records": [{"path": str(record_path)}],
            "state": dict(state),
        }

    monkeypatch.setattr(
        "factor_lab.cli.implementation_transition_status",
        transition,
    )
    monkeypatch.setattr(
        "factor_lab.cli.audit_ledger",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("ordinary audit is not a cross-runtime transition view")
        ),
    )
    activation = {
        "protocol_release": "5.0",
        "protocol_id": "factor-lab/5.0/adaptive-core-overlay",
        "protocol_sha256": "3" * 64,
        "frozen_route": "fixed_core_full",
    }
    monkeypatch.setattr(
        "factor_lab.cli.strict_load_canonical",
        lambda _raw: {
            "kind": "protocol_activation",
            "payload": dict(activation),
        },
    )

    observed_activation, observed_state = _activation_payload(tmp_path)

    assert observed_activation == activation
    assert observed_state == state
    assert calls == [tmp_path]


def test_attest_resolves_latest_and_forwards_resume_contract(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    protocol = Path(__file__).resolve().parents[2] / "protocols" / "5.0.json"
    monkeypatch.setattr(
        "factor_lab.cli._published_tag_oids",
        lambda _root, _tag: ("a" * 40, "b" * 40),
    )
    monkeypatch.setattr(
        "factor_lab.cli.verify_authoritative_run",
        lambda *_args, **_kwargs: dict(AUTHORITATIVE_RUN),
    )
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "activate",
            "--run",
            RUN_ID,
            "--protocol",
            str(protocol),
        ]
    ) == 0
    activation = json.loads(capsys.readouterr().out)
    calls: list[tuple[Path, Path, dict]] = []

    def fake_attest(ledger_root, snapshot, **kwargs):
        calls.append((Path(ledger_root), Path(snapshot), kwargs))
        return {
            "status": "verified",
            "workflow_run_id": kwargs.get("workflow_run_id"),
        }

    monkeypatch.setattr("factor_lab.cli.attest_snapshot", fake_attest)

    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "attest",
            "--purpose",
            "activation_canary",
        ]
    ) == 0
    canary_output = json.loads(capsys.readouterr().out)
    assert canary_output == {"status": "verified", "workflow_run_id": None}
    canary = calls[-1]
    assert canary[1] == Path(activation["snapshot"]["path"])
    assert canary[2]["release_commit_oid"] == "b" * 40
    assert canary[2]["release_tag"] == "5.0"

    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "attest",
            "--snapshot",
            activation["snapshot"]["path"],
            "--purpose",
            "decision_anchor",
            "--decision-record-sha256",
            "c" * 64,
            "--admission-deadline-utc",
            "2026-08-24T01:15:00Z",
            "--workflow-run-id",
            "9345",
            "--repository",
            "example/factor-lab",
            "--release-tag",
            "5.0",
        ]
    ) == 0
    resume_output = json.loads(capsys.readouterr().out)
    assert resume_output["workflow_run_id"] == 9345
    resume = calls[-1]
    assert resume[2]["purpose"] == "decision_anchor"
    assert resume[2]["decision_record_sha256"] == "c" * 64
    assert resume[2]["admission_deadline_utc"] == "2026-08-24T01:15:00Z"
    assert resume[2]["workflow_run_id"] == 9345
    assert resume[2]["repository"] == "example/factor-lab"

    call_count = len(calls)
    with pytest.raises(ValueError, match="differs from the activated snapshot"):
        main(
            [
                "--root",
                str(tmp_path),
                "prospective",
                "attest",
                "--purpose",
                "activation_canary",
                "--release-tag",
                "5.1",
            ]
        )
    assert len(calls) == call_count

    monkeypatch.setattr(
        "factor_lab.cli._published_tag_oids",
        lambda _root, _tag: ("d" * 40, "b" * 40),
    )
    with pytest.raises(ValueError, match="differs from the activation binding"):
        main(
            [
                "--root",
                str(tmp_path),
                "prospective",
                "attest",
                "--purpose",
                "activation_canary",
                "--release-tag",
                "5.0",
            ]
        )
    assert len(calls) == call_count


def test_published_tag_oids_falls_back_to_github_git_database(
    tmp_path: Path,
    monkeypatch,
) -> None:
    tag_oid = "a" * 40
    commit_oid = "b" * 40
    calls: list[tuple[str, ...]] = []

    def fake_run(argv, *, cwd, check, capture_output, text):
        command = tuple(argv)
        calls.append(command)
        assert Path(cwd) == tmp_path
        assert check is True
        assert capture_output is True
        assert text is True
        if command == ("git", "rev-parse", "refs/tags/5.0"):
            stdout = f"{tag_oid}\n"
        elif command == ("git", "cat-file", "-t", tag_oid):
            stdout = "tag\n"
        elif command == ("git", "rev-parse", "refs/tags/5.0^{}"):
            stdout = f"{commit_oid}\n"
        elif command == (
            "git",
            "ls-remote",
            "origin",
            "refs/tags/5.0",
            "refs/tags/5.0^{}",
        ):
            raise subprocess.CalledProcessError(128, argv, stderr="transport down")
        elif command == ("gh", "repo", "view", "--json", "nameWithOwner"):
            stdout = json.dumps({"nameWithOwner": "example/factor-lab"})
        elif command[:3] == (
            "gh",
            "api",
            "repos/example/factor-lab/git/ref/tags/5.0",
        ):
            stdout = json.dumps(
                {
                    "ref": "refs/tags/5.0",
                    "object": {"type": "tag", "sha": tag_oid},
                }
            )
        elif command[:3] == (
            "gh",
            "api",
            f"repos/example/factor-lab/git/tags/{tag_oid}",
        ):
            stdout = json.dumps(
                {
                    "tag": "5.0",
                    "object": {"type": "commit", "sha": commit_oid},
                }
            )
        else:
            raise AssertionError(command)
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    monkeypatch.setattr("factor_lab.cli.subprocess.run", fake_run)
    assert _published_tag_oids(tmp_path, "5.0") == (tag_oid, commit_oid)
    assert all("http.version=HTTP/1.1" not in call for call in calls)


def test_published_tag_api_fallback_rejects_lightweight_remote_tag(
    tmp_path: Path,
    monkeypatch,
) -> None:
    tag_oid = "a" * 40
    commit_oid = "b" * 40

    def fake_run(argv, *, cwd, check, capture_output, text):
        command = tuple(argv)
        if command == ("git", "rev-parse", "refs/tags/5.0"):
            stdout = f"{tag_oid}\n"
        elif command == ("git", "cat-file", "-t", tag_oid):
            stdout = "tag\n"
        elif command == ("git", "rev-parse", "refs/tags/5.0^{}"):
            stdout = f"{commit_oid}\n"
        elif command[0:2] == ("git", "ls-remote"):
            raise subprocess.CalledProcessError(128, argv, stderr="transport down")
        elif command == ("gh", "repo", "view", "--json", "nameWithOwner"):
            stdout = json.dumps({"nameWithOwner": "example/factor-lab"})
        elif command[:3] == (
            "gh",
            "api",
            "repos/example/factor-lab/git/ref/tags/5.0",
        ):
            stdout = json.dumps(
                {
                    "ref": "refs/tags/5.0",
                    "object": {"type": "commit", "sha": commit_oid},
                }
            )
        else:
            raise AssertionError(command)
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    monkeypatch.setattr("factor_lab.cli.subprocess.run", fake_run)
    with pytest.raises(ValueError, match="not annotated"):
        _published_tag_oids(tmp_path, "5.0")
