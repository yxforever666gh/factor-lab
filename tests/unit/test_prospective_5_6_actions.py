from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

import pytest

import factor_lab.data.prospective as prospective_data
import factor_lab.data.prospective_execution as prospective_execution_data
import factor_lab.implementation_closure as implementation_closure
import factor_lab.prospective_ledger as prospective_ledger
from factor_lab.cli import main
from factor_lab.data.prospective_readiness import inspect_prospective_readiness
from tests.unit import test_prospective_ledger as ledger_cases
from tests.unit.test_prospective_readiness import _write_case


@pytest.fixture(autouse=True)
def _isolate_ledger_harness(monkeypatch: pytest.MonkeyPatch) -> None:
    """Apply the existing ledger suite's hermetic released-runtime boundary."""

    monkeypatch.setattr(
        prospective_data,
        "load_prospective_input_snapshot",
        prospective_data._load_prospective_input_snapshot_files,
    )
    monkeypatch.setattr(
        prospective_execution_data,
        "load_prospective_execution_snapshot",
        ledger_cases._load_test_execution_snapshot,
    )
    monkeypatch.setattr(
        implementation_closure,
        "verify_implementation_closure",
        lambda *_args, **_kwargs: {"schema_version": 1},
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_materialize_upgrade_runtime_capsule",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_verify_upgrade_runtime_closure",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_run_active_release_operation",
        ledger_cases._run_test_release_operation_wrapped,
    )


def test_raw_observer_routes_missing_daily_to_market_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_case(tmp_path, monkeypatch, missing_dataset="daily")
    (tmp_path / "runtime/data/raw/enrichment-checkpoint.json").unlink()

    report = inspect_prospective_readiness(
        tmp_path,
        observed_at_utc="2026-08-31T08:00:00Z",
    )

    assert report["status"] == "ready"
    assert report["reason"] == "market_data_sync_ready"
    assert report["next_action"] == "sync_market_data"
    assert report["action"] == {
        "command": "data sync",
        "arguments": {
            "start_date": "2026-08-31",
            "end_date": "2026-08-31",
            "calendar_end_date": "2026-09-30",
            "datasets": ["daily", "daily_basic", "adj_factor"],
            "resume": True,
        },
        "argv": [
            "data",
            "sync",
            "--from",
            "2026-08-31",
            "--to",
            "2026-08-31",
            "--calendar-to",
            "2026-09-30",
            "--dataset",
            "daily",
            "--dataset",
            "daily_basic",
            "--dataset",
            "adj_factor",
            "--resume",
        ],
    }


def test_raw_observer_routes_missing_reference_to_exact_date_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_case(tmp_path, monkeypatch)
    (tmp_path / "runtime/data/raw/enrichment-checkpoint.json").unlink()

    report = inspect_prospective_readiness(
        tmp_path,
        observed_at_utc="2026-08-31T08:00:00Z",
    )

    assert report["status"] == "ready"
    assert report["reason"] == "reference_sync_ready"
    assert report["next_action"] == "sync_reference"
    assert report["reference"]["status"] == "missing"
    assert report["action"] == {
        "command": "data reference",
        "arguments": {"trade_date": "2026-08-31"},
        "argv": [
            "data",
            "reference",
            "--trade-date",
            "2026-08-31",
        ],
    }


def _install_awaiting_receipt_view(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    observed_at_utc: str,
) -> tuple[Path, dict[str, Any], str, dict[str, Any]]:
    ledger = tmp_path / "runtime/prospective/5.0"
    ledger_cases._ready(ledger)
    case = ledger_cases._plan_case(ledger)
    decision = ledger_cases._seal_case(
        ledger,
        case,
        recorded_at="2026-08-24T12:01:00Z",
    )
    expected_ledger = ledger_cases._authoritative_readiness_ledger_view(ledger)
    assert expected_ledger["phase"] == "awaiting_receipt"
    deadline = str(case["plan"]["admission_deadline_utc"])
    observer_report: dict[str, Any] = {
        "observed_at_utc": observed_at_utc,
        "clock_source": "caller_supplied",
        "stable_view": True,
        "status": "waiting",
        "reason": "ledger_not_ready",
        "ready": False,
        "next_action": "wait",
        "action": None,
        "ready_for": {
            "membership_build": False,
            "input_build": False,
            "decision_admission": False,
        },
        "ledger": expected_ledger,
        "candidate": {"due_offset": case["result"].due_offset},
        "issues": [],
    }

    def observer(project_root: Path, **kwargs: Any) -> dict[str, Any]:
        assert Path(project_root) == tmp_path
        assert Path(kwargs["ledger_root"]) == ledger
        assert kwargs["observed_at_utc"] == observed_at_utc
        return deepcopy(observer_report)

    monkeypatch.setattr(
        "factor_lab.data.prospective_readiness.inspect_prospective_readiness",
        observer,
    )
    return ledger, decision, deadline, expected_ledger


def test_authoritative_readiness_exposes_pending_decision_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = "2026-08-25T01:14:59Z"
    ledger, decision, deadline, expected_ledger = _install_awaiting_receipt_view(
        tmp_path,
        monkeypatch,
        observed_at_utc=observed,
    )

    report = prospective_ledger.prospective_readiness(
        ledger,
        project_root=tmp_path,
        observed_at_utc=observed,
    )

    decision_sha = str(decision["record_sha256"])
    snapshot = str(decision["snapshot"]["path"])
    snapshot_sha = str(expected_ledger["snapshot_sha256"])
    release_tag = str(decision["snapshot"]["snapshot"]["release_tag"])
    assert release_tag == "5.0"
    pending = {
        "snapshot": snapshot,
        "snapshot_sha256": snapshot_sha,
        "decision_record_sha256": decision_sha,
        "admission_deadline_utc": deadline,
        "purpose": "decision_anchor",
        "release_tag": release_tag,
    }
    assert report["status"] == "ready"
    assert report["reason"] == "decision_attestation_ready"
    assert report["ready"] is True
    assert report["next_action"] == "attest_decision"
    assert report["pending_attestation"] == pending
    assert report["action"] == {
        "command": "prospective attest",
        "arguments": pending,
        "argv": [
            "prospective",
            "attest",
            "--snapshot",
            snapshot,
            "--purpose",
            "decision_anchor",
            "--release-tag",
            release_tag,
            "--decision-record-sha256",
            decision_sha,
            "--admission-deadline-utc",
            deadline,
        ],
    }


def test_authoritative_readiness_treats_equal_attestation_deadline_as_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deadline = "2026-08-25T01:15:00Z"
    ledger, _decision, observed_deadline, _expected_ledger = _install_awaiting_receipt_view(
        tmp_path,
        monkeypatch,
        observed_at_utc=deadline,
    )
    assert observed_deadline == deadline

    report = prospective_ledger.prospective_readiness(
        ledger,
        project_root=tmp_path,
        observed_at_utc=deadline,
    )

    assert report["status"] == "terminal"
    assert report["reason"] == "decision_attestation_deadline_missed"
    assert report["ready"] is False
    assert report["next_action"] == "none"
    assert report["action"] is None
    assert report["issues"] == [
        {
            "code": "DECISION_ATTESTATION_DEADLINE_MISSED",
            "severity": "fatal",
            "component": "attestation",
            "retryable": False,
            "message": (
                "the sealed decision was not attested before its immutable deadline"
            ),
            "details": {},
        }
    ]


def test_cli_admit_forwards_snapshot_plan_store_and_seal_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot_sha = "1" * 64
    snapshot = tmp_path / "runtime/prospective/5.0/inputs" / snapshot_sha
    snapshot.mkdir(parents=True)
    (snapshot / "manifest.json").write_text("{}", encoding="utf-8")
    ledger = tmp_path / "runtime/prospective/5.0"
    plan_path = tmp_path / "plans" / f"{'2' * 64}.json"
    plan = {
        "schema_version": 2,
        "source_data_snapshot_sha256": snapshot_sha,
        "admission_deadline_utc": "2026-09-01T01:15:00Z",
    }
    stored = {
        "path": str(plan_path),
        "plan_sha256": "2" * 64,
        "created": True,
    }
    decision = {
        "record_sha256": "3" * 64,
        "snapshot": {
            "path": str(tmp_path / "snapshots" / "decision.json"),
            "snapshot": {"release_tag": "5.6"},
        },
    }
    calls: list[tuple[str, Any, Any]] = []

    def build(ledger_root: Path, **kwargs: Any) -> dict[str, Any]:
        calls.append(("build", Path(ledger_root), dict(kwargs)))
        return plan

    def store(ledger_root: Path, value: dict[str, Any]) -> dict[str, Any]:
        calls.append(("store", Path(ledger_root), value))
        return stored

    def seal(ledger_root: Path, requested_plan: Path) -> dict[str, Any]:
        calls.append(("seal", Path(ledger_root), Path(requested_plan)))
        return decision

    monkeypatch.setattr("factor_lab.cli.build_decision_plan", build)
    monkeypatch.setattr("factor_lab.cli.store_decision_plan", store)
    monkeypatch.setattr("factor_lab.cli.seal_decision", seal)

    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "admit",
            "--input",
            str(snapshot / "manifest.json"),
        ]
    ) == 0

    output = json.loads(capsys.readouterr().out)
    assert calls == [
        (
            "build",
            ledger,
            {"source_data_snapshot_sha256": snapshot_sha},
        ),
        ("store", ledger, plan),
        ("seal", ledger, plan_path.resolve()),
    ]
    assert output["status"] == "sealed_pending_attestation"
    assert output["plan"] == plan
    assert output["stored"] == stored
    assert output["decision"] == decision
    assert output["attestation"] == {
        "snapshot": decision["snapshot"]["path"],
        "purpose": "decision_anchor",
        "release_tag": "5.6",
        "decision_record_sha256": "3" * 64,
        "admission_deadline_utc": plan["admission_deadline_utc"],
    }
