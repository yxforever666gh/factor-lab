from __future__ import annotations

import json
from pathlib import Path

import pytest

from factor_lab.cli import build_parser, main


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


def test_parser_exposes_prospective_lifecycle_commands() -> None:
    parser = build_parser()
    arguments = {
        "activate": ["--run", RUN_ID],
        "plan": ["--input", "intent.json"],
        "seal": ["--plan", "plan.json"],
        "outcome": ["--input", "outcome.json"],
        "correct": ["--input", "correction.json"],
        "attest": ["--purpose", "activation_canary"],
        "audit": [],
        "status": [],
    }

    for command, values in arguments.items():
        parsed = parser.parse_args(["prospective", command, *values])
        assert parsed.command == "prospective"
        assert parsed.prospective_command == command

    attest = parser.parse_args(
        ["prospective", "attest", "--purpose", "activation_canary"]
    )
    assert attest.snapshot == "latest"
    assert attest.release_tag == "5.0"
    assert attest.repository == "yxforever666gh/factor-lab"
    assert attest.workflow_run_id is None
    with pytest.raises(SystemExit):
        parser.parse_args(["prospective", "activate"])


def test_status_is_read_only_and_plan_does_not_append_ledger(
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

    intent_path = tmp_path / "decision-intent.json"
    intent_path.write_text(
        json.dumps(
            {
                "decision_session": "2026-08-24",
                "information_cutoff_utc": "2026-08-21T07:00:00Z",
                "input_max_available_at_utc": "2026-08-21T06:59:59Z",
                "input_snapshot_sha256": "1" * 64,
                "model_state_sha256": "2" * 64,
                "code_commit_oid": "b" * 40,
                "expected_nav_fen": 5_000_000_000,
                "targets_ppm": {"000001.SZ": 900_000},
                "cash_weight_ppm": 100_000,
                "planned_at_utc": "2026-08-21T07:00:01Z",
            }
        ),
        encoding="utf-8",
    )
    assert main(
        [
            "--root",
            str(tmp_path),
            "prospective",
            "plan",
            "--input",
            str(intent_path),
        ]
    ) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["plan"]["targets"] == [
        {"target_weight_ppm": 900_000, "ticker": "000001.SZ"}
    ]
    assert planned["plan"]["frozen_route"] == "fixed_core_full"

    assert main(["--root", str(tmp_path), "prospective", "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["record_count"] == 1
    assert status["status"] == "awaiting_new_data"


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
