from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from factor_lab.cli import _published_tag_oids, build_parser, main


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
