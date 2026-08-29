from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

import factor_lab.cli as cli
from factor_lab.cli import build_parser


def test_cli_exposes_only_lightweight_mainline_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["data", "status"]).data_command == "status"
    research = parser.parse_args(["research", "run", "--suite", "next", "--canary"])
    assert research.research_command == "run"
    assert research.canary is True
    assert parser.parse_args(["report", "--run", "latest"]).command == "report"
    enrich = parser.parse_args(
        ["data", "enrich", "--from", "2017-01-01", "--to", "2026-08-13"]
    )
    assert enrich.data_command == "enrich"
    assert enrich.resume is True
    suspensions = parser.parse_args(
        ["data", "suspensions", "--from", "2017-01-01", "--to", "2026-08-21"]
    )
    assert suspensions.data_command == "suspensions"
    assert suspensions.resume is True
    reference = parser.parse_args(
        ["data", "reference", "--trade-date", "2026-08-31"]
    )
    assert reference.data_command == "reference"
    assert reference.trade_date == "2026-08-31"
    shadow = parser.parse_args(["adaptive-shadow", "sync"])
    assert shadow.adaptive_shadow_command == "sync"
    assert not hasattr(shadow, "observed_at_utc")


@pytest.mark.parametrize(
    "arguments",
    [
        [
            "adaptive-shadow",
            "activate",
            "--released-at-utc",
            "2026-09-01T00:00:00Z",
        ],
        ["adaptive-shadow", "activate", "--start-after", "2026-09-01"],
        [
            "adaptive-shadow",
            "activate",
            "--recorded-at-utc",
            "2026-09-01T00:00:00Z",
        ],
        [
            "adaptive-shadow",
            "plan",
            "--formal-plan",
            "formal-plan.json",
            "--formal-decision",
            "a" * 64,
            "--input",
            "input-snapshot",
            "--created-at-utc",
            "2026-09-10T12:00:00Z",
        ],
        [
            "adaptive-shadow",
            "sync",
            "--observed-at-utc",
            "2026-09-30T10:00:00Z",
        ],
    ],
)
def test_adaptive_shadow_write_clock_overrides_are_rejected_by_argparse(
    arguments: list[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        build_parser().parse_args(arguments)

    assert raised.value.code == 2


def test_adaptive_shadow_activate_uses_tag_time_and_current_clock(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    current = datetime(
        2026,
        9,
        2,
        16,
        30,
        45,
        123456,
        tzinfo=timezone.utc,
    )

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return current if tz is None else current.astimezone(tz)

    tag_calls = []
    captured = {}

    def published_tag_metadata(root: Path, tag: str):
        tag_calls.append((root, tag))
        return "a" * 40, "b" * 40, "2026-09-01T02:03:04Z"

    def activate(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"schema_version": 1, "status": "activated"}

    monkeypatch.setattr(cli, "datetime", FixedDateTime)
    monkeypatch.setattr(cli, "_published_tag_metadata", published_tag_metadata)
    monkeypatch.setattr(cli, "activate_shadow_runtime", activate)

    assert (
        cli.main(
            ["--root", str(tmp_path), "adaptive-shadow", "activate"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "activated"
    project_root = tmp_path.resolve()
    assert tag_calls == [(project_root, "5.9")]
    assert captured["args"] == (
        project_root,
        project_root / "runtime" / "adaptive-shadow" / "1",
        project_root / "runtime" / "prospective" / "5.0",
    )
    assert captured["kwargs"] == {
        "protocol_path": Path("protocols/5.9-adaptive-shadow.json"),
        "release_tag": "5.9",
        "release_tag_object_oid": "a" * 40,
        "release_commit_oid": "b" * 40,
        "start_after": "2026-09-03",
        "released_at_utc": "2026-09-01T02:03:04Z",
        "recorded_at_utc": "2026-09-02T16:30:45.123456Z",
    }


def test_adaptive_shadow_plan_delegates_clock_choice_to_runtime(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    captured = {}

    def plan(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"schema_version": 1, "status": "planned"}

    monkeypatch.setattr(cli, "plan_shadow_runtime", plan)
    formal_plan = tmp_path / "formal-plan.json"
    input_snapshot = tmp_path / "input-snapshot"
    decision_sha = "c" * 64

    assert (
        cli.main(
            [
                "--root",
                str(tmp_path),
                "adaptive-shadow",
                "plan",
                "--formal-plan",
                str(formal_plan),
                "--formal-decision",
                decision_sha,
                "--input",
                str(input_snapshot),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "planned"
    project_root = tmp_path.resolve()
    assert captured["args"] == (
        project_root,
        project_root / "runtime" / "adaptive-shadow" / "1",
        project_root / "runtime" / "prospective" / "5.0",
    )
    assert captured["kwargs"] == {
        "formal_plan_path": formal_plan.resolve(),
        "formal_decision_record_sha256": decision_sha,
        "input_snapshot_path": input_snapshot.resolve(),
        "created_at_utc": None,
    }


def test_adaptive_shadow_audit_uses_deep_runtime_audit(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    calls = []

    def audit(project_root: Path, formal_root: Path, shadow_root: Path):
        calls.append((project_root, formal_root, shadow_root))
        return {
            "schema_version": 1,
            "integrity_valid": True,
            "valid": True,
            "deep_replayed_outcome_count": 2,
        }

    monkeypatch.setattr(cli, "audit_adaptive_shadow_runtime", audit)

    assert cli.main(["--root", str(tmp_path), "adaptive-shadow", "audit"]) == 0
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["deep_replayed_outcome_count"] == 2
    project_root = tmp_path.resolve()
    assert calls == [
        (
            project_root,
            project_root / "runtime" / "prospective" / "5.0",
            project_root / "runtime" / "adaptive-shadow" / "1",
        )
    ]


def test_adaptive_shadow_sync_cli_exit_contract(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    advance_calls = []
    checkpoint_calls = []

    def waiting(*args, **kwargs):
        advance_calls.append((args, kwargs))
        return {
            "schema_version": 1,
            "status": "waiting",
            "reason": "no_shadow_action_due",
            "action": None,
            "observed_at_utc": "2026-09-30T10:00:00Z",
        }

    def no_new_evidence(*args, **kwargs):
        checkpoint_calls.append((args, kwargs))
        return {
            "schema_version": 1,
            "status": "waiting",
            "reason": "no_new_source_evidence",
        }

    monkeypatch.setattr(
        cli,
        "advance_adaptive_shadow",
        waiting,
    )
    monkeypatch.setattr(
        cli,
        "checkpoint_adaptive_shadow_evaluation",
        no_new_evidence,
    )

    assert cli.main(["--root", str(tmp_path), "adaptive-shadow", "sync"]) == 2
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["status"] == "waiting"
    assert rendered["evaluation"]["status"] == "waiting"
    assert rendered["evaluation"]["reason"] == "no_new_source_evidence"
    assert advance_calls[0][1] == {"observed_at_utc": None}
    assert checkpoint_calls[0][1] == {
        "observed_at_utc": "2026-09-30T10:00:00Z"
    }

    monkeypatch.setattr(
        cli,
        "advance_adaptive_shadow",
        lambda *args, **kwargs: {
            "schema_version": 1,
            "status": "advanced",
            "reason": "shadow_outcome_appended",
            "action": "outcome",
        },
    )
    assert cli.main(["--root", str(tmp_path), "adaptive-shadow", "sync"]) == 0
    assert json.loads(capsys.readouterr().out)["action"] == "outcome"


def test_adaptive_shadow_sync_checkpoints_evaluation_after_execution_waits(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        cli,
        "advance_adaptive_shadow",
        lambda *args, **kwargs: {
            "schema_version": 1,
            "status": "waiting",
            "reason": "no_shadow_action_due",
            "action": None,
            "observed_at_utc": "2026-09-30T10:00:00Z",
        },
    )
    monkeypatch.setattr(
        cli,
        "checkpoint_adaptive_shadow_evaluation",
        lambda *args, **kwargs: {
            "schema_version": 1,
            "status": "checkpointed",
            "reason": "new_sealed_pair_or_miss_evidence",
            "evaluation_record_sha256": "a" * 64,
        },
    )

    assert cli.main(["--root", str(tmp_path), "adaptive-shadow", "sync"]) == 0
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["status"] == "advanced"
    assert rendered["reason"] == "shadow_evaluation_checkpointed"
    assert rendered["action"] == "evaluation"
    assert rendered["evaluation"]["status"] == "checkpointed"
    assert (
        rendered["evaluation"]["reason"]
        == "new_sealed_pair_or_miss_evidence"
    )


def test_adaptive_shadow_sync_cli_fails_closed(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    def fail(*args, **kwargs):
        raise cli.AdaptiveShadowControllerError("binding differs")

    monkeypatch.setattr(cli, "advance_adaptive_shadow", fail)

    assert cli.main(["--root", str(tmp_path), "adaptive-shadow", "sync"]) == 3
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["status"] == "blocked"
    assert rendered["reason"] == "shadow_controller_error"


def test_adaptive_is_the_default_research_suite() -> None:
    research = build_parser().parse_args(["research", "run", "--canary"])

    assert research.suite == "adaptive"


def test_default_root_finds_checkout_above_noneditable_wheel(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "factor-lab"
    (project / ".git").mkdir(parents=True)
    (project / "protocols").mkdir()
    (project / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (project / "protocols" / "5.0.json").write_text("{}\n", encoding="utf-8")
    installed = (
        project
        / "runtime/environments/5.9/Lib/site-packages/factor_lab/cli.py"
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.setattr(cli, "__file__", str(installed))
    monkeypatch.chdir(elsewhere)

    assert cli._root() == project.resolve()


def test_explicit_root_does_not_require_implicit_discovery(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    def fail_discovery() -> Path:
        raise AssertionError("explicit --root must bypass discovery")

    monkeypatch.setattr(cli, "_root", fail_discovery)
    monkeypatch.setattr(
        cli,
        "ledger_status",
        lambda root: {"ledger_root": str(Path(root).resolve())},
    )

    assert cli.main(["--root", str(tmp_path), "prospective", "status"]) == 0
    rendered = json.loads(capsys.readouterr().out)
    assert Path(rendered["ledger_root"]) == (
        tmp_path / "runtime/prospective/5.0"
    ).resolve()

