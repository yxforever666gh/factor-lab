import json
from pathlib import Path

import pandas as pd
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
    strategy = parser.parse_args(["strategy", "status", "--verify-data"])
    assert strategy.strategy_command == "status"
    assert strategy.verify_data is True
    targets = parser.parse_args(["strategy", "targets", "--signal-date", "latest"])
    assert targets.strategy_command == "targets"

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


@pytest.mark.parametrize("command", ["prospective", "adaptive-shadow"])
def test_retired_runtime_commands_are_rejected(command: str) -> None:
    with pytest.raises(SystemExit) as raised:
        build_parser().parse_args([command])

    assert raised.value.code == 2


def test_help_imports_without_retired_runtime(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(["--help"])

    assert raised.value.code == 0
    help_text = capsys.readouterr().out
    assert "data" in help_text
    assert "research" in help_text
    assert "strategy" in help_text
    assert "report" in help_text
    assert "prospective" not in help_text
    assert "adaptive-shadow" not in help_text


def test_walk_forward_is_the_default_research_suite() -> None:
    research = build_parser().parse_args(["research", "run", "--canary"])

    assert research.suite == "walk-forward"


def test_retired_adaptive_research_suite_is_rejected() -> None:
    with pytest.raises(SystemExit) as raised:
        build_parser().parse_args(
            ["research", "run", "--suite", "adaptive", "--canary"]
        )

    assert raised.value.code == 2


def test_default_root_finds_checkout_above_noneditable_wheel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "factor-lab"
    (project / ".git").mkdir(parents=True)
    (project / "configs").mkdir()
    (project / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (project / "configs" / "data.json").write_text("{}\n", encoding="utf-8")
    installed = project / "runtime/environments/6.0/Lib/site-packages/factor_lab/cli.py"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.setattr(cli, "__file__", str(installed))
    monkeypatch.chdir(elsewhere)

    assert cli._root() == project.resolve()


def test_explicit_root_does_not_require_implicit_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[Path] = []

    def fail_discovery() -> Path:
        raise AssertionError("explicit --root must bypass discovery")

    def data_command(arguments) -> int:
        captured.append(arguments.root)
        return 0

    monkeypatch.setattr(cli, "_root", fail_discovery)
    monkeypatch.setattr(cli, "_data_command", data_command)

    assert cli.main(["--root", str(tmp_path), "data", "status"]) == 0
    assert captured == [tmp_path]


def test_strategy_status_verifies_tracked_implementation_and_evidence() -> None:
    root = Path(__file__).resolve().parents[2]

    result, exit_code = cli._strategy_status(root, verify_data=False)

    assert exit_code == 0
    assert result["status"] == "ready"
    assert result["route"] == "fixed_core_low_churn"
    assert result["profit_claim_allowed"] is False
    assert result["pit_event_search"] == {
        "selected_candidate_id": None,
        "candidate_count": 6,
        "candidate_pass_count": 0,
    }
    assert result["orthogonal_canonical_search"] == {
        "selected_candidate_id": None,
        "candidate_count": 129,
        "train_q20_positive_count": 0,
        "validation_q20_positive_count": 0,
    }
    assert result["next_data_lane"] == {
        "protocol_id": "6.0-analyst-revisions",
        "status": "ingestion_implemented_research_spec_permission_and_vintage_blocked",
        "source_endpoint": "report_rc",
        "returns_or_labels_opened": False,
        "alpha_claim": False,
        "selected_route": "report_rc_sell_side_estimate_revisions",
        "current_permission": "trial_only",
    }
    assert all(
        check["status"] in {"match", "not_verified"}
        for check in result["checks"]
    )
    categories = {check["category"] for check in result["checks"]}
    assert "evidence:pit_event_protocol" in categories
    assert "evidence:pit_event_result" in categories
    assert "implementation:long_only" in categories
    assert "implementation:execution_kernel" in categories
    assert "implementation:analyst_data" in categories
    assert "implementation:analyst_sync" in categories
    assert "evidence:analyst_protocol" in categories
    assert "evidence_payload:analyst_protocol" in categories
    assert "evidence:analyst_scout" in categories
    assert "evidence_payload:analyst_scout" in categories
    assert "protocol_payload:low_churn" in categories


def test_strategy_status_detects_event_evidence_tamper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    actual_sha256 = cli._file_sha256

    def tampered_sha256(path: Path) -> str:
        if path.name == "pit-event-negative.json":
            return "0" * 64
        return actual_sha256(path)

    monkeypatch.setattr(cli, "_file_sha256", tampered_sha256)

    result, exit_code = cli._strategy_status(root, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] == "evidence:pit_event_result"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_strategy_status_detects_protocol_payload_tamper(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    protocol = json.loads(
        (root / "protocols" / "6.0-low-churn.json").read_text(encoding="utf-8")
    )
    protocol["objective"] = "tampered without refreshing payload hash"
    target = tmp_path / "protocols" / "6.0-low-churn.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(protocol), encoding="utf-8")

    result, exit_code = cli._strategy_status(tmp_path, verify_data=False)

    assert exit_code == 3
    assert any(
        check["category"] == "protocol_payload:low_churn"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_strategy_targets_rebuilds_absolute_sleeve_state(tmp_path: Path) -> None:
    data_root = tmp_path / "runtime" / "data" / "top500"
    data_root.mkdir(parents=True)
    calendar = pd.bdate_range("2017-01-03", periods=12)
    tickers = [f"T{value:02d}" for value in range(1, 31)]
    rows: list[dict[str, object]] = []
    for date_index, date in enumerate(calendar):
        ranking = tickers[date_index:] + tickers[:date_index]
        strength = {
            ticker: float(len(tickers) - rank)
            for rank, ticker in enumerate(ranking)
        }
        for ticker in tickers:
            value = strength[ticker]
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "eligible": True,
                    "universe_member": True,
                    "earnings_yield": value,
                    "pb": 1.0,
                    "book_yield": value,
                    "volatility_20": 31.0 - value,
                }
            )
    pd.DataFrame(rows).to_parquet(data_root / "features.parquet", index=False)
    pd.DataFrame({"date": calendar}).to_parquet(
        data_root / "execution.parquet", index=False
    )

    result = cli._strategy_targets(tmp_path, "latest")

    assert result["requested_signal_date"] == calendar[-1].date().isoformat()
    assert result["decision"]["calendar_index"] == 11
    assert result["decision"]["sleeve"] == 1
    assert result["decision"]["status"] == "ok"
    assert len(result["decision"]["selected_tickers"]) == 10

