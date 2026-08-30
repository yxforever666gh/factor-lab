import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

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
    assert parser.parse_args(
        ["strategy", "status", "--release", "6.3"]
    ).release == "6.3"
    assert parser.parse_args(
        ["strategy", "status", "--release", "7.0"]
    ).release == "7.0"
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


def test_strategy_status_verifies_tracked_implementation_and_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    # This test validates the checked-out evidence chain.  A workflow cannot
    # require its own still-running GitHub job to have completed successfully;
    # the remote-CI contract is covered by dedicated tests below.
    monkeypatch.setattr(cli, "_v7_require_head_ci", lambda _root: "a" * 40)
    result, exit_code = cli._strategy_status(root, verify_data=False)
    closure_exists = (root / cli.V7_CLOSURE_PATH).is_file()
    if closure_exists:
        assert exit_code == 0
    else:
        assert exit_code in {0, 2}
        assert result["status"] in {
            "implementation_ready_for_preselection_closure",
            "implementation_pending_clean_commit",
        }
    assert result["version"] == "7.0"
    assert result["route"] == "fixed_multi_asset_causal_trend_budget"
    assert result["profit_claim_allowed"] is False
    if not closure_exists:
        assert result["audit_status"] == "not_opened"
    assert all(
        check["status"] in {"match", "not_verified", "pending_clean_commit"}
        for check in result["checks"]
    )
    categories = {check["category"] for check in result["checks"]}
    if closure_exists:
        assert "release_evidence_chain" in categories
    else:
        assert "protocol_payload" in categories
        assert "asset_selection_payload" in categories


def test_default_strategy_status_reports_the_7_0_preclosure_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if (root / cli.V7_CLOSURE_PATH).is_file():
        pytest.skip("7.0 closure has already superseded the preclosure state")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v7_require_head_ci", lambda _root: "a" * 40)

    result, exit_code = cli._strategy_status(root, verify_data=False)

    assert exit_code == 0
    assert result["status"] == "implementation_ready_for_preselection_closure"
    assert result["version"] == "7.0"
    assert result["selected_candidate_id"] is None
    assert result["audit_status"] == "not_opened"
    assert result["profit_claim_allowed"] is False
    assert all(check["status"] == "match" for check in result["checks"])


def test_default_preclosure_status_does_not_claim_dirty_tree_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if (root / cli.V7_CLOSURE_PATH).is_file():
        pytest.skip("7.0 closure has already superseded the preclosure state")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: False)

    result, exit_code = cli._strategy_status(root, verify_data=False)

    assert exit_code == 2
    assert result["status"] == "implementation_pending_clean_commit"
    assert any(
        check["category"] == "preclosure_working_tree"
        and check["status"] == "pending_clean_commit"
        for check in result["checks"]
    )


def test_7_0_preclosure_status_rejects_contradictory_opened_disclosure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Path(__file__).resolve().parents[2]
    selection = json.loads(
        (root / cli.V7_ASSET_SELECTION_PATH).read_text(encoding="utf-8")
    )
    disclosure = json.loads(
        (root / cli.V7_PRECLOSURE_TRAIN_PATH).read_text(encoding="utf-8")
    )
    disclosure["disclosure"]["audit_market_outcomes_opened"] = True
    disclosure["payload_sha256"] = cli._canonical_payload_sha256(disclosure)
    disclosure_path = tmp_path / cli.V7_PRECLOSURE_TRAIN_PATH
    disclosure_path.parent.mkdir(parents=True)
    disclosure_path.write_text(
        json.dumps(disclosure, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    selection_path = tmp_path / cli.V7_ASSET_SELECTION_PATH
    selection_path.parent.mkdir(parents=True, exist_ok=True)
    selection_path.write_bytes((root / cli.V7_ASSET_SELECTION_PATH).read_bytes())
    protocol = json.loads(
        (root / cli.V7_PROTOCOL_PATH).read_text(encoding="utf-8")
    )
    protocol["preclosure_train_disclosure"].update(
        {
            "file_sha256": hashlib.sha256(disclosure_path.read_bytes()).hexdigest(),
            "payload_sha256": disclosure["payload_sha256"],
        }
    )
    protocol["payload_sha256"] = cli._canonical_payload_sha256(protocol)
    protocol_path = tmp_path / cli.V7_PROTOCOL_PATH
    protocol_path.parent.mkdir(parents=True, exist_ok=True)
    protocol_path.write_text(
        json.dumps(protocol, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    runner = cli._load_v7_runner(root)
    monkeypatch.setattr(cli, "_load_v7_runner", lambda _root: runner)
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v7_require_head_ci", lambda _root: "a" * 40)

    result, exit_code = cli._strategy_status_7_0_pending(
        tmp_path, verify_data=False
    )

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] == "preclosure_outcome_boundary"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_default_preclosure_status_requires_pushed_successful_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if (root / cli.V7_CLOSURE_PATH).is_file():
        pytest.skip("7.0 closure has already superseded the preclosure state")
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(
        cli,
        "_v7_require_head_ci",
        lambda _root: (_ for _ in ()).throw(RuntimeError("push CI missing")),
    )

    result, exit_code = cli._strategy_status(root, verify_data=False)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] == "preclosure_head_push_ci"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_7_0_status_rejects_orphan_audit_from_full_chain_verifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closure = tmp_path / cli.V7_CLOSURE_PATH
    closure.parent.mkdir(parents=True)
    closure.write_text("{}", encoding="utf-8")
    verifier = SimpleNamespace(
        verify_release_state=lambda **_: (_ for _ in ()).throw(
            ValueError("audit/result exists without a winner freeze")
        )
    )
    monkeypatch.setattr(cli, "_load_v7_runner", lambda _root: verifier)

    result, exit_code = cli._strategy_status_7_0(tmp_path, verify_data=True)

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert result["canonical_data_hashes_verified"] is False
    assert "without a winner freeze" in result["checks"][-1]["error"]


def test_7_0_verify_data_is_forwarded_to_stage_verifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closure_path = tmp_path / cli.V7_CLOSURE_PATH
    closure_path.parent.mkdir(parents=True)
    closure_path.write_text("{}", encoding="utf-8")
    calls: list[tuple[bool, bool]] = []

    def verify_release_state(*, verify_data: bool, verify_runtime: bool) -> dict:
        calls.append((verify_data, verify_runtime))
        return {
            "status": "selection_frozen_pending_historical_audit",
            "closure": {"route": "fixed_multi_asset_causal_trend_budget"},
            "protocol": {
                "protocol_id": "factor-lab/7.0/fixed-multi-asset-trend-budget-v1",
                "claim_contract": {
                    "historical_evidence_class": "pre_registered_historical_diagnostic_only",
                    "profit_claim_allowed": False,
                },
            },
            "selection": {},
            "freeze": {"selected_candidate_id": "causal_multi_horizon_trend_budget"},
            "audit": None,
            "result": None,
        }

    monkeypatch.setattr(
        cli,
        "_load_v7_runner",
        lambda _root: SimpleNamespace(verify_release_state=verify_release_state),
    )
    monkeypatch.setattr(cli, "_working_tree_is_clean", lambda _root: True)
    monkeypatch.setattr(cli, "_v7_require_head_ci", lambda _root: "a" * 40)

    result, exit_code = cli._strategy_status_7_0(tmp_path, verify_data=True)

    assert exit_code == 0
    assert calls == [(True, False)]
    assert result["canonical_data_hashes_verified"] is True
    assert any(
        check["category"] == "head_push_ci" and check["status"] == "match"
        for check in result["checks"]
    )
    assert any(
        check["category"] == "canonical_stage_artifacts"
        and check["status"] == "match"
        for check in result["checks"]
    )


def test_strategy_status_detects_implementation_tamper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if not (root / cli.PRESELECTION_CLOSURE_PATH).is_file():
        pytest.skip("6.3 closure is created from the clean implementation commit")
    actual_sha256 = cli._file_sha256

    def tampered_sha256(path: Path) -> str:
        if path.name == "wide_pricing.py":
            return "0" * 64
        return actual_sha256(path)

    monkeypatch.setattr(cli, "_file_sha256", tampered_sha256)

    result, exit_code = cli._strategy_status(
        root, verify_data=False, release="6.3"
    )

    assert exit_code == 3
    assert result["status"] == "integrity_mismatch"
    assert any(
        check["category"] == "implementation:wide_pricing"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_strategy_status_detects_protocol_file_tamper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    if not (root / cli.PRESELECTION_CLOSURE_PATH).is_file():
        pytest.skip("6.3 closure is created from the clean implementation commit")
    actual_sha256 = cli._file_sha256

    def tampered_sha256(path: Path) -> str:
        if path.name == "6.2-wide-universe.json":
            return "0" * 64
        return actual_sha256(path)

    monkeypatch.setattr(cli, "_file_sha256", tampered_sha256)

    result, exit_code = cli._strategy_status(
        root, verify_data=False, release="6.3"
    )

    assert exit_code == 3
    assert any(
        check["category"] == "protocol_file:wide_universe"
        and check["status"] == "mismatch"
        for check in result["checks"]
    )


def test_strategy_status_can_explicitly_audit_legacy_6_0() -> None:
    root = Path(__file__).resolve().parents[2]

    result, exit_code = cli._strategy_status(
        root, verify_data=False, release="6.0"
    )

    assert exit_code == 3
    assert result["version"] == "6.0"
    assert any(
        check["category"] == "implementation:cli"
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
