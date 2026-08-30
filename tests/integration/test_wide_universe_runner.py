from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-wide-universe-evidence.py"
SPEC = importlib.util.spec_from_file_location("run_wide_universe_evidence", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


@pytest.mark.parametrize(
    ("physical_end", "expected"),
    [
        ("2022-12-31", ("train",)),
        ("2024-12-31", ("train", "validation")),
        ("2026-08-21", ("train", "validation", "audit")),
    ],
)
def test_evaluation_phase_bounds_only_opens_physically_available_phases(
    physical_end: str, expected: tuple[str, ...]
) -> None:
    bounds = RUNNER._evaluation_phase_bounds(physical_end)

    assert tuple(bounds) == expected
    assert all(value.end <= pd.Timestamp(physical_end) for value in bounds.values())


def test_evaluation_phase_bounds_rejects_pre_anchor_stage() -> None:
    with pytest.raises(ValueError, match="precedes the frozen anchor"):
        RUNNER._evaluation_phase_bounds("2017-01-02")


def test_pre_return_protocol_amendment_has_valid_lineage_and_self_hash() -> None:
    protocol = RUNNER._read_json(ROOT / "protocols" / "6.1-wide-universe.json")
    amendment = RUNNER._read_json(
        ROOT / "protocols" / "6.1-wide-universe-amendment-1.json"
    )

    assert protocol["payload_sha256"] == RUNNER._payload_sha256(protocol)
    assert amendment["payload_sha256"] == RUNNER._payload_sha256(amendment)
    assert amendment["wide_return_evaluation_opened_before_freeze"] is False
    assert amendment["base_protocol"]["payload_sha256"] == (
        "4d544251e3d64f48f1980c7886ea33418118fe7112b0a5f62d18ce270dab781f"
    )


def test_selected_definition_requires_exact_frozen_contract() -> None:
    candidate_id = "daily_adv20_top1500"
    expected = RUNNER._selected_definition(candidate_id)

    assert RUNNER._selected_definition_matches(expected, candidate_id)
    for key in tuple(expected):
        tampered = dict(expected)
        tampered[key] = "tampered"
        assert not RUNNER._selected_definition_matches(tampered, candidate_id)
    with_extra = {**expected, "unregistered_override": True}
    assert not RUNNER._selected_definition_matches(with_extra, candidate_id)


def test_phase_trace_hashes_bind_period_trade_and_daily_nav_rows() -> None:
    result = SimpleNamespace(
        periods=[
            {
                "signal_date": "2022-12-20",
                "start_date": "2022-12-21",
                "end_date": "2022-12-30",
                "account_nav_path_start_sequence": 0,
                "account_nav_path_end_sequence": 1,
                "opaque_exact_field": "train",
            },
            {
                "signal_date": "2023-01-03",
                "start_date": "2023-01-04",
                "end_date": "2023-01-17",
                "account_nav_path_start_sequence": 1,
                "account_nav_path_end_sequence": 2,
                "opaque_exact_field": "validation",
            },
        ],
        trades=[
            {"date": "2022-12-21", "ticker": "A", "notional": 1.0},
            {"date": "2023-01-04", "ticker": "B", "notional": 2.0},
        ],
        account_nav_path=[
            {"sequence": 0, "nav": 100.0},
            {"sequence": 1, "nav": 101.0},
            {"sequence": 2, "nav": 102.0},
        ],
    )
    bounds = RUNNER.PhaseBounds.from_values("2017-01-03", "2022-12-31")
    baseline = RUNNER._phase_trace_sha256(result, bounds)

    result.trades[0]["notional"] = 1.01
    assert RUNNER._phase_trace_sha256(result, bounds) != baseline
    result.trades[0]["notional"] = 1.0
    result.periods[0]["opaque_exact_field"] = "tampered"
    assert RUNNER._phase_trace_sha256(result, bounds) != baseline
    result.periods[0]["opaque_exact_field"] = "train"
    result.account_nav_path[0]["nav"] = 99.0
    assert RUNNER._phase_trace_sha256(result, bounds) != baseline


def test_audit_contract_freezes_one_cutoff_and_create_only_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit_path = tmp_path / "historical-audit.json"
    monkeypatch.setattr(RUNNER, "AUDIT_EVIDENCE_PATH", str(audit_path))
    args = RUNNER._parse_args(
        [
            "--mode",
            "audit",
            "--freeze",
            str(ROOT / RUNNER.WINNER_FREEZE),
            "--audit-end",
            RUNNER.AUDIT_END.date().isoformat(),
        ]
    )
    audit_path.write_text("{}", encoding="utf-8")

    with pytest.raises(FileExistsError, match="create-only"):
        RUNNER.run_audit(args)


def test_mode_defaults_physically_separate_selection_and_audit_status() -> None:
    selection = RUNNER._parse_args(["--mode", "selection"])
    audit = RUNNER._parse_args(
        [
            "--mode",
            "audit",
            "--freeze",
            str(ROOT / "protocols" / "evidence" / "6.1" / "winner-freeze.json"),
            "--audit-end",
            "2026-08-21",
        ]
    )

    assert selection.suspensions != audit.suspensions
    assert selection.suspension_metadata != audit.suspension_metadata
    assert selection.stock_st_checkpoint != audit.stock_st_checkpoint
    assert selection.train_suspensions != selection.suspensions
    assert selection.train_suspension_metadata != selection.suspension_metadata
    assert selection.train_stock_st_checkpoint != selection.stock_st_checkpoint
    assert "train" in selection.train_stock_st_checkpoint.parts
    assert "selection" in selection.suspensions.parts
    assert "audit" in audit.suspensions.parts


def test_audit_mode_rejects_selection_status_paths() -> None:
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "audit",
                "--freeze",
                str(ROOT / "winner.json"),
                "--audit-end",
                "2026-08-21",
                "--stock-st-checkpoint",
                str(ROOT / RUNNER.SELECTION_ST_CHECKPOINT),
            ]
        )


def test_selection_mode_rejects_shared_train_and_validation_status_path() -> None:
    shared = str(ROOT / "shared-stock-st.json")
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "selection",
                "--train-stock-st-checkpoint",
                shared,
                "--stock-st-checkpoint",
                shared,
            ]
        )


def test_audit_mode_rejects_default_train_status_path() -> None:
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "audit",
                "--freeze",
                str(ROOT / "winner.json"),
                "--audit-end",
                "2026-08-21",
                "--stock-st-checkpoint",
                str(ROOT / RUNNER.TRAIN_ST_CHECKPOINT),
            ]
        )


def test_scope_filter_ignores_only_bj_and_rejects_unknown_sh_sz() -> None:
    date = pd.Timestamp("2024-01-02")
    frame = pd.DataFrame(
        {"ts_code": ["000001.SZ", "430001.BJ"], "value": [1, 2]}
    )

    selected, ignored = RUNNER._restrict_partition_to_security_scope(
        frame,
        identifier="ts_code",
        allowed_tickers={"000001.SZ"},
        role="stock_st",
        date=date,
    )

    assert selected["ts_code"].tolist() == ["000001.SZ"]
    assert ignored == 1
    with pytest.raises(ValueError, match="absent from the security master"):
        RUNNER._restrict_partition_to_security_scope(
            pd.DataFrame({"ts_code": ["999999.SH"]}),
            identifier="ts_code",
            allowed_tickers={"000001.SZ"},
            role="stock_st",
            date=date,
        )
