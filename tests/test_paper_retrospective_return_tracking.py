import json

import pandas as pd

from factor_lab.paper_retrospective_return_tracking import (
    build_paper_retrospective_return_tracking,
    compute_equal_weight_forward_return,
    retrospective_tracking_to_markdown,
    write_paper_retrospective_return_tracking,
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_compute_equal_weight_forward_return_uses_position_weights_and_forward_column(tmp_path):
    portfolio = {
        "strategy_name": "small_institutional_value_sleeve_mvp",
        "as_of_date": "2023-12-29",
        "positions": [
            {"ticker": "000001.SZ", "weight": 0.25},
            {"ticker": "000002.SZ", "weight": 0.75},
        ],
    }
    dataset = pd.DataFrame(
        [
            {"date": "2023-12-29", "ticker": "000001.SZ", "forward_return_5d": 0.02},
            {"date": "2023-12-29", "ticker": "000002.SZ", "forward_return_5d": -0.01},
            {"date": "2023-12-22", "ticker": "000001.SZ", "forward_return_5d": 0.99},
        ]
    )

    result = compute_equal_weight_forward_return(portfolio, dataset)

    assert result["tracking_status"] == "ok"
    assert result["matched_position_count"] == 2
    assert result["missing_position_count"] == 0
    assert result["portfolio_forward_return"] == -0.0025


def test_build_tracking_reports_insufficient_forward_window_when_returns_missing(tmp_path):
    portfolio_path = _write_json(
        tmp_path / "current_portfolio.json",
        {
            "strategy_name": "small_institutional_value_sleeve_mvp",
            "as_of_date": "2023-12-29",
            "position_count": 2,
            "positions": [{"ticker": "000001.SZ", "weight": 0.5}, {"ticker": "000002.SZ", "weight": 0.5}],
        },
    )
    dataset_path = _write_csv(
        tmp_path / "dataset.csv",
        [
            {"date": "2023-12-29", "ticker": "000001.SZ", "forward_return_5d": ""},
            {"date": "2023-12-29", "ticker": "000002.SZ", "forward_return_5d": ""},
        ],
    )
    diagnostics_path = _write_json(tmp_path / "portfolio_diagnostics.json", {"benchmark": {"benchmark_id": "CSI1000", "tracking_mode": "metadata_only"}})

    payload = build_paper_retrospective_return_tracking(
        portfolio_path=portfolio_path,
        dataset_path=dataset_path,
        diagnostics_path=diagnostics_path,
    )

    assert payload["tracking_status"] == "insufficient_forward_window"
    assert payload["portfolio_return"]["matched_position_count"] == 0
    assert payload["benchmark"]["benchmark_id"] == "CSI1000"
    assert payload["benchmark"]["tracking_mode"] == "metadata_only"


def test_build_tracking_reports_ok_with_forward_return_and_benchmark_metadata(tmp_path):
    portfolio_path = _write_json(
        tmp_path / "current_portfolio.json",
        {
            "strategy_name": "small_institutional_value_sleeve_mvp",
            "as_of_date": "2023-12-29",
            "position_count": 2,
            "positions": [{"ticker": "000001.SZ", "weight": 0.4}, {"ticker": "000002.SZ", "weight": 0.6}],
        },
    )
    dataset_path = _write_csv(
        tmp_path / "dataset.csv",
        [
            {"date": "2023-12-29", "ticker": "000001.SZ", "forward_return_5d": 0.01},
            {"date": "2023-12-29", "ticker": "000002.SZ", "forward_return_5d": 0.03},
        ],
    )
    diagnostics_path = _write_json(tmp_path / "portfolio_diagnostics.json", {"benchmark": {"benchmark_id": "CSI1000", "tracking_mode": "metadata_only"}})

    payload = build_paper_retrospective_return_tracking(
        portfolio_path=portfolio_path,
        dataset_path=dataset_path,
        diagnostics_path=diagnostics_path,
    )

    assert payload["tracking_status"] == "ok"
    assert payload["portfolio_return"]["portfolio_forward_return"] == 0.022
    assert payload["benchmark"]["benchmark_id"] == "CSI1000"


def test_write_tracking_writes_json_and_markdown(tmp_path):
    portfolio_path = _write_json(
        tmp_path / "current_portfolio.json",
        {"strategy_name": "x", "as_of_date": "2023-12-29", "position_count": 1, "positions": [{"ticker": "000001.SZ", "weight": 1.0}]},
    )
    dataset_path = _write_csv(tmp_path / "dataset.csv", [{"date": "2023-12-29", "ticker": "000001.SZ", "forward_return_5d": 0.05}])
    json_path = tmp_path / "retrospective_return_tracking.json"
    markdown_path = tmp_path / "retrospective_return_tracking.md"

    payload = write_paper_retrospective_return_tracking(
        portfolio_path=portfolio_path,
        dataset_path=dataset_path,
        diagnostics_path=tmp_path / "missing_diagnostics.json",
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["tracking_status"] == "ok"
    assert json.loads(json_path.read_text(encoding="utf-8"))["tracking_status"] == "ok"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Paper Retrospective Return Tracking" in markdown
    assert "0.05" in markdown


def test_retrospective_tracking_markdown_includes_insufficient_forward_window():
    markdown = retrospective_tracking_to_markdown(
        {
            "generated_at_utc": "2026-05-12T00:00:00+00:00",
            "tracking_status": "insufficient_forward_window",
            "portfolio": {"strategy_name": "x", "as_of_date": "2023-12-29", "position_count": 2},
            "portfolio_return": {"portfolio_forward_return": None, "matched_position_count": 0, "missing_position_count": 2},
            "benchmark": {"benchmark_id": "CSI1000", "tracking_mode": "metadata_only"},
        }
    )

    assert "insufficient_forward_window" in markdown
    assert "CSI1000" in markdown
