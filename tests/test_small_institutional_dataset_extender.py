import json

import pandas as pd

from factor_lab.small_institutional_dataset_extender import (
    build_small_institutional_dataset_extension_plan,
    extend_small_institutional_dataset,
)


def _write_dataset(path, start="2020-01-31", periods=3):
    rows = []
    for date in pd.date_range(start, periods=periods, freq="ME"):
        rows.append(
            {
                "date": str(date.date()),
                "ticker": "000001.SZ",
                "forward_return_5d": 0.01,
                "industry_relative_book_yield": 0.2,
                "industry_relative_earnings_yield": 0.1,
                "roe": 0.05,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _policy(path, dataset_path):
    payload = {
        "dataset_path": str(dataset_path),
        "signal_columns": ["industry_relative_book_yield", "industry_relative_earnings_yield", "roe"],
        "return_column": "forward_return_5d",
        "year_windows": [
            {"label": "2020-2021", "start_date": "2020-01-01", "end_date": "2021-12-31"},
            {"label": "2022-2023", "start_date": "2022-01-01", "end_date": "2023-12-31"},
        ],
        "holding_counts": [50, 75, 100],
        "universe_limit": 100,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _extended_frame():
    rows = []
    for date in pd.to_datetime(["2020-01-31", "2021-12-31", "2022-12-30", "2023-12-29"]):
        rows.append(
            {
                "date": date,
                "ticker": "000001.SZ",
                "forward_return_5d": 0.01,
                "industry_relative_book_yield": 0.2,
                "industry_relative_earnings_yield": 0.1,
                "roe": 0.05,
            }
        )
    return pd.DataFrame(rows)


def test_extension_plan_uses_feature_store_when_it_covers_required_window(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _write_dataset(dataset_path, periods=3)
    policy_path = _policy(tmp_path / "policy.json", dataset_path)

    plan = build_small_institutional_dataset_extension_plan(
        policy_path=policy_path,
        inspect_coverage_fn=lambda **kwargs: {"available": True, "covers_exact": True, "min_date": "2020-01-01", "max_date": "2023-12-31"},
    )

    assert plan["extension_status"] == "ready_from_feature_store"
    assert plan["required_window"] == {"start_date": "2020-01-01", "end_date": "2023-12-31"}
    assert plan["current_dataset"]["max_date"] == "2020-03-31"
    assert plan["api_fetch_required"] is False
    assert plan["next_action"] == "write_extended_dataset_from_feature_store"


def test_extension_plan_blocks_external_fetch_when_feature_store_does_not_cover(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _write_dataset(dataset_path, periods=3)
    policy_path = _policy(tmp_path / "policy.json", dataset_path)

    plan = build_small_institutional_dataset_extension_plan(
        policy_path=policy_path,
        inspect_coverage_fn=lambda **kwargs: {"available": True, "covers_exact": False, "min_date": "2020-01-01", "max_date": "2021-12-31"},
    )

    assert plan["extension_status"] == "needs_external_fetch"
    assert plan["api_fetch_required"] is True
    assert plan["next_action"] == "run_with_write_and_allow_fetch"


def test_dry_run_does_not_write_extended_dataset(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _write_dataset(dataset_path, periods=3)
    original = dataset_path.read_text(encoding="utf-8")
    policy_path = _policy(tmp_path / "policy.json", dataset_path)

    result = extend_small_institutional_dataset(
        policy_path=policy_path,
        write=False,
        json_path=tmp_path / "extension.json",
        markdown_path=tmp_path / "extension.md",
        inspect_coverage_fn=lambda **kwargs: {"available": True, "covers_exact": True, "min_date": "2020-01-01", "max_date": "2023-12-31"},
        slice_feature_store_fn=lambda **kwargs: _extended_frame(),
    )

    assert result["extension_status"] == "ready_from_feature_store"
    assert result["write_performed"] is False
    assert dataset_path.read_text(encoding="utf-8") == original


def test_write_rebuilds_dataset_from_feature_store_and_validates(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _write_dataset(dataset_path, periods=3)
    policy_path = _policy(tmp_path / "policy.json", dataset_path)

    result = extend_small_institutional_dataset(
        policy_path=policy_path,
        write=True,
        json_path=tmp_path / "extension.json",
        markdown_path=tmp_path / "extension.md",
        inspect_coverage_fn=lambda **kwargs: {"available": True, "covers_exact": True, "min_date": "2020-01-01", "max_date": "2023-12-31"},
        slice_feature_store_fn=lambda **kwargs: _extended_frame(),
    )

    written = pd.read_csv(dataset_path)
    assert result["extension_status"] == "written"
    assert result["write_performed"] is True
    assert result["validation"]["covers_required_window"] is True
    assert written["date"].max() == "2023-12-29"
    assert (tmp_path / "dataset.csv.bak").exists()


def test_write_with_allow_fetch_calls_feature_coverage_before_rebuild(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _write_dataset(dataset_path, periods=3)
    policy_path = _policy(tmp_path / "policy.json", dataset_path)
    calls = []

    def ensure_fn(**kwargs):
        calls.append(kwargs)
        return kwargs["universe_name"]

    result = extend_small_institutional_dataset(
        policy_path=policy_path,
        write=True,
        json_path=tmp_path / "extension.json",
        markdown_path=tmp_path / "extension.md",
        allow_fetch=True,
        inspect_coverage_fn=lambda **kwargs: {"available": True, "covers_exact": False, "min_date": "2020-01-01", "max_date": "2021-12-31"},
        ensure_coverage_fn=ensure_fn,
        slice_feature_store_fn=lambda **kwargs: _extended_frame(),
    )

    assert result["extension_status"] == "written"
    assert calls
    assert calls[0]["start_date"] == "2020-01-01"
    assert calls[0]["end_date"] == "2023-12-31"


def test_write_blocks_fetch_without_allow_fetch(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _write_dataset(dataset_path, periods=3)
    policy_path = _policy(tmp_path / "policy.json", dataset_path)

    result = extend_small_institutional_dataset(
        policy_path=policy_path,
        write=True,
        json_path=tmp_path / "extension.json",
        markdown_path=tmp_path / "extension.md",
        allow_fetch=False,
        inspect_coverage_fn=lambda **kwargs: {"available": False, "covers_exact": False, "min_date": None, "max_date": None},
    )

    assert result["extension_status"] == "blocked_external_fetch_not_allowed"
    assert result["write_performed"] is False
    assert result["next_action"] == "rerun_with_allow_fetch_or_seed_feature_store"


def test_allow_fetch_falls_back_to_direct_provider_when_feature_store_has_interior_gap(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _write_dataset(dataset_path, periods=3)
    policy_path = _policy(tmp_path / "policy.json", dataset_path)
    direct_fetch_calls = []

    class Dataset:
        frame = _extended_frame()

    def direct_fetch_fn(**kwargs):
        direct_fetch_calls.append(kwargs)
        return Dataset()

    result = extend_small_institutional_dataset(
        policy_path=policy_path,
        write=True,
        json_path=tmp_path / "extension.json",
        markdown_path=tmp_path / "extension.md",
        allow_fetch=True,
        inspect_coverage_fn=lambda **kwargs: {
            "available": True,
            "covers_exact": False,
            "covers_start": False,
            "min_date": "2020-06-02",
            "max_date": "2026-03-16",
        },
        ensure_coverage_fn=lambda **kwargs: kwargs["universe_name"],
        slice_feature_store_fn=lambda **kwargs: _write_dataset(dataset_path, start="2020-06-02", periods=3) or pd.read_csv(dataset_path),
        direct_fetch_fn=direct_fetch_fn,
    )

    assert result["extension_status"] == "written"
    assert result["write_performed"] is True
    assert result["fallback_fetch_performed"] is True
    assert direct_fetch_calls
    assert direct_fetch_calls[0]["start_date"] == "2020-01-01"
    assert direct_fetch_calls[0]["end_date"] == "2023-12-31"
