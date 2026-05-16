import pandas as pd

from factor_lab.margin_controlled_probe import build_margin_controlled_probe, load_margin_feature_sample
from factor_lab.margin_feature_builder import MarginFeatureBuildConfig


def _sample_frame() -> pd.DataFrame:
    rows = []
    for year in [2020, 2021, 2022, 2023]:
        for month in range(1, 4):
            date = f"{year}{month:02d}28"
            for i in range(20):
                baseline = (i % 5) / 10
                low_margin = i / 20
                # Holdout intentionally benefits from confirmation above baseline.
                forward = 0.001 * baseline + 0.02 * low_margin
                rows.append({
                    "date": date,
                    "ticker": f"{i:06d}.SZ",
                    "forward_return_5d": forward,
                    "value_quality_baseline": baseline,
                    "low_margin_crowding": low_margin,
                    "margin_low_crowding_confirmation": baseline + low_margin,
                })
    return pd.DataFrame(rows)


def test_load_margin_feature_sample_requires_expected_columns(tmp_path):
    path = tmp_path / "sample.csv"
    _sample_frame().to_csv(path, index=False)

    out = load_margin_feature_sample(path)

    assert len(out) == len(_sample_frame())
    assert {"value_quality_baseline", "low_margin_crowding", "margin_low_crowding_confirmation"}.issubset(out.columns)


def test_build_margin_controlled_probe_strong_passes_when_holdout_improves(tmp_path):
    path = tmp_path / "sample.csv"
    _sample_frame().to_csv(path, index=False)

    result = build_margin_controlled_probe(
        path,
        holdout_start="2023-01-01",
        config=MarginFeatureBuildConfig(min_overlap_rows=10, min_overlap_dates=1, benchmark_spread=0.001),
    )

    assert result["coverage"]["holdout_dates"] == 3
    assert result["decision"]["decision"] == "strong_pass_prepare_workflow_integration"


def test_build_margin_controlled_probe_fails_when_holdout_confirmation_collapses(tmp_path):
    df = _sample_frame()
    mask = df["date"].astype(str).str.startswith("2023")
    df.loc[mask, "forward_return_5d"] = -df.loc[mask, "margin_low_crowding_confirmation"]
    path = tmp_path / "sample.csv"
    df.to_csv(path, index=False)

    result = build_margin_controlled_probe(path, holdout_start="2023-01-01")

    assert result["decision"]["decision"] == "fail_stop_margin_low_crowding_probe"
