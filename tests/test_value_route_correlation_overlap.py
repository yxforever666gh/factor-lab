
import pandas as pd

from factor_lab.value_route_correlation_overlap import pair_metrics, ROUTE_BUCKETS, reconstruct_route_frame


def _frame(vals):
    return pd.DataFrame({"date":["2020-01-01"]*10+["2020-01-02"]*10,"ticker":[f"T{i}" for i in range(10)]*2,"factor_value":vals*2,"forward_return_5d":[v/100 for v in vals]*2})


def test_identical_signals_have_high_corr_and_overlap():
    f=_frame(list(range(10)))
    m=pair_metrics("industry_relative_value", f, "value_quality_no_distress", f.copy())
    assert m["factor_score_spearman_corr"] == 1.0
    assert m["long_bucket_overlap_mean"] == 1.0
    assert m["common_dates"] == 2


def test_reconstruct_route_frame_blocks_when_dataset_is_missing(tmp_path):
    f, meta = reconstruct_route_frame(
        "value_quality_no_distress",
        dataset_path=tmp_path / "missing.csv",
    )
    assert f.empty
    assert meta["status"] == "blocked"
    assert meta["reason"] == "missing_dataset"


def test_reconstruct_route_frame_from_explicit_dataset(tmp_path):
    dataset_path = tmp_path / "route.csv"
    pd.DataFrame(
        {
            "date": ["2020-01-02", "2020-01-02"],
            "ticker": ["000001.SZ", "000002.SZ"],
            "forward_return_5d": [0.01, -0.02],
            "industry_relative_book_yield": [0.2, -0.1],
            "roe": [0.3, 0.1],
        }
    ).to_csv(dataset_path, index=False)

    f, meta = reconstruct_route_frame(
        "value_quality_no_distress",
        dataset_path=dataset_path,
    )
    assert meta["status"] == "ok"
    assert {"date","ticker","forward_return_5d","factor_value"}.issubset(f.columns)
    assert len(f) == 2
