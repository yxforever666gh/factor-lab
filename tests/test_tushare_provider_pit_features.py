import pandas as pd

from factor_lab.tushare_provider import TushareDataProvider


class FakePitProvider(TushareDataProvider):
    def __init__(self):
        pass

    def _fetch_financial_statement_tables(self, *, tickers, start_date, end_date, timing=None):
        assert tickers == ["000001.SZ"]
        return {
            "cashflow": pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "ann_date": "20190430",
                        "f_ann_date": "20190430",
                        "end_date": "20181231",
                        "n_cashflow_act": 20.0,
                        "net_profit": 10.0,
                        "free_cashflow": 5.0,
                    }
                ]
            ),
            "balancesheet": pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "ann_date": "20190430",
                        "f_ann_date": "20190430",
                        "end_date": "20181231",
                        "total_assets": 100.0,
                        "total_liab": 30.0,
                        "total_cur_assets": 40.0,
                        "total_cur_liab": 20.0,
                    }
                ]
            ),
            "fina_indicator": pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "ann_date": "20190430",
                        "end_date": "20181231",
                        "debt_to_assets": 0.31,
                        "netprofit_yoy": 12.0,
                        "tr_yoy": 8.0,
                    }
                ]
            ),
        }


def test_enrich_frame_with_pit_financial_features_adds_aliases_and_provenance(tmp_path):
    frame = pd.DataFrame(
        [
            {"ticker": "000001.SZ", "date": "2019-03-01", "industry_relative_book_yield": 0.1},
            {"ticker": "000001.SZ", "date": "2019-05-02", "industry_relative_book_yield": 0.2},
        ]
    )

    enriched = FakePitProvider().enrich_frame_with_pit_financial_features(frame, cache_dir=tmp_path)

    early = enriched.loc[enriched["date"] == pd.Timestamp("2019-03-01")].iloc[0]
    late = enriched.loc[enriched["date"] == pd.Timestamp("2019-05-02")].iloc[0]
    assert pd.isna(early["operating_cashflow_to_profit"])
    assert late["operating_cashflow_to_profit"] == 2.0
    assert late["debt_to_asset"] == 0.31
    assert late["debt_to_assets"] == 0.31
    assert late["profit_yoy"] == 12.0
    assert late["netprofit_yoy"] == 12.0
    assert late["revenue_yoy"] == 8.0
    assert late["tr_yoy"] == 8.0
    assert bool(late["pit_feature_validated"])
    assert "20181231" in str(late["pit_source_end_date"])
    assert "20190430" in str(late["pit_source_ann_date"])


def test_enrich_frame_retains_cashflow_raw_inputs_only_when_diagnostics_requested(tmp_path):
    frame = pd.DataFrame(
        [
            {"ticker": "000001.SZ", "date": "2019-05-02", "industry_relative_book_yield": 0.2},
        ]
    )

    normal = FakePitProvider().enrich_frame_with_pit_financial_features(frame, cache_dir=tmp_path / "normal")
    diagnostic = FakePitProvider().enrich_frame_with_pit_financial_features(
        frame,
        cache_dir=tmp_path / "diagnostic",
        retain_pit_cashflow_diagnostics=True,
    )

    assert "pit_cashflow_numerator_raw" not in normal.columns
    assert "pit_cashflow_denominator_raw" not in normal.columns
    assert diagnostic.loc[0, "pit_cashflow_numerator_raw"] == 20.0
    assert diagnostic.loc[0, "pit_cashflow_denominator_raw"] == 10.0
    assert "_diag" in next((tmp_path / "diagnostic").glob("pit_financial_*.csv")).name
