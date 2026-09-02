from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.data.corporate_actions import (
    CNINFO_RESPONSE_COLUMNS,
    CorporateActionDataError,
    DIVIDEND_FIELDS,
    TUSHARE_REFERENCE_DIAGNOSTIC_COLUMNS,
    canonical_implemented_actions,
    canonical_cninfo_actions,
    resolve_corporate_actions,
    resolve_cninfo_share_arrivals,
    project_akshare_cninfo_response,
    normalize_dividend_response,
)


def _row(**updates):
    value = {
        "ts_code": "000001.SZ",
        "end_date": "20201231",
        "ann_date": "20210301",
        "div_proc": "实施",
        "stk_div": 0.1,
        "stk_bo_rate": 0.05,
        "stk_co_rate": 0.05,
        "cash_div": 0.2,
        "cash_div_tax": 0.2,
        "record_date": "20210519",
        "ex_date": "20210520",
        "pay_date": "20210521",
        "div_listdate": "20210524",
        "imp_ann_date": "20210517",
        "base_date": "20201231",
        "base_share": 1000.0,
    }
    value.update(updates)
    return value


def _frame(*rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=DIVIDEND_FIELDS)


def test_announcement_duplicates_collapse_to_one_economic_action() -> None:
    frame = _frame(
        _row(ann_date="20210301", imp_ann_date="20210516"),
        _row(ann_date="20210401", imp_ann_date="20210517"),
    )
    result = canonical_implemented_actions(
        frame, start_date="2021-01-01", end_date="2021-12-31"
    )
    assert len(result) == 1
    assert result.iloc[0].source_row_count == 2
    assert result.iloc[0].available_date == "2021-05-17"
    assert result.iloc[0].stock_dividend_per_share == pytest.approx(0.1)
    assert result.iloc[0].cash_dividend_after_tax_per_share == pytest.approx(0.2)


def test_distinct_distributions_on_same_ex_date_remain_distinct() -> None:
    frame = _frame(
        _row(end_date="20201231", stk_div=0.0, stk_bo_rate=0.0, stk_co_rate=0.0, cash_div=1.3, cash_div_tax=1.3),
        _row(end_date="20210331", stk_div=0.0, stk_bo_rate=0.0, stk_co_rate=0.0, cash_div=0.1, cash_div_tax=0.1),
    )
    result = canonical_implemented_actions(
        frame, start_date="2021-01-01", end_date="2021-12-31"
    )
    assert len(result) == 2
    assert result.cash_dividend_after_tax_per_share.sum() == pytest.approx(1.4)
    assert result.action_id.nunique() == 2


def test_different_end_dates_remain_distinct_even_when_payouts_match() -> None:
    frame = _frame(
        _row(end_date="20201231", ann_date="20210301"),
        _row(end_date="20210331", ann_date="20210401"),
    )
    result = canonical_implemented_actions(
        frame, start_date="2021-01-01", end_date="2021-12-31"
    )
    assert len(result) == 2


def test_same_action_identity_with_conflicting_economics_fails() -> None:
    frame = _frame(
        _row(cash_div=0.2, cash_div_tax=0.2),
        _row(cash_div=0.3, cash_div_tax=0.3),
    )
    with pytest.raises(CorporateActionDataError, match="conflicting economics"):
        canonical_implemented_actions(
            frame, start_date="2021-01-01", end_date="2021-12-31"
        )


def test_nonimplemented_rows_are_not_actions() -> None:
    frame = _frame(_row(div_proc="预案", cash_div=0.0, stk_div=0.0, stk_bo_rate=0.0, stk_co_rate=0.0))
    result = canonical_implemented_actions(
        frame, start_date="2021-01-01", end_date="2021-12-31"
    )
    assert result.empty


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"imp_ann_date": None}, "lacks end/record/ex/implementation"),
        ({"imp_ann_date": "20210521"}, "not available"),
        ({"cash_div": 0.2, "pay_date": None}, "pay date"),
        ({"stk_div": 0.1, "div_listdate": None}, "share-list"),
        ({"stk_div": 0.2}, "bonus plus capitalization"),
    ],
)
def test_invalid_implemented_action_fails_closed(updates, message) -> None:
    with pytest.raises(CorporateActionDataError, match=message):
        canonical_implemented_actions(
            _frame(_row(**updates)),
            start_date="2021-01-01",
            end_date="2021-12-31",
        )


def test_provider_response_schema_and_ticker_scope_are_exact() -> None:
    frame = _frame(_row())
    normalized = normalize_dividend_response(
        frame, expected_tickers={"000001.SZ"}
    )
    assert normalized.ts_code.tolist() == ["000001.SZ"]
    with pytest.raises(CorporateActionDataError, match="unexpected tickers"):
        normalize_dividend_response(frame, expected_tickers={"600000.SH"})
    with pytest.raises(CorporateActionDataError, match="columns differ"):
        normalize_dividend_response(frame.drop(columns="base_share"))


def _cninfo_row(**updates):
    value = {
        "ticker": "000001.SZ",
        "implementation_ann_date": "2021-05-17",
        "dividend_type": "年度分红",
        "bonus_per_10": 2.0,
        "capitalization_per_10": 3.0,
        "cash_before_tax_per_10": 10.0,
        "record_date": "2021-05-19",
        "ex_date": "2021-05-20",
        "pay_date": "2021-05-20",
        "share_arrival_date": "2021-05-20",
        "plan_text": "10送2转3派10",
        "report_period": "2020年报",
    }
    value.update(updates)
    return value


def _cninfo_frame(*rows):
    return pd.DataFrame(rows, columns=CNINFO_RESPONSE_COLUMNS)


def test_cninfo_actions_convert_per10_and_keep_special_distribution() -> None:
    frame = _cninfo_frame(
        _cninfo_row(dividend_type="年度分红", cash_before_tax_per_10=12.0),
        _cninfo_row(dividend_type="特别分红", cash_before_tax_per_10=1.0),
    )
    result = canonical_cninfo_actions(
        frame,
        start_date="2021-01-01",
        end_date="2021-12-31",
        expected_tickers={"000001.SZ"},
    )
    assert len(result) == 2
    assert result.cash_dividend_before_tax_per_share.sum() == pytest.approx(1.3)
    assert result.stock_dividend_per_share.eq(0.5).all()


def test_cninfo_exact_duplicate_does_not_change_action_id() -> None:
    one = canonical_cninfo_actions(
        _cninfo_frame(_cninfo_row()),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    duplicate = canonical_cninfo_actions(
        _cninfo_frame(_cninfo_row(), _cninfo_row()),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    assert one.action_id.tolist() == duplicate.action_id.tolist()
    assert duplicate.source_row_count.tolist() == [2]


def test_cninfo_same_identity_conflicting_economics_fails() -> None:
    with pytest.raises(CorporateActionDataError, match="conflicting economics"):
        canonical_cninfo_actions(
            _cninfo_frame(
                _cninfo_row(cash_before_tax_per_10=10.0),
                _cninfo_row(cash_before_tax_per_10=11.0),
            ),
            start_date="2021-01-01",
            end_date="2021-12-31",
        )


def test_cninfo_missing_share_arrival_is_retained_for_secondary_fill() -> None:
    result = canonical_cninfo_actions(
        _cninfo_frame(_cninfo_row(share_arrival_date=None)),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    assert result.share_arrival_date.isna().all()


def test_akshare_cninfo_projection_uses_column_positions_not_labels() -> None:
    localized = pd.DataFrame(
        [[value for value in _cninfo_row().values()][1:]],
        columns=[f"localized_{index}" for index in range(11)],
    )
    projected = project_akshare_cninfo_response(
        localized, ticker="000001.SZ"
    )
    assert tuple(projected.columns) == CNINFO_RESPONSE_COLUMNS
    assert projected.ticker.tolist() == ["000001.SZ"]


def test_missing_cninfo_share_arrival_uses_unique_tushare_timing() -> None:
    cninfo = canonical_cninfo_actions(
        _cninfo_frame(_cninfo_row(share_arrival_date=None)),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    tushare = canonical_implemented_actions(
        _frame(
            _row(
                stk_div=0.5,
                stk_bo_rate=0.2,
                stk_co_rate=0.3,
                pay_date="20210520",
                div_listdate="20210521",
            )
        ),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    result = resolve_cninfo_share_arrivals(cninfo, tushare)
    assert result.share_arrival_date.tolist() == ["2021-05-21"]
    assert result.share_arrival_source.tolist() == ["tushare"]


def test_share_arrival_cross_source_disagreement_fails() -> None:
    cninfo = canonical_cninfo_actions(
        _cninfo_frame(_cninfo_row(share_arrival_date="20210520")),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    tushare = canonical_implemented_actions(
        _frame(
            _row(
                stk_div=0.5,
                stk_bo_rate=0.2,
                stk_co_rate=0.3,
                pay_date="20210520",
                div_listdate="20210521",
            )
        ),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    with pytest.raises(CorporateActionDataError, match="disagree"):
        resolve_cninfo_share_arrivals(cninfo, tushare)


def _reference_diagnostics(
    actions: pd.DataFrame, *, eligible: bool
) -> pd.DataFrame:
    rows = []
    for action in actions.itertuples(index=False):
        rows.append(
            {
                "action_id": action.action_id,
                "ticker": action.ticker,
                "record_date": action.record_date,
                "ex_date": action.ex_date,
                "previous_session": "2021-05-19",
                "previous_close": 10.2,
                "provider_pre_close": 10.0,
                "previous_adj_factor": 1.0,
                "ex_adj_factor": 1.02,
                "factor_ratio": 1.02,
                "price_ratio": 1.02,
                "theoretical_reference": 10.0,
                "theoretical_reference_relative_error": 0.0,
                "factor_reference_absolute_ratio_error": 0.0,
                "factor_jump": True,
                "fallback_eligible": eligible,
                "status": (
                    "eligible_raw_reference_fallback"
                    if eligible
                    else "raw_reference_mismatch"
                ),
            }
        )
    return pd.DataFrame(rows, columns=TUSHARE_REFERENCE_DIAGNOSTIC_COLUMNS)


def test_tushare_economics_require_explicit_raw_reference_admission() -> None:
    empty_cninfo = canonical_cninfo_actions(
        _cninfo_frame(), start_date="2021-01-01", end_date="2021-12-31"
    )
    tushare = canonical_implemented_actions(
        _frame(
            _row(
                stk_div=0.0,
                stk_bo_rate=0.0,
                stk_co_rate=0.0,
                cash_div=0.2,
                cash_div_tax=0.2,
                div_listdate=None,
            )
        ),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    rejected = resolve_corporate_actions(
        empty_cninfo, tushare, _reference_diagnostics(tushare, eligible=False)
    )
    assert rejected.empty
    admitted = resolve_corporate_actions(
        empty_cninfo, tushare, _reference_diagnostics(tushare, eligible=True)
    )
    assert admitted.economic_source.tolist() == [
        "tushare_raw_reference_fallback"
    ]
    assert admitted.cash_dividend_before_tax_per_share.tolist() == [0.2]
    assert admitted.share_arrival_source.tolist() == ["not_applicable"]


def test_tushare_fallback_cannot_overlap_cninfo_ticker_ex_date() -> None:
    cninfo = canonical_cninfo_actions(
        _cninfo_frame(_cninfo_row()),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    tushare = canonical_implemented_actions(
        _frame(
            _row(
                stk_div=0.5,
                stk_bo_rate=0.2,
                stk_co_rate=0.3,
                pay_date="20210520",
                div_listdate="20210520",
            )
        ),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    with pytest.raises(CorporateActionDataError, match="overlaps"):
        resolve_corporate_actions(
            cninfo, tushare, _reference_diagnostics(tushare, eligible=True)
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"cash_div": None, "cash_div_tax": 0.2},
        {"cash_div": 0.2, "cash_div_tax": None},
    ],
)
def test_one_missing_cash_field_is_ambiguous(updates) -> None:
    with pytest.raises(CorporateActionDataError, match="ambiguous"):
        canonical_implemented_actions(
            _frame(_row(**updates)),
            start_date="2021-01-01",
            end_date="2021-12-31",
        )


def test_tushare_base_diagnostics_do_not_change_per_share_action_identity() -> None:
    first = canonical_implemented_actions(
        _frame(_row(base_date="20201231", base_share=1000.0)),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    changed = canonical_implemented_actions(
        _frame(_row(base_date="20210131", base_share=2000.0)),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    assert first.action_id.tolist() == changed.action_id.tolist()


def test_tushare_input_order_and_exact_duplicates_do_not_change_action_id() -> None:
    first = _row(ann_date="20210301", imp_ann_date="20210516")
    second = _row(ann_date="20210401", imp_ann_date="20210517")
    one = canonical_implemented_actions(
        _frame(first, second),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    shuffled = canonical_implemented_actions(
        _frame(second, first, first),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    assert one.action_id.tolist() == shuffled.action_id.tolist()
