
import pandas as pd

from factor_lab.pit_asof import normalize_statement_dates, select_latest_statement_asof


def test_annual_report_not_visible_before_announcement_but_visible_after():
    statements = pd.DataFrame([
        {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "f_ann_date": "20240425", "net_profit": 100},
    ])
    normalized = normalize_statement_dates(statements)
    trades = pd.DataFrame([
        {"ts_code": "000001.SZ", "date": "2024-03-01"},
        {"ts_code": "000001.SZ", "date": "2024-05-01"},
    ])
    joined = select_latest_statement_asof(normalized, trades, source_fields=["net_profit"])
    assert joined.loc[0, "pit_validated"] is False or joined.loc[0, "pit_validated"] == False
    assert joined.loc[0, "pit_blocked_reason"] == "no_statement_asof"
    assert joined.loc[1, "pit_validated"] is True or joined.loc[1, "pit_validated"] == True
    assert joined.loc[1, "net_profit"] == 100
    assert joined.loc[1, "source_end_date"] == "20231231"
    assert joined.loc[1, "source_ann_date"] == "20240425"


def test_missing_announcement_date_blocks():
    statements = pd.DataFrame([{"ts_code": "000001.SZ", "end_date": "20231231", "net_profit": 100}])
    normalized = normalize_statement_dates(statements)
    assert normalized.loc[0, "pit_validated"] is False or normalized.loc[0, "pit_validated"] == False
    assert normalized.loc[0, "pit_blocked_reason"] == "missing_announcement_date"


def test_duplicate_code_end_date_rows_are_flagged():
    statements = pd.DataFrame([
        {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240420", "net_profit": 90},
        {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "net_profit": 100},
    ])
    normalized = normalize_statement_dates(statements)
    trades = pd.DataFrame([{"ts_code": "000001.SZ", "date": "2024-05-01"}])
    joined = select_latest_statement_asof(normalized, trades, source_fields=["net_profit"])
    assert joined.loc[0, "net_profit"] == 100
    assert joined.loc[0, "duplicate_code_end_date_flag"] is True or joined.loc[0, "duplicate_code_end_date_flag"] == True
    assert joined.loc[0, "duplicate_history_count"] == 2


def test_f_ann_date_is_preferred_over_ann_date():
    statements = pd.DataFrame([
        {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240420", "f_ann_date": "20240510", "net_profit": 100},
    ])
    normalized = normalize_statement_dates(statements)
    trades = pd.DataFrame([{"ts_code": "000001.SZ", "date": "2024-04-25"}])
    joined = select_latest_statement_asof(normalized, trades, source_fields=["net_profit"])
    assert joined.loc[0, "pit_validated"] is False or joined.loc[0, "pit_validated"] == False


def test_never_selects_by_end_date_when_announcement_missing():
    statements = pd.DataFrame([
        {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": None, "net_profit": 100},
    ])
    normalized = normalize_statement_dates(statements)
    trades = pd.DataFrame([{"ts_code": "000001.SZ", "date": "2024-12-31"}])
    joined = select_latest_statement_asof(normalized, trades, source_fields=["net_profit"])
    assert joined.loc[0, "pit_validated"] is False or joined.loc[0, "pit_validated"] == False
    assert "net_profit" not in joined.columns or pd.isna(joined.loc[0, "net_profit"])


def test_valid_join_outputs_auditable_metadata():
    statements = pd.DataFrame([
        {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "net_profit": 100},
    ])
    trades = pd.DataFrame([{"ts_code": "000001.SZ", "date": "2024-05-01"}])
    joined = select_latest_statement_asof(statements, trades, source_table="income", source_fields=["net_profit"])
    assert joined.loc[0, "source_table"] == "income"
    assert joined.loc[0, "source_field"] == "net_profit"
    assert joined.loc[0, "source_end_date"] == "20231231"
    assert joined.loc[0, "source_ann_date"] == "20240425"
    assert joined.loc[0, "pit_validated"] is True or joined.loc[0, "pit_validated"] == True
    assert joined.loc[0, "pit_blocked_reason"] == ""
