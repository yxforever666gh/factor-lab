from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.data.corporate_actions import (
    CNINFO_ACTION_COLUMNS,
    DIVIDEND_FIELDS,
)


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-13.0-profit-first.py"


def _module():
    spec = importlib.util.spec_from_file_location("factor_lab_13_0_runner", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Client:
    def __init__(self):
        self.calls = []

    def query(self, endpoint, **kwargs):
        self.calls.append((endpoint, kwargs))
        if kwargs["ex_date"] != "20210105":
            return pd.DataFrame(columns=DIVIDEND_FIELDS)
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "end_date": "20201231",
                    "ann_date": "20210101",
                    "div_proc": "实施",
                    "stk_div": 0.0,
                    "stk_bo_rate": 0.0,
                    "stk_co_rate": 0.0,
                    "cash_div": 0.1,
                    "cash_div_tax": 0.1,
                    "record_date": "20210104",
                    "ex_date": "20210105",
                    "pay_date": "20210105",
                    "div_listdate": None,
                    "imp_ann_date": "20210102",
                    "base_date": "20201231",
                    "base_share": 1000.0,
                },
                {
                    "ts_code": "600000.SH",
                    "end_date": "20201231",
                    "ann_date": "20210101",
                    "div_proc": "实施",
                    "stk_div": 0.0,
                    "stk_bo_rate": 0.0,
                    "stk_co_rate": 0.0,
                    "cash_div": 0.1,
                    "cash_div_tax": 0.1,
                    "record_date": "20210104",
                    "ex_date": "20210105",
                    "pay_date": "20210105",
                    "div_listdate": None,
                    "imp_ann_date": "20210102",
                    "base_date": "20201231",
                    "base_share": 1000.0,
                },
            ],
            columns=DIVIDEND_FIELDS,
        )


def test_tushare_capture_queries_exact_sessions_and_filters_scope() -> None:
    module = _module()
    client = _Client()
    sessions = pd.to_datetime(["2021-01-04", "2021-01-05"])
    result, receipt = module._capture_tushare_sample(
        ["000001.SZ"], sessions, client
    )
    assert len(client.calls) == 2
    assert receipt["request_count"] == 2
    assert receipt["provider_row_count"] == 2
    assert result.ts_code.tolist() == ["000001.SZ"]


def test_protocol_and_scope_constants_are_frozen() -> None:
    module = _module()
    protocol = module._read_protocol()
    assert protocol["payload_sha256"] == module.PROTOCOL_PAYLOAD_SHA256
    assert module.ALL_SCOPE_COUNT == 2252
    assert module.START_DATE == pd.Timestamp("2017-10-09")
    assert module.END_DATE == pd.Timestamp("2023-01-03")
    assert tuple(module.ACTION_ARTIFACT_ROLES) == (
        "scope",
        "cninfo_first",
        "cninfo_second",
        "tushare_first",
        "tushare_second",
        "cninfo_actions",
        "tushare_actions",
        "tushare_reference_diagnostics",
        "resolved_actions",
    )
    assert [
        value["label"] for value in module.REANCHOR_SOURCE_IDENTITIES
    ] == ["candidate-actions-v2", "candidate-actions-v3"]


def test_candidate_reanchor_requires_both_pinned_sources(tmp_path: Path) -> None:
    module = _module()
    with pytest.raises(ValueError, match="both pinned source captures"):
        module.reanchor_candidate_actions(
            tmp_path / "actions", source_roots=()
        )


class _ReferenceStore:
    market_sessions = tuple(pd.to_datetime(["2021-05-19", "2021-05-20"]))

    def __init__(self, *, factor_jump: bool = True):
        self.factor_jump = factor_jump

    def read_market(self, session):
        date = pd.Timestamp(session)
        if date == pd.Timestamp("2021-05-19"):
            values = {
                "ts_code": "000001.SZ",
                "close": 10.2,
                "pre_close": 10.0,
                "adj_factor": 1.0,
            }
        else:
            values = {
                "ts_code": "000001.SZ",
                "close": 10.0,
                "pre_close": 10.0,
                "adj_factor": 1.02 if self.factor_jump else 1.0,
            }
        return pd.DataFrame([values])


def _canonical_tushare_action(module):
    row = {
        "ts_code": "000001.SZ",
        "end_date": "20201231",
        "ann_date": "20210501",
        "div_proc": "实施",
        "stk_div": 0.0,
        "stk_bo_rate": 0.0,
        "stk_co_rate": 0.0,
        "cash_div": 0.2,
        "cash_div_tax": 0.2,
        "record_date": "20210519",
        "ex_date": "20210520",
        "pay_date": "20210520",
        "div_listdate": None,
        "imp_ann_date": "20210517",
        "base_date": "20201231",
        "base_share": 1000.0,
    }
    return module.canonical_implemented_actions(
        pd.DataFrame([row], columns=DIVIDEND_FIELDS),
        start_date="2021-01-01",
        end_date="2021-12-31",
    )


def test_raw_reference_fallback_requires_factor_jump_and_exact_reference() -> None:
    module = _module()
    actions = _canonical_tushare_action(module)
    cninfo = pd.DataFrame(columns=CNINFO_ACTION_COLUMNS)
    eligible = module._tushare_reference_diagnostics(
        cninfo, actions, store=_ReferenceStore(factor_jump=True)
    )
    assert eligible.fallback_eligible.tolist() == [True]
    assert eligible.status.tolist() == ["eligible_raw_reference_fallback"]
    no_jump = module._tushare_reference_diagnostics(
        cninfo, actions, store=_ReferenceStore(factor_jump=False)
    )
    assert no_jump.fallback_eligible.tolist() == [False]
    assert no_jump.status.tolist() == ["no_factor_jump"]
