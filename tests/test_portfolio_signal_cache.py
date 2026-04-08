import pandas as pd

from factor_lab.factors import FactorDefinition
from factor_lab.portfolio import build_composite_factor


def _frame():
    rows = []
    for d in range(3):
        for i in range(5):
            rows.append(
                {
                    "date": pd.Timestamp("2026-01-01") + pd.Timedelta(days=d),
                    "ticker": f"T{i}",
                    "forward_return_5d": float(i) / 100,
                    "momentum_20": float(i + d),
                }
            )
    return pd.DataFrame(rows)


def test_build_composite_factor_accepts_factor_value_cache():
    frame = _frame()
    defs = [FactorDefinition(name="mom_20", expression="momentum_20")]
    cache = {"mom_20": frame["momentum_20"]}

    signal = build_composite_factor(frame, defs, factor_value_cache=cache)

    assert len(signal) == len(frame)
    assert signal.notna().all()
