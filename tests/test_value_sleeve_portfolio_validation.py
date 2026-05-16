
import pandas as pd

from factor_lab.value_sleeve_portfolio_validation import zscore_by_date, compose_sleeve_signal, normalize_weights


def test_zscore_per_date_mean_zero():
    f = pd.DataFrame({"date":["d1"]*3+["d2"]*3,"ticker":list("abcdef"),"forward_return_5d":[1,2,3,1,2,3],"factor_value":[1,2,3,10,11,12]})
    z = zscore_by_date(f)
    means = z.groupby("date")["normalized_factor_value"].mean().round(10).abs().tolist()
    assert means == [0.0, 0.0]


def test_weighted_composition_normalizes_weights():
    f1 = pd.DataFrame({"date":["d1"]*3,"ticker":["a","b","c"],"forward_return_5d":[1,2,3],"factor_value":[1,2,3]})
    f2 = pd.DataFrame({"date":["d1"]*3,"ticker":["a","b","c"],"forward_return_5d":[1,2,3],"factor_value":[3,2,1]})
    sleeve, meta = compose_sleeve_signal({"a":f1,"b":f2}, {"a":2,"b":1})
    assert meta["weights"] == normalize_weights({"a":2,"b":1})
    assert len(sleeve) == 3
