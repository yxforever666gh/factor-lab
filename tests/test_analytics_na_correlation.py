import pandas as pd

from factor_lab.analytics import factor_correlation_matrix


def test_factor_correlation_matrix_coerces_pd_na_values():
    frame = pd.DataFrame({"factor_a": [1.0, pd.NA, 3.0], "factor_b": [2.0, 3.0, 4.0]})

    corr = factor_correlation_matrix(factor_value_frame=frame)

    assert list(corr.columns) == ["factor_a", "factor_b"]
