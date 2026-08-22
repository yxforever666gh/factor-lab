from __future__ import annotations

import numpy as np
import pytest

from factor_lab.research_os.statistics import (
    block_bootstrap,
    circular_block_bootstrap_samples,
    deflated_sharpe_ratio,
    dependence_adjusted_online_alpha,
    holm_adjust,
    holm_rejections,
)


def test_circular_block_bootstrap_is_deterministic_and_preserves_sample_shape() -> None:
    values = np.arange(20, dtype=float)
    first = circular_block_bootstrap_samples(values, block_size=4, resamples=8, seed=17)
    second = circular_block_bootstrap_samples(values, block_size=4, resamples=8, seed=17)
    assert first.shape == (8, 20)
    np.testing.assert_array_equal(first, second)
    assert set(np.unique(first)).issubset(set(values))
    for row in first:
        for start in range(0, 20, 4):
            block = row[start : start + 4]
            if len(block) > 1:
                assert np.all((np.diff(block) % len(values)) == 1)


def test_block_bootstrap_reports_positive_evidence_without_destroying_dependence() -> None:
    rng = np.random.default_rng(4)
    innovations = rng.normal(0.004, 0.006, 300)
    returns = np.empty_like(innovations)
    returns[0] = innovations[0]
    for index in range(1, len(returns)):
        returns[index] = 0.6 * returns[index - 1] + innovations[index]
    result = block_bootstrap(returns, block_size=12, resamples=500, seed=5)
    assert result.observed > 0
    assert result.confidence_interval[0] > 0
    assert result.probability_positive > 0.99
    assert result.one_sided_p_value <= 2 / 501


def test_deflated_sharpe_penalizes_the_complete_trial_family() -> None:
    rng = np.random.default_rng(8)
    returns = rng.normal(0.004, 0.02, 260)
    one_trial = deflated_sharpe_ratio(returns, number_of_trials=1)
    many_trials = deflated_sharpe_ratio(
        returns,
        number_of_trials=50,
        trial_sharpes=np.linspace(-0.8, 1.6, 50).tolist(),
    )
    assert one_trial.expected_maximum_sharpe == 0
    assert many_trials.expected_maximum_sharpe > 0
    assert many_trials.deflated_sharpe_probability < one_trial.deflated_sharpe_probability
    assert 0 <= many_trials.deflated_sharpe_probability <= 1


def test_holm_adjustment_is_step_down_and_order_stable() -> None:
    adjusted = holm_adjust([0.01, 0.04, 0.03])
    assert adjusted == pytest.approx((0.03, 0.06, 0.06))
    assert holm_rejections([0.01, 0.04, 0.03], alpha=0.05) == (True, False, False)


def test_dependence_adjusted_online_alpha_never_replenishes_budget() -> None:
    decisions = dependence_adjusted_online_alpha(
        [1e-8, 0.001, 0.02, 0.5],
        family_alpha_budget=0.10,
        maximum_family_trials=24,
    )
    assert decisions[0].rejected
    assert [item.alpha for item in decisions] == sorted((item.alpha for item in decisions), reverse=True)
    assert decisions[-1].cumulative_alpha_spent <= 0.10
    assert decisions[-1].remaining_alpha_budget >= 0
    assert decisions[1].alpha == pytest.approx(
        dependence_adjusted_online_alpha([0.5, 0.5], family_alpha_budget=0.10, maximum_family_trials=24)[1].alpha
    )


@pytest.mark.parametrize("p_values", [[-0.1], [1.1], [float("nan")]])
def test_multiple_testing_rejects_invalid_p_values(p_values: list[float]) -> None:
    with pytest.raises(ValueError):
        holm_adjust(p_values)
    with pytest.raises(ValueError):
        dependence_adjusted_online_alpha(p_values)

