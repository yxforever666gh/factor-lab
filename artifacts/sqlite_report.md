# SQLite Experiment Report

## Candidate Leaderboard

- hybrid_mom_20_liquidity_turnover_shock | family=momentum | status=fragile | evals=79 | windows=2 | avg=9.490097 | best=10.340405 | latest=10.072773 | pass_rate=0.9367 | next=run_robustness_validation
- hybrid_liquidity_turnover_shock_mom_20 | family=momentum | status=fragile | evals=39 | windows=2 | avg=8.892481 | best=10.340405 | latest=10.072773 | pass_rate=0.8718 | next=run_robustness_validation
- mom_20 | family=momentum | status=fragile | evals=200 | windows=5 | avg=1.726368 | best=10.128923 | latest=9.793076 | pass_rate=0.255 | next=run_robustness_validation
- liquidity_turnover_shock | family=liquidity | status=fragile | evals=200 | windows=5 | avg=2.119795 | best=9.938888 | latest=9.723041 | pass_rate=0.255 | next=run_robustness_validation
- mom_plus_value | family=momentum | status=fragile | evals=200 | windows=5 | avg=1.190673 | best=9.601585 | latest=9.601585 | pass_rate=0.25 | next=run_robustness_validation
- hybrid_mom_20_value_ep | family=momentum | status=fragile | evals=98 | windows=2 | avg=8.516041 | best=10.000145 | latest=9.400734 | pass_rate=0.9082 | next=run_robustness_validation
- value_ep | family=value | status=fragile | evals=200 | windows=5 | avg=2.377536 | best=9.099535 | latest=9.099535 | pass_rate=0.25 | next=run_robustness_validation
- size_small | family=other | status=fragile | evals=200 | windows=5 | avg=2.600241 | best=9.015223 | latest=9.015223 | pass_rate=0.0 | next=run_robustness_validation
- hybrid_size_small_value_bp | family=value | status=fragile | evals=35 | windows=2 | avg=8.186928 | best=8.709162 | latest=8.689527 | pass_rate=0.0 | next=run_robustness_validation
- value_bp | family=value | status=fragile | evals=200 | windows=5 | avg=1.087484 | best=7.432826 | latest=7.432826 | pass_rate=0.245 | next=run_robustness_validation

## Candidate Families

- momentum | candidates=5 | promising=0 | testing=5 | rejected=0 | avg_candidate=5.963132 | avg_latest=9.788188 | evals=616 | windows=16
- liquidity | candidates=1 | promising=0 | testing=1 | rejected=0 | avg_candidate=2.119795 | avg_latest=9.723041 | evals=200 | windows=5
- other | candidates=1 | promising=0 | testing=1 | rejected=0 | avg_candidate=2.600241 | avg_latest=9.015223 | evals=200 | windows=5
- value | candidates=4 | promising=0 | testing=4 | rejected=0 | avg_candidate=4.551218 | avg_latest=6.646145 | evals=438 | windows=13

## Candidate Relationship Pairs

- mom_plus_value <-> mom_20 | type=high_corr | strength=1.0 | run_id=a7e9c7d8-a2ac-45d7-86a6-5011370b66da
- hybrid_mom_20_liquidity_turnover_shock <-> liquidity_turnover_shock | type=high_corr | strength=1.0 | run_id=9ef112b7-0226-452b-84c6-e09472cb1e95
- hybrid_mom_20_value_ep <-> mom_20 | type=high_corr | strength=1.0 | run_id=238da830-8d4f-45f9-8606-d6b280ece4ed
- hybrid_liquidity_turnover_shock_mom_20 <-> liquidity_turnover_shock | type=high_corr | strength=1.0 | run_id=fa2b1ba8-bfae-4f1a-a855-3108f7eabafa
- hybrid_liquidity_turnover_shock_value_ep <-> liquidity_turnover_shock | type=high_corr | strength=1.0 | run_id=6020e4bb-ba0d-440c-9281-de3e148bac57
- hybrid_liquidity_turnover_shock_value_ep <-> hybrid_liquidity_turnover_shock_mom_20 | type=refinement_of | strength=0.92 | run_id=None
- hybrid_liquidity_turnover_shock_value_ep <-> hybrid_mom_20_liquidity_turnover_shock | type=refinement_of | strength=0.92 | run_id=None
- hybrid_size_small_value_bp <-> size_small | type=refinement_of | strength=0.92 | run_id=None
- hybrid_mom_20_liquidity_turnover_shock <-> hybrid_liquidity_turnover_shock_mom_20 | type=duplicate_of | strength=0.9 | run_id=None
- hybrid_mom_20_liquidity_turnover_shock <-> liquidity_turnover_shock | type=refinement_of | strength=0.82 | run_id=9ef112b7-0226-452b-84c6-e09472cb1e95
- hybrid_liquidity_turnover_shock_mom_20 <-> liquidity_turnover_shock | type=refinement_of | strength=0.82 | run_id=fa2b1ba8-bfae-4f1a-a855-3108f7eabafa
- hybrid_mom_20_value_ep <-> hybrid_liquidity_turnover_shock_value_ep | type=refinement_of | strength=0.82 | run_id=None

## Top Factors by Average Score

- hybrid_mom_20_liquidity_turnover_shock | avg_score=1.343860 | runs=79
- hybrid_liquidity_turnover_shock_mom_20 | avg_score=1.265618 | runs=39
- hybrid_mom_20_value_ep | avg_score=1.054826 | runs=98
- hybrid_liquidity_turnover_shock_value_ep | avg_score=0.985774 | runs=3
- hybrid_size_small_value_bp | avg_score=0.511823 | runs=35
- liquidity_turnover_shock | avg_score=0.258676 | runs=3610
- value_ep | avg_score=0.232306 | runs=3529
- size_small | avg_score=0.077083 | runs=3448
- mom_20 | avg_score=-0.000085 | runs=3730
- value_bp | avg_score=-0.228718 | runs=3383

## Stable Candidates

- mom_20 | candidate_runs=1071
- liquidity_turnover_shock | candidate_runs=964
- mom_plus_value | candidate_runs=803
- hybrid_mom_20_value_ep | candidate_runs=89
- hybrid_mom_20_liquidity_turnover_shock | candidate_runs=74
- hybrid_liquidity_turnover_shock_mom_20 | candidate_runs=34
- value_bp | candidate_runs=23
- hybrid_liquidity_turnover_shock_value_ep | candidate_runs=2

## Portfolio Strategy Averages

- long_short_top_bottom_candidates_only | avg_sharpe=7.100386 | avg_return=1.365066 | runs=1120
- long_short_top_bottom_all_factors | avg_sharpe=3.333099 | avg_return=0.559266 | runs=3915
- long_short_top_bottom_cluster_representatives | avg_sharpe=2.775355 | avg_return=0.414127 | runs=3915
- long_short_top_bottom_candidates_only_neutralized | avg_sharpe=2.170886 | avg_return=0.342567 | runs=1120
- long_short_top_bottom_neutralized | avg_sharpe=-0.117345 | avg_return=-0.034285 | runs=3915
- long_short_top_bottom_cluster_representatives_neutralized | avg_sharpe=-0.500860 | avg_return=-0.124731 | runs=3915