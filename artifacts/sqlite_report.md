# SQLite Experiment Report

## Candidate Leaderboard

- hybrid_mom_20_liquidity_turnover_shock | family=momentum | status=fragile | evals=79 | windows=2 | avg=9.490097 | best=10.340405 | latest=10.072773 | pass_rate=0.9367 | next=run_robustness_validation
- hybrid_liquidity_turnover_shock_mom_20 | family=momentum | status=fragile | evals=39 | windows=2 | avg=8.892481 | best=10.340405 | latest=10.072773 | pass_rate=0.8718 | next=run_robustness_validation
- hybrid_mom_20_value_ep | family=momentum | status=fragile | evals=98 | windows=2 | avg=8.516041 | best=10.000145 | latest=9.400734 | pass_rate=0.9082 | next=run_robustness_validation
- hybrid_size_small_value_bp | family=value | status=fragile | evals=35 | windows=2 | avg=8.186928 | best=8.709162 | latest=8.689527 | pass_rate=0.0 | next=run_robustness_validation
- size_small | family=other | status=fragile | evals=200 | windows=5 | avg=2.57859 | best=9.015223 | latest=1.821213 | pass_rate=0.0 | next=run_robustness_validation
- value_ep | family=value | status=fragile | evals=200 | windows=5 | avg=2.341134 | best=9.099535 | latest=1.818988 | pass_rate=0.245 | next=run_robustness_validation
- hybrid_liquidity_turnover_shock_value_ep | family=value | status=fragile | evals=3 | windows=1 | avg=6.552922 | best=9.377971 | latest=1.362693 | pass_rate=0.6667 | next=run_robustness_validation
- liquidity_turnover_shock | family=liquidity | status=fragile | evals=200 | windows=5 | avg=2.119795 | best=9.938888 | latest=0.384066 | pass_rate=0.255 | next=run_robustness_validation
- mom_20 | family=momentum | status=fragile | evals=200 | windows=5 | avg=1.726368 | best=10.128923 | latest=0.186667 | pass_rate=0.255 | next=run_robustness_validation
- value_bp | family=value | status=fragile | evals=200 | windows=5 | avg=1.087484 | best=7.432826 | latest=-0.812508 | pass_rate=0.245 | next=run_robustness_validation

## Candidate Families

- momentum | candidates=5 | promising=0 | testing=5 | rejected=0 | avg_candidate=5.952596 | avg_latest=5.759734 | evals=616 | windows=16
- value | candidates=4 | promising=0 | testing=4 | rejected=0 | avg_candidate=4.542117 | avg_latest=2.764675 | evals=438 | windows=13
- other | candidates=1 | promising=0 | testing=1 | rejected=0 | avg_candidate=2.57859 | avg_latest=1.821213 | evals=200 | windows=5
- liquidity | candidates=1 | promising=0 | testing=1 | rejected=0 | avg_candidate=2.119795 | avg_latest=0.384066 | evals=200 | windows=5

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
- liquidity_turnover_shock | avg_score=0.258373 | runs=3612
- value_ep | avg_score=0.232343 | runs=3531
- size_small | avg_score=0.077029 | runs=3450
- mom_20 | avg_score=-0.000335 | runs=3732
- value_bp | avg_score=-0.228696 | runs=3385

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
- long_short_top_bottom_all_factors | avg_sharpe=3.331906 | avg_return=0.559127 | runs=3917
- long_short_top_bottom_cluster_representatives | avg_sharpe=2.774140 | avg_return=0.413971 | runs=3917
- long_short_top_bottom_candidates_only_neutralized | avg_sharpe=2.170886 | avg_return=0.342567 | runs=1120
- long_short_top_bottom_neutralized | avg_sharpe=-0.117266 | avg_return=-0.034281 | runs=3917
- long_short_top_bottom_cluster_representatives_neutralized | avg_sharpe=-0.500729 | avg_return=-0.124716 | runs=3917