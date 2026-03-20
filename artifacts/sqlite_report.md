# SQLite Experiment Report

## Candidate Leaderboard

- mom_20 | family=momentum | status=promising | evals=187 | windows=9 | avg=8.471138 | best=74.55231 | latest=74.55231 | pass_rate=0.9144 | next=refine
- quality_roe | family=quality | status=promising | evals=2 | windows=1 | avg=73.57363 | best=73.57363 | latest=73.57363 | pass_rate=1.0 | next=refine
- quality_minus_value | family=value | status=promising | evals=2 | windows=1 | avg=73.12289 | best=73.12289 | latest=73.12289 | pass_rate=1.0 | next=refine
- value_ep | family=value | status=testing | evals=147 | windows=9 | avg=7.86276 | best=72.849566 | latest=72.849566 | pass_rate=0.0136 | next=validate_more_windows
- liquidity_turnover_shock | family=liquidity | status=testing | evals=187 | windows=9 | avg=8.006394 | best=68.96729 | latest=68.96729 | pass_rate=0.0 | next=validate_more_windows
- size_small | family=other | status=testing | evals=185 | windows=8 | avg=7.358002 | best=15.237325 | latest=2.607648 | pass_rate=0.0 | next=validate_more_windows
- value_bp | family=value | status=promising | evals=145 | windows=8 | avg=5.644497 | best=16.097857 | latest=1.17126 | pass_rate=0.8897 | next=refine
- mom_plus_value | family=momentum | status=promising | evals=185 | windows=8 | avg=7.414127 | best=15.557449 | latest=1.033596 | pass_rate=0.9135 | next=refine

## Top Factors by Average Score

- quality_roe | avg_score=4.512202 | runs=2
- quality_minus_value | avg_score=3.606423 | runs=2
- mom_20 | avg_score=1.386701 | runs=187
- mom_plus_value | avg_score=1.032017 | runs=185
- size_small | avg_score=0.472745 | runs=185
- liquidity_turnover_shock | avg_score=0.470518 | runs=187
- value_ep | avg_score=0.399132 | runs=147
- value_bp | avg_score=-0.226092 | runs=145

## Stable Candidates

- mom_20 | candidate_runs=172
- mom_plus_value | candidate_runs=170
- value_ep | candidate_runs=3
- quality_minus_value | candidate_runs=2
- quality_roe | candidate_runs=2
- value_bp | candidate_runs=1

## Portfolio Strategy Averages

- long_short_top_bottom_all_factors | avg_sharpe=10.078685 | avg_return=1.865463 | runs=187
- long_short_top_bottom_cluster_representatives | avg_sharpe=9.636109 | avg_return=1.730556 | runs=187
- long_short_top_bottom_candidates_only | avg_sharpe=5.827967 | avg_return=1.607481 | runs=172
- long_short_top_bottom_cluster_representatives_neutralized | avg_sharpe=1.277386 | avg_return=0.178122 | runs=185
- long_short_top_bottom_candidates_only_neutralized | avg_sharpe=0.934032 | avg_return=0.163628 | runs=170
- long_short_top_bottom_neutralized | avg_sharpe=0.272130 | avg_return=0.030461 | runs=185