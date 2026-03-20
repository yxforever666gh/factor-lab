# SQLite Experiment Report

## Candidate Leaderboard

- mom_20 | family=momentum | status=promising | evals=187 | windows=9 | avg=8.524486 | best=9.471187 | latest=4.865683 | pass_rate=0.9135 | next=refine
- quality_roe | family=quality | status=testing | evals=2 | windows=1 | avg=4.41182 | best=4.41182 | latest=4.41182 | pass_rate=1.0 | next=validate_more_windows
- quality_minus_value | family=value | status=testing | evals=2 | windows=1 | avg=4.197889 | best=4.197889 | latest=4.197889 | pass_rate=1.0 | next=validate_more_windows
- value_ep | family=value | status=testing | evals=147 | windows=9 | avg=7.491671 | best=8.593484 | latest=4.071135 | pass_rate=0.0 | next=validate_more_windows
- size_small | family=other | status=testing | evals=185 | windows=8 | avg=7.989338 | best=8.747369 | latest=3.025676 | pass_rate=0.0 | next=validate_more_windows
- liquidity_turnover_shock | family=liquidity | status=testing | evals=187 | windows=9 | avg=7.969937 | best=8.835956 | latest=2.364005 | pass_rate=0.0 | next=validate_more_windows
- value_bp | family=value | status=promising | evals=145 | windows=8 | avg=5.910476 | best=8.956324 | latest=1.311582 | pass_rate=0.8897 | next=refine
- mom_plus_value | family=momentum | status=promising | evals=185 | windows=8 | avg=8.11212 | best=9.055424 | latest=1.144852 | pass_rate=0.9135 | next=refine

## Candidate Families

- quality | candidates=1 | promising=0 | testing=1 | rejected=0 | avg_candidate=4.41182 | avg_latest=4.41182 | evals=2 | windows=1
- value | candidates=3 | promising=1 | testing=2 | rejected=0 | avg_candidate=5.866679 | avg_latest=3.193535 | evals=294 | windows=18
- other | candidates=1 | promising=0 | testing=1 | rejected=0 | avg_candidate=7.989338 | avg_latest=3.025676 | evals=185 | windows=8
- momentum | candidates=2 | promising=2 | testing=0 | rejected=0 | avg_candidate=8.318303 | avg_latest=3.005268 | evals=372 | windows=17
- liquidity | candidates=1 | promising=0 | testing=1 | rejected=0 | avg_candidate=7.969937 | avg_latest=2.364005 | evals=187 | windows=9

## Candidate Relationship Pairs


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