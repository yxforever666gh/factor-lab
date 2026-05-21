# Factor Lab Small Institutionalization Status

Generated: 2026-05-21T11:32:36.493326+00:00
Phase: A_baseline
Strategy mode: long_only_equity_enhancement
Decision: ready_for_portfolio_mvp
Next action: repair_simulated_portfolio_construction

## Blockers
- none

## Runtime safety
- Safe: True
- Recommendations: ['pause_broad_daemon', 'allow_controlled_only_daemon']
- Would-run count: 0

## Value sleeve
- Decision: collapse_to_value_sleeve_with_primary_route
- Primary route: value_quality_no_distress
- Confirmation route: value_momentum_confirmation

## Paper portfolio
- Ready: True
- Strategy: small_institutional_value_sleeve_mvp
- As-of date: 2021-12-28
- Position count: 72
- Benchmark ID: CSI1000
- One-way turnover estimate: 0.0
- Estimated round-trip cost: 0.0

## Paper monitoring
- Weekly report status: ready
- Cadence: weekly
- Runtime safe: True
- Missing artifacts: []

## Retrospective tracking
- Tracking status: ok
- Portfolio forward return: 0.01842
- Matched position count: 72
- Missing position count: 0

## Portfolio constraint hardening
- Constraint status: pass
- Violations: []
- Warnings: []

## Paper/live promotion readiness
- Readiness status: ready_for_manual_approval
- Blockers: []
- Warnings: []
- Manual approval required: True
- Live trading enabled: False

## Small institutional simulation
- Diagnosis status: blocked
- Primary issue: drawdown_risk_too_high
- Severity: high
- Recommended run mode: bounded_matrix
- Automation allowed: False

## Simulated portfolio construction repair
- Repair status: blocked_no_drawdown_safe_candidate
- Candidate count: 0
- Recommended candidate: None
- Best available max drawdown: -0.478256
- Drawdown gap to limit: 0.128256
- Automation allowed: False

## Next phase policy
- Target holdings: 50-100
- Benchmark candidates: ['CSI500', 'CSI1000', 'custom_mid_small_cap_universe']
- Rebalance candidates: ['monthly', 'biweekly']
