# Data Source Truth Audit

Generated: 2026-05-07T21:08:43.196821+00:00

Scope: read-only cache/schema audit. No network, no queue write, no daemon start.

## Summary
- usable: 33
- monitor_only: 1
- ambiguous_legacy: 4
- blocked: 13
- strict PIT fields: debt_to_asset, debt_to_assets, netprofit_yoy, operating_cashflow_to_profit, pit_feature_validated, pit_source_ann_date, pit_source_end_date, profit_yoy, revenue_yoy, tr_yoy

## Field truth table
| field | provider | category | PIT status | decision | date range | median date coverage | mechanism | notes |
|---|---|---|---|---|---|---:|---|---|
| analyst_forecast_revision | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| book_yield | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | value | derived only from contemporaneous/history market fields |
| buyback_amount | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| close | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | price history, return calculation | daily market/provider-observed field |
| debt_to_asset | tushare/cache | PIT financial | strict_pit | ambiguous_legacy | 2020-06-02..2023-12-28 | 1.0 | distress/leverage | PIT financial cache has validation marker and source announcement date; feature_schema still marks this as blocked/needs gated use |
| debt_to_assets | tushare/cache | PIT financial | strict_pit | usable | 2020-06-02..2023-12-28 | 1.0 | distress/leverage | PIT financial cache has validation marker and source announcement date |
| dividend_yield | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 0.7544 |  | daily market/provider-observed field |
| dragon_tiger_net_buy | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| earnings_express | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| earnings_preannouncement | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| earnings_yield | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | value | derived only from contemporaneous/history market fields |
| forward_return_5d | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | evaluation target only | daily market/provider-observed field |
| industry | tushare/cache | external candidate | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | daily market/provider-observed field |
| industry_relative_book_yield | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 0.6102 | industry-relative value | derived only from contemporaneous/history market fields |
| industry_relative_earnings_yield | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 0.6102 | industry-relative value | derived only from contemporaneous/history market fields |
| industry_relative_pb | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 0.6102 |  | derived only from contemporaneous/history market fields |
| industry_relative_pe | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 0.6102 |  | derived only from contemporaneous/history market fields |
| insider_trade_amount | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| institutional_holding_ratio | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| margin_financing_balance | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| momentum_120 | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | derived only from contemporaneous/history market fields |
| momentum_20 | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | derived only from contemporaneous/history market fields |
| momentum_60 | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | derived only from contemporaneous/history market fields |
| momentum_60_skip_5 | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | derived only from contemporaneous/history market fields |
| netprofit_yoy | tushare/cache | PIT financial | strict_pit | usable | 2020-06-02..2023-12-28 | 1.0 | profitability improvement | PIT financial cache has validation marker and source announcement date |
| news_sentiment | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| operating_cashflow_to_profit | tushare/cache | PIT financial | strict_pit | monitor_only | 2020-06-02..2023-12-28 | 1.0 | cash conversion, monitor-only cashflow quality | PIT financial cache has validation marker and source announcement date; feature_schema still marks this as blocked/needs gated use |
| order_book_imbalance | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| pb | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | market-implied valuation | daily market/provider-observed field |
| pe_ttm | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | market-implied valuation | daily market/provider-observed field |
| pit_feature_validated | tushare/cache | PIT financial | strict_pit | usable | 2020-06-02..2023-12-28 | 1.0 |  | PIT financial cache has validation marker and source announcement date |
| pit_source_ann_date | tushare/cache | PIT financial | strict_pit | usable | 2020-06-02..2023-12-28 | 1.0 |  | PIT financial cache has validation marker and source announcement date |
| pit_source_end_date | tushare/cache | PIT financial | strict_pit | usable | 2020-06-02..2023-12-28 | 1.0 |  | PIT financial cache has validation marker and source announcement date |
| pledge_ratio | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| profit_yoy | tushare/cache | PIT financial | strict_pit | ambiguous_legacy | 2020-06-02..2023-12-28 | 1.0 | profitability improvement | PIT financial cache has validation marker and source announcement date; feature_schema still marks this as blocked/needs gated use |
| ps_ttm | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | daily market/provider-observed field |
| ps_yield | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | derived only from contemporaneous/history market fields |
| return_1d | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | return/risk, momentum/reversal | daily market/provider-observed field |
| revenue_yoy | tushare/cache | PIT financial | strict_pit | ambiguous_legacy | 2020-06-02..2023-12-28 | 1.0 | revenue growth | PIT financial cache has validation marker and source announcement date; feature_schema still marks this as blocked/needs gated use |
| roe | tushare/cache | legacy derived | legacy_ambiguous | ambiguous_legacy | 2020-06-02..2023-12-28 | 1.0 |  | legacy ROE is derived from PE/PB unless PIT provenance is explicitly present |
| roe_delta | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | derived only from contemporaneous/history market fields |
| roe_yoy | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 0.9464 |  | derived only from contemporaneous/history market fields |
| shareholder_count | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| short_selling_balance | external_candidate | external candidate | blocked | blocked_missing_field | .. | None |  | field not present in inspected cache/schema |
| size_inv | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | derived only from contemporaneous/history market fields |
| total_mv | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 |  | daily market/provider-observed field |
| tr_yoy | tushare/cache | PIT financial | strict_pit | usable | 2020-06-02..2023-12-28 | 1.0 | revenue growth | PIT financial cache has validation marker and source announcement date |
| turnover | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | liquidity, weak crowding proxy | daily market/provider-observed field |
| turnover_shock_5_20 | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | attention/liquidity shock, weak crowding proxy | derived only from contemporaneous/history market fields |
| volatility_20 | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | risk control, crowding proxy | derived only from contemporaneous/history market fields |
| volatility_60 | tushare/cache | price/volume/valuation | market_daily_observed | usable | 2020-06-02..2023-12-28 | 1.0 | risk control, crowding proxy | derived only from contemporaneous/history market fields |

## Interpretation
- Existing cache mainly supports market/valuation/liquidity fields plus a limited PIT financial slice.
- True crowding/ownership/analyst/event/microstructure fields are absent and must be treated as data-source expansion candidates, not usable factors.
- Financial fields should remain gated by PIT provenance and prior route closure evidence; PIT safety is not alpha evidence.
