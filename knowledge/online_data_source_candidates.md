# Online Data Source Preflight

Scope: bounded API sample only. No factor run, no queue write, no daemon start, no full-market pull.

## Final decision
- Decision: `proceed_margin_mvp`
- Reason: 融资融券接口可用，且具备可审计日期字段，适合支撑 low-crowding 机制 MVP。
- Selected source: tushare / margin_financing / 融资融券

## Candidate ranking
| rank | provider | source | recommendation | score | pit/date control | frequency | rows | endpoint | blockers |
|---:|---|---|---|---:|---|---|---:|---|---|
| 1 | tushare | 融资融券 | mvp_candidate | 100 | trade_date_observable | daily | 3841 | margin_detail |  |
| 2 | tushare | 机构持仓 | mvp_candidate | 90 | announcement_date_pit | quarterly | 40 | top10_floatholders |  |
| 3 | tushare | 龙虎榜 | mvp_candidate | 85 | trade_date_observable | daily_event | 79 | top_list |  |
| 4 | tushare | 大宗交易 | mvp_candidate | 85 | trade_date_observable | daily_event | 149 | block_trade |  |
| 5 | tushare | 业绩快报 | secondary_candidate | 75 | announcement_date_pit | event | 1608 | express |  |
| 6 | tushare | 回购 | secondary_candidate | 75 | announcement_date_pit | event | 2000 | repurchase |  |
| 7 | tushare | 高管/股东增减持 | secondary_candidate | 75 | announcement_date_pit | event | 3000 | stk_holdertrade |  |
| 8 | tushare | 股东户数 | secondary_candidate | 70 | announcement_date_pit | quarterly_or_event | 5 | stk_holdernumber |  |
| 9 | tushare | 业绩预告 | monitor_or_later | 57 | announcement_date_pit | event | 7 | forecast |  |
| 10 | tushare | 质押 | manual_review_before_research | 50 | end_date_only_not_pit_safe | event_or_periodic | 1058 | pledge_stat | not_pit_safe |
| 11 | tushare | 分析师预期 | manual_review_before_research | 50 | date_available_needs_review | event_or_periodic | 5000 | report_rc | not_pit_safe |
| 12 | diemeng | 融资融券 | blocked_no_access_or_no_rows | 0 | unknown | daily | 0 | /stock/margin_detail | no_successful_endpoint |
| 13 | diemeng | 股东户数 | blocked_no_access_or_no_rows | 0 | unknown | quarterly_or_event | 0 | /stock/stk_holdernumber | no_successful_endpoint |
| 14 | diemeng | 机构持仓 | blocked_no_access_or_no_rows | 0 | unknown | quarterly | 0 | /stock/top10_holders | no_successful_endpoint |
| 15 | diemeng | 业绩预告 | blocked_no_access_or_no_rows | 0 | unknown | event | 0 | /stock/forecast | no_successful_endpoint |
| 16 | diemeng | 业绩快报 | blocked_no_access_or_no_rows | 0 | unknown | event | 0 | /stock/express | no_successful_endpoint |
| 17 | diemeng | 龙虎榜 | blocked_no_access_or_no_rows | 0 | unknown | daily_event | 0 | /stock/top_list | no_successful_endpoint |
| 18 | diemeng | 大宗交易 | blocked_no_access_or_no_rows | 0 | unknown | daily_event | 0 | /stock/block_trade | no_successful_endpoint |
| 19 | diemeng | 回购 | blocked_no_access_or_no_rows | 0 | unknown | event | 0 | /stock/repurchase | no_successful_endpoint |
| 20 | diemeng | 质押 | blocked_no_access_or_no_rows | 0 | unknown | event_or_periodic | 0 | /stock/pledge_stat | no_successful_endpoint |
| 21 | diemeng | 高管/股东增减持 | blocked_no_access_or_no_rows | 0 | unknown | event | 0 | /stock/stk_holdertrade | no_successful_endpoint |
| 22 | diemeng | 分析师预期 | blocked_no_access_or_no_rows | 0 | unknown | event_or_periodic | 0 | /stock/report_rc | no_successful_endpoint |

## Interpretation
- `mvp_candidate` means the source returned sample rows and has a trade_date or announcement-date control suitable for a small MVP.
- `manual_review_before_research` means there are rows but the date/PIT control is not enough for immediate factor research.
- `blocked_no_access_or_no_rows` means this bounded probe could not obtain usable rows; it may still be available with different params or permissions, but should not be assumed usable.
