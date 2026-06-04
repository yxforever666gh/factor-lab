# Autonomous Strategy Route Registry v2

decision_recommendation: run_field_resolution_for_top_candidate
top_route_id: industry_cycle_inflection_value_anchor_v1
controlled_execution_allowed: False
queue_write_allowed: False

| Priority | Route | Status | Data status | Next |
|---:|---|---|---|---|
| 1 | industry_cycle_inflection_value_anchor_v1 | field_resolution_candidate | derivable_from_available_market_history | run_field_resolution |
| 2 | balance_sheet_improvement_recovery_v1 | proxy_review_candidate | proxy_available_requires_review | run_proxy_field_resolution |
| 3 | earnings_revision_valuation_repair_v2 | request_data_candidate | request_data | write_data_request |
