# Factor Lab script entrypoints and namespace plan

Generated/updated: 2026-06-02

## Policy

- Top-level scripts remain the compatibility surface for documented commands, systemd units, and tests.
- New scripts should be placed under `scripts/prod/`, `scripts/ops/`, `scripts/reports/`, or `scripts/devtools/` according to role.
- Move existing scripts one family at a time only after caller/test reference checks; keep wrappers at old top-level paths when referenced.
- `scripts/archive/` is for historical one-offs after explicit caller-map confirmation; do not direct-delete scripts.

## Namespace directories

- `scripts/prod/` — Stable runtime entrypoints used by humans or systemd.
- `scripts/ops/` — Audits, inspectors, dry-runs, inventory, and operational status tools.
- `scripts/reports/` — Report/materializer scripts, primarily `write_*.py`.
- `scripts/devtools/` — Probes, smoke tests, diagnostics, and throwaway helpers.
- `scripts/archive/` — Historical scripts after caller-map confirmation only.

## Stable top-level compatibility entrypoints

- `scripts/audit_runtime_takeover.py`
- `scripts/dry_run_controlled_restart.py`
- `scripts/prepare_bucket_aware_tasks.py`
- `scripts/run_ab_harness.py`
- `scripts/run_autonomous_research_cycle.py`
- `scripts/run_autonomous_research_loop_once.py`
- `scripts/run_autonomous_strategy_lab.py`
- `scripts/run_autonomous_strategy_lab_controlled.py`
- `scripts/run_controlled_admission_feeder.py`
- `scripts/run_controlled_orchestrator_once.py`
- `scripts/run_earnings_test.py`
- `scripts/run_first_workflow.py`
- `scripts/run_generated_batch_from_llm.py`
- `scripts/run_harvest_agent_once.py`
- `scripts/run_harvest_autonomous_research_controller.py`
- `scripts/run_harvest_cycle.py`
- `scripts/run_harvest_evolution_loop.py`
- `scripts/run_harvest_strategy_governor.py`
- `scripts/run_hermes_briefings.py`
- `scripts/run_llm_bridge_prepare.py`
- `scripts/run_llm_cycle.py`
- `scripts/run_llm_decision_ab.py`
- `scripts/run_p0_pit_data_preflight.py`
- `scripts/run_post_h_controlled_restart_acceptance.py`
- `scripts/run_research_daemon.py`
- `scripts/run_research_orchestrator.py`
- `scripts/run_research_task_worker.py`
- `scripts/run_robustness_batch.py`
- `scripts/run_scheduled_cycle.py`
- `scripts/run_small_institutional_risk_reduction_executor.py`
- `scripts/run_turnover_test.py`
- `scripts/run_tushare_batch.py`
- `scripts/run_tushare_workflow.py`
- `scripts/run_web_ui.py`

## Candidate groups

### ops_tool_candidate (14)

- `scripts/audit_hermes_vocabulary.py`
- `scripts/audit_research_daemon_systemd_env.py`
- `scripts/audit_research_waste.py`
- `scripts/check_and_import_llm_bridge.py`
- `scripts/check_autonomous_research_cycle_gate.py`
- `scripts/check_candidate_codegen_policy.py`
- `scripts/check_factor_lab_llm_provider.py`
- `scripts/check_harvest_gate.py`
- `scripts/check_research_gate.py`
- `scripts/check_workflow_codegen_policy.py`
- `scripts/inspect_harvest_controller_status.py`
- `scripts/inspect_harvest_strategy_status.py`
- `scripts/inspect_harvest_v3_status.py`
- `scripts/inspect_hermes_correction_status.py`

### report_writer_candidate (76)

- `scripts/write_autonomous_alpha_restart.py`
- `scripts/write_autonomous_research_cycle_plan.py`
- `scripts/write_autonomous_research_evidence.py`
- `scripts/write_autonomous_research_loop_report.py`
- `scripts/write_autonomous_research_verdict.py`
- `scripts/write_autonomous_strategy_cheap_screen_plan.py`
- `scripts/write_autonomous_strategy_coverage_preflight.py`
- `scripts/write_autonomous_strategy_data_request_report.py`
- `scripts/write_autonomous_strategy_field_derivations.py`
- `scripts/write_autonomous_strategy_field_resolution.py`
- `scripts/write_autonomous_strategy_lab_report.py`
- `scripts/write_autonomous_strategy_routes.py`
- `scripts/write_autonomous_strategy_worker_verdict.py`
- `scripts/write_controlled_route_policy.py`
- `scripts/write_controlled_run_ledger.py`
- `scripts/write_data_source_truth_audit.py`
- `scripts/write_dragon_tiger_source_mvp.py`
- `scripts/write_earnings_event_admission_dry_run.py`
- `scripts/write_earnings_event_failure_diagnosis.py`
- `scripts/write_earnings_event_source_mvp.py`
- `scripts/write_harvest_cycle_plan.py`
- `scripts/write_harvest_evidence.py`
- `scripts/write_harvest_report.py`
- `scripts/write_harvest_research_lessons.py`
- `scripts/write_harvest_state_snapshot.py`
- `scripts/write_harvest_verdict.py`
- `scripts/write_hermes_correction_state.py`
- `scripts/write_institutional_holding_source_mvp.py`
- `scripts/write_margin_controlled_probe.py`
- `scripts/write_margin_feature_daily_asof_overlay.py`
- `scripts/write_margin_feature_monthly_panel.py`
- `scripts/write_margin_feature_sample.py`
- `scripts/write_margin_low_crowding_controlled_config.py`
- `scripts/write_margin_source_mvp.py`
- `scripts/write_next_autonomous_research_plan.py`
- `scripts/write_next_harvest_plan.py`
- `scripts/write_online_data_source_preflight.py`
- `scripts/write_ownership_concentration_source_mvp.py`
- `scripts/write_paper_live_promotion_readiness.py`
- `scripts/write_paper_monitoring_report.py`
- `scripts/write_paper_portfolio_diagnostics.py`
- `scripts/write_paper_retrospective_return_tracking.py`
- `scripts/write_pit_cashflow_closure_policy.py`
- `scripts/write_pit_cashflow_conditioning_diagnostics.py`
- `scripts/write_pit_cashflow_coverage_diagnostics.py`
- `scripts/write_pit_cashflow_denominator_audit.py`
- `scripts/write_pit_cashflow_diagnostic_dataset.py`
- `scripts/write_pit_cashflow_source_audit.py`
- `scripts/write_pit_field_decision.py`
- `scripts/write_pit_field_transform_diagnostics.py`
- `scripts/write_pit_missing_value_diagnostics.py`
- `scripts/write_pit_non_cashflow_mechanism_preflight.py`
- `scripts/write_pit_value_trap_attribution.py`
- `scripts/write_pledge_controlled_probe_plan.py`
- `scripts/write_pledge_controlled_validation.py`
- `scripts/write_pledge_source_mvp.py`
- `scripts/write_portfolio_constraint_hardening.py`
- `scripts/write_research_quality_summary.py`
- `scripts/write_shareholder_count_mvp.py`
- `scripts/write_shareholder_crowding_source_mvp.py`
- `scripts/write_simulated_portfolio_construction_repair.py`
- `scripts/write_simulated_portfolio_drawdown_group_diagnostic.py`
- `scripts/write_simulated_portfolio_repair_review.py`
- `scripts/write_simulation_risk_constraint_diagnostics.py`
- `scripts/write_small_institutional_backtest_matrix.py`
- `scripts/write_small_institutional_dataset_preflight.py`
- `scripts/write_small_institutional_drawdown_blocker_evidence.py`
- `scripts/write_small_institutional_risk_reduction_plan.py`
- `scripts/write_small_institutional_self_diagnosis.py`
- `scripts/write_small_institutionalization_status.py`
- `scripts/write_value_route_bucket_aware_report.py`
- `scripts/write_value_route_correlation_overlap.py`
- `scripts/write_value_route_scorecard.py`
- `scripts/write_value_sleeve_decision.py`
- `scripts/write_value_sleeve_policy.py`
- `scripts/write_value_sleeve_portfolio_validation.py`

### devtool_candidate (10)

- `scripts/probe_diemeng_financial_params.py`
- `scripts/probe_diemeng_methods.py`
- `scripts/probe_llm_cache_usage.py`
- `scripts/probe_p0_data_sources.py`
- `scripts/probe_research_daemon_active_worker_stop.py`
- `scripts/probe_research_daemon_once.py`
- `scripts/smoke_hermes_profiles.py`
- `scripts/smoke_test_factor_lab.py`
- `scripts/test_llm_bridge.py`
- `scripts/test_v1_run.py`

### ops_or_devtool_candidate (19)

- `scripts/diagnose_value_route_direction.py`
- `scripts/diagnose_value_route_mechanism.py`
- `scripts/extend_autonomous_strategy_history_cache.py`
- `scripts/extend_small_institutional_dataset.py`
- `scripts/generate_batch_from_llm_plan.py`
- `scripts/generate_defensive_quality_experiments.py`
- `scripts/generate_pit_value_trap_repair_batch.py`
- `scripts/generate_pledge_followup_probe.py`
- `scripts/generate_value_route_batch.py`
- `scripts/generate_value_route_bucket_aware_batch.py`
- `scripts/generate_value_route_direction_batch.py`
- `scripts/generate_value_route_followup_batch.py`
- `scripts/prepare_autonomous_strategy_workers.py`
- `scripts/prepare_gated_research_task.py`
- `scripts/prepare_tushare_data.py`
- `scripts/quarantine_legacy_research_tasks.py`
- `scripts/render_autonomous_research_loop_timer.py`
- `scripts/render_controlled_feeder_systemd_units.py`
- `scripts/render_harvest_agent_timer.py`

### needs_manual_classification (41)

- `scripts/admit_pledge_controlled_probe_task.py`
- `scripts/apply_strategy_plan.py`
- `scripts/bootstrap_hermes_profiles.py`
- `scripts/build_ab_harness.py`
- `scripts/build_approved_candidate_universe.py`
- `scripts/build_candidate_triage_model.py`
- `scripts/build_change_report.py`
- `scripts/build_decision_impact_report.py`
- `scripts/build_decision_observation_report.py`
- `scripts/build_html_report.py`
- `scripts/build_index_page.py`
- `scripts/build_llm_retrospective.py`
- `scripts/build_paper_portfolio.py`
- `scripts/build_paper_portfolio_retrospective.py`
- `scripts/build_queue_explanation.py`
- `scripts/build_recommendation_context.py`
- `scripts/build_recommendation_memory.py`
- `scripts/build_research_attribution.py`
- `scripts/build_research_branch_plan.py`
- `scripts/build_research_candidate_pool.py`
- `scripts/build_research_metrics.py`
- `scripts/build_research_planner_proposal.py`
- `scripts/build_research_planner_snapshot.py`
- `scripts/build_research_space_map.py`
- `scripts/build_research_space_registry.py`
- `scripts/build_research_state_snapshot.py`
- `scripts/build_robustness_batch.py`
- `scripts/build_run_summary.py`
- `scripts/build_sqlite_report.py`
- `scripts/build_strategy_plan.py`
- `scripts/build_tushare_master_cache.py`
- `scripts/enable_hermes_native.py`
- `scripts/import_llm_bridge_response.py`
- `scripts/inject_research_planner_tasks.py`
- `scripts/refresh_llm_snapshot_with_memory.py`
- `scripts/seed_research_queue.py`
- `scripts/summarize_llm_usage.py`
- `scripts/update_harvest_knowledge.py`
- `scripts/validate_research_planner_proposal.py`
- `scripts/verify_de_hermes_native_runtime.py`
- `scripts/verify_de_openclaw_runtime.py`

## Phase 4 acceptance status

- Directory structure exists.
- Stable top-level surface is documented.
- Existing scripts were not moved in this low-risk pass; this prevents breaking systemd/docs/tests while the repo is still noisy.
- Future moves should keep wrappers and be verified group-by-group.
