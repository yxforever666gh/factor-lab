from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from factor_lab.adaptive_shadow_execution import (
    ShadowCyclePlan,
    evaluate_shadow_cycle,
    genesis_shadow_account,
)
from factor_lab.data import adaptive_shadow_execution as shadow_data
from factor_lab.data import prospective_execution as execution_data
from factor_lab.data.adaptive_shadow_execution import (
    AdaptiveShadowExecutionDataError,
    build_adaptive_shadow_execution_snapshot,
    load_adaptive_shadow_execution_snapshot,
)
from factor_lab.data.prospective_execution import build_prospective_execution_snapshot
from factor_lab.prospective_execution import AccountPosition, SleeveAccountState
from factor_lab.prospective_targets import (
    GenerationResult,
    InputSnapshot,
    SleeveState,
    TenSleeveState,
)

from test_prospective_execution_data import (
    _SignalSource,
    _adj,
    _calendar_artifact,
    _daily,
    _daily_basic,
    _partition,
    _suspensions,
    _write_json,
)


CURRENT = tuple(f"{index:06d}.SZ" for index in range(1, 13))
TARGETS = CURRENT[:10]
EXITED_PRIOR = "000099.SZ"
PLAN_BINDINGS = {
    "plan_record_sha256": "4" * 64,
    "source_data_snapshot_sha256": "e" * 64,
    "shadow_target_rows_sha256": "5" * 64,
    "formal_route_target_plan_sha256": "6" * 64,
}


def _tree_digest(path: Path) -> str:
    digest = hashlib.sha256()
    if not path.exists():
        return digest.hexdigest()
    for item in sorted(path.rglob("*"), key=lambda value: value.as_posix()):
        relative = item.relative_to(path).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        if item.is_file():
            payload = item.read_bytes()
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


def _generation(input_sha: str, sessions: list[str], signal_index: int) -> GenerationResult:
    offset = signal_index % 10
    deployment = "d" * 64
    sleeves = [SleeveState(offset=value) for value in range(10)]
    sleeves[offset] = SleeveState(
        offset=offset,
        initialized=True,
        last_signal_date=sessions[signal_index],
        last_calendar_index=signal_index,
        targets_ppm={TARGETS[0]: 100_000},
        cash_ppm=900_000,
    )
    state = TenSleeveState(
        deployment_sha256=deployment,
        activation_record_sha256="a" * 64,
        implementation_upgrade_record_sha256="b" * 64,
        last_processed_calendar_index=signal_index,
        last_processed_session=sessions[signal_index],
        sleeves=sleeves,
    )
    plans = tuple(
        {
            "action": "seed" if sleeve.offset == offset else "cash",
            **sleeve.to_dict(),
        }
        for sleeve in state.sleeves
    )
    return GenerationResult(
        deployment_sha256=deployment,
        input_snapshot_sha256=input_sha,
        previous_state_sha256="c" * 64,
        signal_date=sessions[signal_index],
        trade_date=sessions[signal_index + 1],
        calendar_index=signal_index,
        due_offset=offset,
        skipped_sessions=(),
        sleeve_plans=plans,
        aggregate_targets_ppm={TARGETS[0]: 10_000},
        aggregate_cash_ppm=990_000,
        next_state=state,
    )


@pytest.fixture
def source_backed_market(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    Path,
    GenerationResult,
    _SignalSource,
    list[str],
    execution_data.ProspectiveExecutionDataSnapshot,
    ShadowCyclePlan,
]:
    root = tmp_path
    sessions = [
        value.date().isoformat()
        for value in pd.bdate_range("2026-07-01", periods=35)
    ]
    signal_index = 22
    signal = sessions[signal_index]
    trade = sessions[signal_index + 1]
    holding_end = sessions[signal_index + 11]
    monkeypatch.setattr(execution_data, "_OFFICIAL_DELIST_MINIMUM_ROWS", 1)

    class _OfficialClient:
        def stock_basic(self, **kwargs: Any) -> pd.DataFrame:
            assert kwargs["list_status"] == "D"
            return pd.DataFrame(
                {
                    "ts_code": ["900001.SH"],
                    "list_status": ["D"],
                    "delist_date": ["20000101"],
                }
            )

    monkeypatch.setattr(
        execution_data,
        "_official_delist_client",
        lambda _root: _OfficialClient(),
    )
    monkeypatch.setattr(
        execution_data,
        "_official_retrieved_at_utc",
        lambda: f"{holding_end}T08:29:00Z",
    )

    target_frame = pd.DataFrame(
        {
            "date": signal,
            "ticker": CURRENT,
            "eligible": [True] * 10 + [False] * 2,
            "universe_member": True,
            "earnings_yield": [0.20 - index * 0.005 for index in range(12)],
            "pb": [1.0 + index * 0.1 for index in range(12)],
            "book_yield": [1.0 / (1.0 + index * 0.1) for index in range(12)],
            "volatility_20": [0.20 + index * 0.001 for index in range(12)],
        }
    )
    signal_frame = target_frame.copy()
    signal_frame["close"] = [121.0 + index * 20.0 for index in range(12)]
    signal_frame["close_adj"] = signal_frame["close"] * 2.0
    signal_frame["adj_factor"] = 2.0
    signal_frame["adj_calibration_multiplier"] = 1.0
    signal_frame["adv_20"] = 100_000_000.0

    source_sha = PLAN_BINDINGS["source_data_snapshot_sha256"]
    source_directory = root / "runtime/prospective/5.0/inputs" / source_sha
    source_directory.mkdir(parents=True)
    top500 = root / "runtime/data/top500"
    top500.mkdir(parents=True, exist_ok=True)
    features_path = top500 / "features.parquet"
    pd.DataFrame(
        {
            "ticker": [*CURRENT, EXITED_PRIOR],
            "delist_date": pd.Series(
                [pd.NaT] * 13,
                dtype="datetime64[ns]",
            ),
        }
    ).to_parquet(features_path, index=False)
    _features_cas, features_binding = execution_data._capture_immutable_artifact(
        root,
        features_path,
    )

    checkpoint: dict[str, Any] = {
        "schema_version": 1,
        "partitions": {},
        "calendars": {},
    }
    calendar_sha = _calendar_artifact(
        root,
        sessions,
        checkpoint,
        completed_at=f"{signal}T06:00:00Z",
    )
    calendar_checkpoint = checkpoint["calendars"][calendar_sha]
    calendar_path = Path(calendar_checkpoint["path"])
    calendar_manifest_path = Path(calendar_checkpoint["manifest_path"])
    _calendar_cas, calendar_binding = execution_data._capture_immutable_artifact(
        root,
        calendar_path,
        expected_sha256=calendar_checkpoint["artifact_sha256"],
        sha_field="artifact_sha256",
    )
    _calendar_manifest_cas, calendar_manifest_binding = (
        execution_data._capture_immutable_artifact(
            root,
            calendar_manifest_path,
            expected_sha256=calendar_checkpoint["manifest_sha256"],
            sha_field="manifest_sha256",
            path_field="immutable_manifest_path",
            size_field="manifest_size_bytes",
            media_field="manifest_media_type",
        )
    )
    source = _SignalSource(
        signal_date=signal,
        trade_date=trade,
        snapshot_sha256=source_sha,
        directory=source_directory,
        build_completed_at_utc=f"{signal}T08:30:00Z",
        inputs_available_at_utc=f"{signal}T08:00:00Z",
        frame=signal_frame,
        manifest={
            "inputs": [
                {
                    "role": "canonical_features",
                    "path": "runtime/data/top500/features.parquet",
                    **features_binding,
                    "availability_basis": "pre_activation_frozen_canonical",
                }
            ],
            "calendar": {
                "sources": [
                    {
                        "calendar_content_sha256": calendar_sha,
                        "path": execution_data._relative(calendar_path, root),
                        **calendar_binding,
                        "manifest_path": execution_data._relative(
                            calendar_manifest_path,
                            root,
                        ),
                        **calendar_manifest_binding,
                        "completed_at_utc": f"{signal}T06:00:00Z",
                        "availability_basis": "checkpoint_completed_at_utc",
                        "source_start_date": sessions[0],
                        "source_end_date": sessions[-1],
                        "row_count": int(calendar_checkpoint["row_count"]),
                        "open_day_count": int(
                            calendar_checkpoint["open_day_count"]
                        ),
                    }
                ]
            },
        },
        calendar_sessions=tuple(sessions[: signal_index + 2]),
        target_frame=target_frame,
        target_rows_sha256="1" * 64,
        input_sources_sha256="2" * 64,
        membership_artifact_sha256="3" * 64,
    )
    input_snapshot = InputSnapshot(
        signal_date=signal,
        calendar_sessions=sessions[: signal_index + 2],
        rows=target_frame,
        source_data_snapshot_sha256=source_sha,
        target_rows_sha256=source.target_rows_sha256,
        input_sources_sha256=source.input_sources_sha256,
        membership_artifact_sha256=source.membership_artifact_sha256,
        source_build_checkpoint_utc=source.build_completed_at_utc,
        max_available_at_utc=source.inputs_available_at_utc,
        information_cutoff_utc=source.build_completed_at_utc,
        signal_close_utc=f"{signal}T07:00:00Z",
        admission_deadline_utc=f"{trade}T01:15:00Z",
    )
    generation = _generation(input_snapshot.snapshot_sha256, sessions, signal_index)
    monkeypatch.setattr(
        execution_data,
        "load_prospective_input_snapshot",
        lambda _path: source,
    )

    raw_tickers = [*CURRENT, EXITED_PRIOR]
    raw_sessions = sessions[signal_index - 20 : signal_index + 12]
    for day_index, session in enumerate(raw_sessions):
        completion = f"{session}T08:00:00Z"
        _partition(
            root,
            checkpoint,
            "daily",
            session,
            _daily(raw_tickers, session, day_index),
            completed_at=completion,
        )
        _partition(
            root,
            checkpoint,
            "daily_basic",
            session,
            _daily_basic(raw_tickers, session),
            completed_at=completion,
        )
        _partition(
            root,
            checkpoint,
            "adj_factor",
            session,
            _adj(raw_tickers, session),
            completed_at=completion,
        )
    checkpoint_path = root / "runtime/data/raw/checkpoint.json"
    _write_json(checkpoint_path, checkpoint)
    _suspensions(
        root,
        start=execution_data.SUSPENSION_FULL_START_DATE,
        end=holding_end,
        completed_at=f"{holding_end}T08:30:00Z",
    )

    # The slow path may only *reference* existing formal CAS.  Pre-seal the
    # signal and twenty prior daily/adj partitions before building the formal
    # execution and before taking the formal-tree digest in each test.
    for session in sessions[signal_index - 20 : signal_index + 1]:
        for dataset in ("daily", "adj_factor"):
            entry = checkpoint["partitions"][f"{dataset}/{session}"]
            execution_data._capture_immutable_artifact(
                root,
                Path(entry["path"]),
                expected_sha256=entry["sha256"],
            )

    formal = build_prospective_execution_snapshot(
        root,
        generation,
        source_data_snapshot_sha256=source_sha,
    )
    plan = ShadowCyclePlan(
        registry_sha256="7" * 64,
        candidate_id="low_turnover_20_v1",
        candidate_sha256="8" * 64,
        offset=generation.due_offset,
        signal_date=signal,
        trade_date=trade,
        targets_ppm={ticker: 100_000 for ticker in TARGETS},
        formal_input_snapshot_sha256=generation.input_snapshot_sha256,
        formal_decision_record_sha256="9" * 64,
        planned_at_utc=f"{signal}T13:00:00Z",
        formal_trade_deadline_utc=f"{trade}T01:15:00Z",
    )
    return root, generation, source, sessions, formal, plan


def _prior_state(plan: ShadowCyclePlan, start_date: str) -> SleeveAccountState:
    price = 500.0
    quantity = 1.0
    return SleeveAccountState(
        deployment_sha256=plan.account_deployment_sha256,
        offset=plan.offset,
        cycle_count=1,
        cash_hex=(5_000_000.0 - price * quantity).hex(),
        positions=(
            AccountPosition(
                ticker=EXITED_PRIOR,
                quantity_hex=quantity.hex(),
                last_price_hex=price.hex(),
                average_cost_hex=price.hex(),
                last_observation_date=start_date,
            ),
        ),
        nav_fen=500_000_000,
        last_holding_end_date=start_date,
        last_generation_result_sha256="a" * 64,
        last_execution_snapshot_sha256="b" * 64,
    )


def test_public_builder_rejects_a_bare_formal_snapshot(source_backed_market) -> None:
    root, generation, _source, _sessions, formal, plan = source_backed_market
    with pytest.raises(AdaptiveShadowExecutionDataError, match="source-backed bundle"):
        build_adaptive_shadow_execution_snapshot(
            root,
            generation,
            formal.snapshot,
            plan,
            genesis_shadow_account(plan),
            plan_bindings=PLAN_BINDINGS,
        )


def test_fast_path_reuses_formal_bytes_is_idempotent_and_reloads(
    source_backed_market,
) -> None:
    root, generation, _source, _sessions, formal, plan = source_backed_market
    previous = genesis_shadow_account(plan)
    first = build_adaptive_shadow_execution_snapshot(
        root,
        generation,
        formal,
        plan,
        previous,
        plan_bindings=PLAN_BINDINGS,
    )
    first_bytes = (first.sources_path.read_bytes(), first.snapshot_path.read_bytes())
    second = build_adaptive_shadow_execution_snapshot(
        root,
        generation,
        formal.directory,
        plan,
        previous,
        plan_bindings=PLAN_BINDINGS,
    )

    assert first.source_contract["mode"] == "formal_snapshot_reuse"
    market = first.snapshot.execution_snapshot
    assert market.rows == formal.snapshot.rows
    assert market.calendar_sessions == formal.snapshot.calendar_sessions
    assert market.benchmark_tickers == formal.snapshot.benchmark_tickers
    assert market.execution_source_sha256 == first.source_contract_sha256
    assert market.execution_source_sha256 != formal.snapshot.execution_source_sha256
    outcome = evaluate_shadow_cycle(plan, first.snapshot, previous)
    assert (
        first.source_contract["formal_execution_snapshot_sha256"]
        == formal.snapshot.snapshot_sha256
    )
    assert outcome.shadow_execution_snapshot_sha256 == first.snapshot.snapshot_sha256
    assert outcome.market_execution_snapshot_sha256 == market.snapshot_sha256
    assert outcome.market_execution_snapshot_sha256 != formal.snapshot.snapshot_sha256
    assert second.bundle_sha256 == first.bundle_sha256
    assert (second.sources_path.read_bytes(), second.snapshot_path.read_bytes()) == first_bytes
    rebound = build_adaptive_shadow_execution_snapshot(
        root,
        generation,
        formal,
        plan,
        previous,
        plan_bindings={**PLAN_BINDINGS, "plan_record_sha256": "0" * 64},
    )
    assert rebound.source_contract_sha256 != first.source_contract_sha256
    assert rebound.snapshot_sha256 != first.snapshot_sha256
    loaded = load_adaptive_shadow_execution_snapshot(
        first.directory,
        generation,
        plan,
        previous,
        plan_bindings=PLAN_BINDINGS,
    )
    assert loaded.snapshot.to_dict() == first.snapshot.to_dict()


def test_fast_reload_deep_replays_the_referenced_formal_cas(
    source_backed_market,
) -> None:
    root, generation, _source, _sessions, formal, plan = source_backed_market
    previous = genesis_shadow_account(plan)
    built = build_adaptive_shadow_execution_snapshot(
        root,
        generation,
        formal,
        plan,
        previous,
        plan_bindings=PLAN_BINDINGS,
    )
    raw_source = formal.source_contract["raw_partitions"][0]
    (root / raw_source["immutable_path"]).write_bytes(b"tampered formal CAS")

    with pytest.raises(
        AdaptiveShadowExecutionDataError,
        match="formal execution bundle failed independent source replay",
    ):
        load_adaptive_shadow_execution_snapshot(
            built.directory,
            generation,
            plan,
            previous,
            plan_bindings=PLAN_BINDINGS,
        )


def test_monthly_roster_exit_is_supplemented_and_formal_tree_is_unchanged(
    source_backed_market,
) -> None:
    root, generation, _source, sessions, formal, plan = source_backed_market
    previous = _prior_state(plan, formal.snapshot.holding_start_date)
    formal_root = root / "runtime/prospective/5.0"
    before = _tree_digest(formal_root)
    built = build_adaptive_shadow_execution_snapshot(
        root,
        generation,
        formal,
        plan,
        previous,
        plan_bindings=PLAN_BINDINGS,
    )
    after = _tree_digest(formal_root)

    assert before == after
    assert built.source_contract["mode"] == "supplemented_prior_holdings"
    assert built.source_contract["supplemented_tickers"] == [EXITED_PRIOR]
    market = built.snapshot.execution_snapshot
    assert market.benchmark_tickers == formal.snapshot.benchmark_tickers
    prior_rows = [row for row in market.rows if row.ticker == EXITED_PRIOR]
    assert len(prior_rows) == 11
    assert prior_rows[0].date == formal.snapshot.holding_start_date
    assert prior_rows[-1].date == formal.snapshot.holding_end_date
    assert prior_rows[0].open_adj_hex == (500.0).hex()
    assert prior_rows[0].execution_input_date == sessions[generation.calendar_index]
    assert prior_rows[0].adv_20_asof_hex == (100_000_000.0).hex()
    assert len(built.source_contract["fallback_raw_partitions"]) == 42
    outcome = evaluate_shadow_cycle(plan, built.snapshot, previous)
    assert (
        built.source_contract["formal_execution_snapshot_sha256"]
        == formal.snapshot.snapshot_sha256
    )
    assert outcome.shadow_execution_snapshot_sha256 == built.snapshot.snapshot_sha256
    assert outcome.market_execution_snapshot_sha256 == market.snapshot_sha256
    assert outcome.market_execution_snapshot_sha256 != formal.snapshot.snapshot_sha256
    assert outcome.next_account_state.cycle_count == 2

    loaded = load_adaptive_shadow_execution_snapshot(
        built.directory,
        generation,
        plan,
        previous,
        plan_bindings=PLAN_BINDINGS,
    )
    assert loaded.snapshot.to_dict() == built.snapshot.to_dict()
    assert _tree_digest(formal_root) == before


def test_slow_path_fails_when_required_history_cas_is_missing(
    source_backed_market,
) -> None:
    root, generation, _source, sessions, formal, plan = source_backed_market
    previous = _prior_state(plan, formal.snapshot.holding_start_date)
    checkpoint = json.loads(
        (root / "runtime/data/raw/checkpoint.json").read_text(encoding="utf-8")
    )
    entry = checkpoint["partitions"][f"daily/{sessions[generation.calendar_index]}"]
    cas = (
        root
        / "runtime/prospective/5.0/source-artifacts"
        / f"sha256={entry['sha256']}"
        / "artifact"
    )
    cas.unlink()

    with pytest.raises(AdaptiveShadowExecutionDataError, match="CAS is missing"):
        build_adaptive_shadow_execution_snapshot(
            root,
            generation,
            formal,
            plan,
            previous,
            plan_bindings=PLAN_BINDINGS,
        )


def test_loader_rejects_cas_and_stored_plan_binding_tampering(
    source_backed_market,
) -> None:
    root, generation, _source, _sessions, formal, plan = source_backed_market
    previous = _prior_state(plan, formal.snapshot.holding_start_date)
    built = build_adaptive_shadow_execution_snapshot(
        root,
        generation,
        formal,
        plan,
        previous,
        plan_bindings=PLAN_BINDINGS,
    )
    wrong_bindings = {**PLAN_BINDINGS, "plan_record_sha256": "0" * 64}
    with pytest.raises(AdaptiveShadowExecutionDataError, match="different generation, plan"):
        load_adaptive_shadow_execution_snapshot(
            built.directory,
            generation,
            plan,
            previous,
            plan_bindings=wrong_bindings,
        )

    fallback = built.source_contract["fallback_raw_partitions"][0]
    (root / fallback["immutable_path"]).write_bytes(b"tampered fallback CAS")
    with pytest.raises(AdaptiveShadowExecutionDataError, match="fallback CAS"):
        load_adaptive_shadow_execution_snapshot(
            built.directory,
            generation,
            plan,
            previous,
            plan_bindings=PLAN_BINDINGS,
        )
