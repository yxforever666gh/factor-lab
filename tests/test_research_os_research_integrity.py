from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from factor_lab.research_os.catalog import TrialLedgerEntry
from factor_lab.research_os.contracts import (
    FactorDirection,
    SignalFieldSpec,
    SleeveSpec,
    TrialOutcome,
)
from factor_lab.research_os.cycle import (
    HistoricalResearchCycle,
    _consume_inner_folds,
    _lifetime_net_sharpes,
)
from factor_lab.research_os.data_sync import read_frame
from factor_lab.research_os.dsl import DslValidationError, FactorGraph, FieldNode, FieldSpec
from factor_lab.research_os.governance import TrialLedger
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.snapshots import (
    SnapshotIntegrityError,
    build_immutable_snapshot_manifest,
    verify_snapshot_frame_binding,
)
from factor_lab.research_os.walk_forward import build_nested_walk_forward_plan


def _environment_hashes() -> dict[str, str]:
    return {
        "config_hash": "a" * 64,
        "code_hash": "b" * 64,
        "dirty_patch_hash": "c" * 64,
        "dependency_lock_hash": "d" * 64,
    }


def test_research_frame_is_bound_to_manifest_file_and_iceberg_tag(tmp_path) -> None:
    data = tmp_path / "gold-frame.parquet"
    pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "ticker": ["000001.SZ", "000001.SZ"],
            "open": [10.0, 10.2],
        }
    ).to_parquet(data, index=False)
    manifest = build_immutable_snapshot_manifest(
        (data,),
        base_dir=tmp_path,
        tier="gold",
        as_of=datetime(2026, 8, 22, tzinfo=timezone.utc),
        parent_snapshot_ids=("e" * 64,),
        environment_hashes=_environment_hashes(),
        quality_report={"status": "pass"},
        trust_labels=("point_in_time",),
    )
    reference = manifest.to_snapshot_ref(
        uri=f"iceberg://factorlab/factor_lab.gold#{'ros_' + manifest.snapshot_id}"
    )
    frame = read_frame(data)
    binding = verify_snapshot_frame_binding(reference, frame)
    assert binding.snapshot_id == manifest.snapshot_id
    assert binding.row_count == 2
    assert binding.manifest_file == "gold-frame.parquet"

    altered = frame.copy()
    altered.loc[0, "open"] = 999.0
    with pytest.raises(SnapshotIntegrityError, match="differs"):
        verify_snapshot_frame_binding(reference, altered)

    fake_reference = reference.model_copy(
        update={"uri": f"s3://bucket/{manifest.snapshot_id}"}
    )
    with pytest.raises(SnapshotIntegrityError, match="Iceberg"):
        verify_snapshot_frame_binding(fake_reference, frame)


def test_trusted_calendar_is_content_addressed_in_gold_manifest(tmp_path) -> None:
    data = tmp_path / "gold-frame.parquet"
    pd.DataFrame({"date": ["2026-01-02"], "ticker": ["000001.SZ"]}).to_parquet(
        data, index=False
    )
    calendar = {
        "source": "reconciled_silver:trade_calendar",
        "quality_status": "accepted",
        "sessions": ["2026-01-02", "2026-01-05"],
    }
    manifest = build_immutable_snapshot_manifest(
        (data,),
        base_dir=tmp_path,
        tier="gold",
        as_of=datetime(2026, 1, 2, 15, tzinfo=timezone.utc),
        parent_snapshot_ids=("e" * 64,),
        environment_hashes=_environment_hashes(),
        quality_report={"status": "pass"},
        trading_calendar=calendar,
    )
    payload = manifest.to_dict()
    assert payload["trading_calendar"]["sessions"] == calendar["sessions"]
    assert len(payload["trading_calendar"]["content_hash"]) == 64

    tampered = dict(payload)
    tampered["trading_calendar"] = {
        **payload["trading_calendar"],
        "sessions": ["2026-01-02", "2026-01-06"],
    }
    from factor_lab.research_os.snapshots import verify_immutable_snapshot_manifest

    verification = verify_immutable_snapshot_manifest(tampered, base_dir=tmp_path)
    assert verification["valid"] is False
    assert {item["code"] for item in verification["errors"]} >= {
        "snapshot_id_mismatch",
        "trading_calendar_invalid",
    }


def test_inner_selection_consumes_every_train_and_validation_fold() -> None:
    sessions = pd.bdate_range("2017-01-02", "2025-12-31")
    frame = pd.DataFrame(
        {
            "date": sessions,
            "ticker": "000001.SZ",
            "signal": range(len(sessions)),
        }
    )
    signal = frame["signal"].astype(float)
    plan = build_nested_walk_forward_plan(
        sessions,
        {
            "initial_train_start": "2017-01-01",
            "initial_train_end": "2020-12-31",
            "outer_test_years": (2021, 2022, 2023, 2024, 2025),
            "diagnostic_years": (),
            "purge_sessions": 6,
            "embargo_sessions": 5,
        },
    )
    first = plan.outer_folds[0]
    evidence = _consume_inner_folds(frame, signal, first.inner)
    assert len(evidence) == len(first.inner)
    assert {item["fold_id"] for item in evidence} == {
        fold.fold_id for fold in first.inner
    }
    assert all(item["train_observations"] > 0 for item in evidence)
    assert all(item["validation_observations"] > 0 for item in evidence)
    assert all(len(item["evidence_hash"]) == 64 for item in evidence)

    broken = signal.copy()
    validation_index = frame.index[
        frame["date"].between(first.inner[0].test_start, first.inner[0].test_end)
    ]
    broken.loc[validation_index] = float("nan")
    with pytest.raises(ValueError, match="no finite validation signal"):
        _consume_inner_folds(frame, broken, first.inner)


def test_dsr_population_uses_trial_metadata_not_variant_identifier() -> None:
    now = datetime(2026, 8, 22, tzinfo=timezone.utc)
    entries = (
        TrialLedgerEntry(
            trial_id="trial-one",
            experiment_id="exp-one",
            family="value",
            candidate_id="candidate-one",
            outcome=TrialOutcome.FAILURE,
            reason="observed",
            occurred_at=now,
            metadata={"variant_id": "99.0", "net_sharpe": 0.42},
        ),
        TrialLedgerEntry(
            trial_id="trial-two",
            experiment_id="exp-two",
            family="value",
            candidate_id="candidate-two",
            outcome=TrialOutcome.FAILURE,
            reason="missing metric",
            occurred_at=now,
            metadata={"variant_id": "1.23", "net_sharpe": "8.8"},
        ),
        TrialLedgerEntry(
            trial_id="trial-three",
            experiment_id="exp-three",
            family="trend",
            candidate_id="candidate-three",
            outcome=TrialOutcome.SUCCESS,
            reason="other family",
            occurred_at=now,
            metadata={"net_sharpe": 7.0},
        ),
    )
    ledger = TrialLedger.from_catalog_entries(entries)
    assert _lifetime_net_sharpes(ledger, "value") == (0.42,)
    assert ledger.family_records("value")[0].metadata["net_sharpe"] == 0.42


def test_sleeve_signal_must_be_preregistered_dsl_and_cannot_use_forward_field() -> None:
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-02"]),
            "ticker": ["000001.SZ"],
            "safe": [1.0],
            "forward_return_5d": [9.0],
        }
    )
    safe_graph = FactorGraph(
        nodes=(FieldNode("raw", "safe"),), output_id="raw"
    ).to_dict()
    spec = SimpleNamespace(
        factor=None,
        sleeve=SimpleNamespace(
            signal_expression=safe_graph,
            signal_field_registry=(SignalFieldSpec(name="safe"),),
        ),
    )
    with pytest.raises(ValueError, match="runtime sleeve_signal is forbidden"):
        HistoricalResearchCycle._signal(
            None,
            spec,
            frame,
            (FieldSpec("safe"),),
            sleeve_signal="safe",
        )

    forward_graph = FactorGraph(
        nodes=(FieldNode("raw", "forward_return_5d"),), output_id="raw"
    ).to_dict()
    leaked = SimpleNamespace(
        factor=None,
        sleeve=SimpleNamespace(
            signal_expression=forward_graph,
            signal_field_registry=(SignalFieldSpec(name="forward_return_5d"),),
        ),
    )
    with pytest.raises(DslValidationError):
        HistoricalResearchCycle._signal(
            None,
            leaked,
            frame,
            (FieldSpec("forward_return_5d"),),
            sleeve_signal=None,
        )

    rogue_graph = FactorGraph(
        nodes=(FieldNode("raw", "rogue"),), output_id="raw"
    ).to_dict()
    unregistered = SimpleNamespace(
        factor=None,
        sleeve=SimpleNamespace(
            signal_expression=rogue_graph,
            signal_field_registry=(SignalFieldSpec(name="safe"),),
        ),
    )
    with pytest.raises(DslValidationError):
        HistoricalResearchCycle._signal(
            None,
            unregistered,
            frame.assign(rogue=123.0),
            (),
            sleeve_signal=None,
        )


def test_sleeve_signal_registry_changes_candidate_fingerprint() -> None:
    common = {
        "sleeve_id": "value_quality",
        "name": "Value quality",
        "mechanism": "cheap quality companies may be underpriced",
        "factor_ids": ("value", "quality"),
        "signal_expression": FactorGraph(
            nodes=(FieldNode("raw", "book_yield"),), output_id="raw"
        ).to_dict(),
        "falsification_criteria": ("outer OOS excess is non-positive",),
    }
    left = SleeveSpec(
        **common,
        signal_field_registry=(SignalFieldSpec(name="book_yield"),),
    )
    right = SleeveSpec(
        **common,
        signal_field_registry=(
            SignalFieldSpec(name="book_yield", minimum_lag_sessions=1),
        ),
    )
    assert content_fingerprint(left) != content_fingerprint(right)


def test_factor_signal_uses_only_its_fingerprint_bound_field_registry() -> None:
    graph = FactorGraph(
        nodes=(FieldNode("raw", "book_yield"),), output_id="raw"
    ).to_dict()
    spec = SimpleNamespace(
        factor=SimpleNamespace(
            expression=graph,
            signal_field_registry=(SignalFieldSpec(name="book_yield"),),
            direction=FactorDirection.HIGHER_IS_BETTER,
        ),
        sleeve=None,
        preregistration=SimpleNamespace(direction="positive"),
    )
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "ticker": ["000001.SZ", "000001.SZ"],
            "book_yield": [0.1, 0.2],
        }
    )
    signal = HistoricalResearchCycle._signal(
        None, spec, frame, (), sleeve_signal=None
    )
    assert signal.tolist() == [0.1, 0.2]

    with pytest.raises(ValueError, match="runtime field registry differs"):
        HistoricalResearchCycle._signal(
            None,
            spec,
            frame,
            (FieldSpec("book_yield", minimum_lag_sessions=1),),
            sleeve_signal=None,
        )
