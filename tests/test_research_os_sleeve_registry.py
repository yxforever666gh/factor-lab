from __future__ import annotations

import json
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.dsl import (
    Availability,
    DecisionPoint,
    FieldRole,
    FieldSpec,
    ValueType,
    compile_factor_graph,
    factor_graph_from_spec,
)
from factor_lab.research_os.sleeve_registry import (
    INITIAL_ROSTER_NAME,
    INITIAL_SLEEVE_IDS,
    SleeveActiveReturnSeries,
    SleeveClusteringError,
    SleeveRegistryError,
    build_active_return_series,
    build_sleeve_roster_manifest,
    cluster_sleeve_active_returns,
    load_sleeve_roster,
    persist_sleeve_cluster_manifest,
    persist_sleeve_roster,
    validate_registered_sleeve,
)


ROOT = Path(__file__).resolve().parents[1]
ROSTER_PATH = ROOT / "configs" / "research_os_initial_sleeves.json"
AS_OF = datetime(2026, 8, 21, 23, 59, tzinfo=timezone.utc)


def _roster():
    return load_sleeve_roster(ROSTER_PATH)


def _returns(count: int = 80):
    rng = np.random.default_rng(20260822)
    base = rng.normal(0.0, 0.01, count)
    return {
        "value_quality_v1": base - 0.003,
        "low_risk_defensive_v1": (
            base * 0.99 + rng.normal(0.0, 0.0001, count) + 0.003
        ),
        "medium_term_trend_v1": rng.normal(0.0, 0.01, count),
        "reversal_liquidity_v1": rng.normal(0.0, 0.01, count),
    }


def _evidence(count: int = 80, *, source_as_of: datetime = AS_OF):
    start = datetime(2025, 1, 2, tzinfo=timezone.utc).date()
    sessions = [start + timedelta(days=index) for index in range(count)]
    result = []
    for sleeve_id, values in _returns(count).items():
        observations = [
            {
                "session": session,
                "active_return": float(value),
                "available_at": datetime.combine(
                    session, time(10, 0), tzinfo=timezone.utc
                ),
            }
            for session, value in zip(sessions, values, strict=True)
        ]
        result.append(
            build_active_return_series(
                sleeve_id=sleeve_id,
                source_id=f"experiment:{sleeve_id}:outer-oos",
                source_content_hash=(sleeve_id.encode().hex() + "0" * 64)[:64],
                source_as_of=source_as_of,
                source_kind="authoritative_experiment",
                observations=observations,
            )
        )
    return result


def test_initial_roster_json_is_parseable_typed_and_research_only():
    raw = json.loads(ROSTER_PATH.read_text(encoding="utf-8"))
    assert raw["schema_version"] == "research-os/sleeve-roster/v1"
    roster = _roster()
    assert roster.roster_name == INITIAL_ROSTER_NAME
    assert roster.promotion_policy == "authoritative_experiment_only"
    assert tuple(sorted(INITIAL_SLEEVE_IDS)) == tuple(
        entry.sleeve.sleeve_id for entry in roster.entries
    )
    assert all(
        entry.registration_status == "registered_research_only"
        for entry in roster.entries
    )

    for entry in roster.entries:
        sleeve = validate_registered_sleeve(entry.sleeve)
        assert sleeve.long_only is True
        assert sleeve.maximum_weight == pytest.approx(0.35)
        assert sleeve.cluster_id
        assert sleeve.mechanism
        assert sleeve.falsification_criteria
        assert sleeve.signal_expression["schema_version"] == "research-os/factor-dsl/v1"
        assert all(field.available_at_column for field in sleeve.signal_field_registry)
        # Validation is not merely a JSON shape check: every graph compiles at
        # the frozen after-close decision point.
        compile_factor_graph(
            factor_graph_from_spec({"expression": sleeve.signal_expression}),
            {
                field.name: FieldSpec(
                    name=field.name,
                    value_type=ValueType(field.value_type),
                    role=FieldRole(field.role),
                    availability=Availability(field.availability),
                    minimum_lag_sessions=field.minimum_lag_sessions,
                    available_at_column=field.available_at_column,
                )
                for field in sleeve.signal_field_registry
            },
            decision_point=DecisionPoint.AFTER_CLOSE,
        )


def test_roster_is_content_addressed_order_invariant_and_rejects_duplicates():
    roster = _roster()
    rebuilt = build_sleeve_roster_manifest(
        reversed(roster.entries), roster_name=roster.roster_name
    )
    assert rebuilt == roster
    assert rebuilt.roster_id == roster.roster_id

    with pytest.raises(SleeveRegistryError, match="manifest"):
        build_sleeve_roster_manifest(
            [*roster.entries, roster.entries[0]], roster_name="duplicate-test-roster"
        )


def test_active_return_series_hash_is_order_invariant_and_content_bound():
    series = _evidence()[0]
    rebuilt = build_active_return_series(
        sleeve_id=series.sleeve_id,
        source_id=series.source.source_id,
        source_content_hash=series.source.content_hash,
        source_as_of=series.source.as_of,
        source_kind=series.source.source_kind,
        observations=reversed(series.observations),
    )
    assert rebuilt.series_content_hash == series.series_content_hash
    payload = series.model_dump(mode="python")
    payload["series_content_hash"] = "f" * 64
    with pytest.raises(Exception, match="does not match"):
        SleeveActiveReturnSeries.model_validate(payload)


def test_clustering_is_deterministic_order_invariant_and_deduplicates_correlated_sleeves():
    roster = _roster()
    evidence = _evidence()
    first = cluster_sleeve_active_returns(
        evidence,
        roster=roster,
        as_of=AS_OF,
        correlation_threshold=0.95,
    )
    second = cluster_sleeve_active_returns(
        reversed(evidence),
        roster=roster,
        as_of=AS_OF,
        correlation_threshold=0.95,
    )
    assert first == second
    assert first.cluster_manifest_id == second.cluster_manifest_id
    assert first.aligned_observations == 80
    assert first.promotion_effect == "none_research_dedup_only"
    assert len(first.correlation_matrix) == 4
    assert all(len(row.correlations) == 4 for row in first.correlation_matrix)

    correlated = next(
        cluster
        for cluster in first.clusters
        if "value_quality_v1" in cluster.members
    )
    assert set(correlated.members) == {
        "value_quality_v1",
        "low_risk_defensive_v1",
    }
    # The lower declared priority wins; no Sharpe/performance statistic enters
    # this rule, even though value has deliberately worse mean return here.
    means = {
        item.sleeve_id: np.mean([row.active_return for row in item.observations])
        for item in evidence
    }
    assert means["value_quality_v1"] < means["low_risk_defensive_v1"]
    assert correlated.representative_sleeve_id == "value_quality_v1"
    assert correlated.representative_rule == "declared_priority_then_coverage_then_id"
    assert {row.source_id for row in first.source_bindings} == {
        f"experiment:{sleeve_id}:outer-oos" for sleeve_id in INITIAL_SLEEVE_IDS
    }


def test_clustering_rejects_duplicate_or_unbound_sources():
    roster = _roster()
    evidence = _evidence()
    with pytest.raises(SleeveClusteringError, match="duplicate"):
        cluster_sleeve_active_returns(
            [*evidence, evidence[0]], roster=roster, as_of=AS_OF
        )

    unbound = evidence[0].model_dump(mode="python")
    del unbound["source"]
    with pytest.raises(SleeveClusteringError, match="content-bound"):
        cluster_sleeve_active_returns(
            [unbound, *evidence[1:]], roster=roster, as_of=AS_OF
        )

    missing_return = evidence[0].observations[0].model_dump(mode="python")
    missing_return["active_return"] = None
    with pytest.raises(SleeveClusteringError, match="invalid active-return"):
        build_active_return_series(
            sleeve_id=evidence[0].sleeve_id,
            source_id=evidence[0].source.source_id,
            source_content_hash=evidence[0].source.content_hash,
            source_as_of=evidence[0].source.as_of,
            source_kind=evidence[0].source.source_kind,
            observations=[missing_return, *evidence[0].observations[1:]],
        )


def test_clustering_fails_closed_below_sixty_common_observations():
    with pytest.raises(SleeveClusteringError, match="59 common"):
        cluster_sleeve_active_returns(
            _evidence(59), roster=_roster(), as_of=AS_OF
        )


def test_clustering_rejects_future_availability_and_newer_source():
    roster = _roster()
    evidence = _evidence()
    future_source_as_of = AS_OF + timedelta(days=1)
    newer = _evidence(source_as_of=future_source_as_of)
    with pytest.raises(SleeveClusteringError, match="newer than"):
        cluster_sleeve_active_returns(newer, roster=roster, as_of=AS_OF)

    first = evidence[0]
    observations = [row.model_dump(mode="python") for row in first.observations]
    observations[-1]["available_at"] = AS_OF + timedelta(seconds=1)
    with pytest.raises(SleeveClusteringError, match="PIT/content binding"):
        build_active_return_series(
            sleeve_id=first.sleeve_id,
            source_id=first.source.source_id,
            source_content_hash=first.source.content_hash,
            source_as_of=AS_OF,
            source_kind=first.source.source_kind,
            observations=observations,
        )


def test_roster_and_cluster_manifests_are_idempotently_persisted_without_promotion(
    tmp_path: Path,
) -> None:
    roster = _roster()
    clusters = cluster_sleeve_active_returns(
        _evidence(), roster=roster, as_of=AS_OF, correlation_threshold=0.95
    )
    with ResearchCatalog(tmp_path / "catalog.sqlite") as catalog:
        catalog.initialize_schema()
        first_roster = persist_sleeve_roster(catalog, roster, recorded_at=AS_OF)
        second_roster = persist_sleeve_roster(catalog, roster, recorded_at=AS_OF)
        cluster_run = persist_sleeve_cluster_manifest(
            catalog, clusters, recorded_at=AS_OF
        )

        assert first_roster.run_id == second_roster.run_id
        assert first_roster.metadata["authority"] == (
            "registered_research_only_no_promotion"
        )
        assert cluster_run.metadata["authority"] == (
            "research_dedup_only_no_promotion"
        )
        assert len(catalog.list_runs(run_type="sleeve_roster")) == 1
        assert len(catalog.list_runs(run_type="sleeve_cluster_manifest")) == 1
        assert catalog.list_experiments() == []
        assert catalog.list_trials() == []
