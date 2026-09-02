from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from factor_lab.research.pit_stock_minute_scope import (
    FORMAL_SIGNAL_COUNT,
    FORMAL_STAGE1_PAIR_COUNT,
    FORMAL_STAGE1_PAYLOAD_SHA256,
    FORMAL_STAGE1_UNIQUE_TICKER_COUNT,
    FORMAL_STAGE2_PAIR_COUNT,
    FORMAL_STAGE2_PAYLOAD_SHA256,
    FORMAL_STAGE2_UNIQUE_TICKER_COUNT,
    STAGE1_RECORD_FIELDS,
    STAGE2_RECORD_FIELDS,
    MinuteScopeError,
    build_formal_development_scopes,
    canonical_scope_sha256,
    generate_stage1_candidate_scope,
    generate_stage2_all_roles_scope,
    verify_stage1_overlap,
)


ROOT = Path(__file__).resolve().parents[2]


SIGNALS = ("2020-09-30", "2020-03-31", "2020-06-30")
EXECUTIONS = {
    "2020-03-31": "2020-04-01",
    "2020-06-30": "2020-07-01",
    "2020-09-30": "2020-10-09",
}
CANDIDATES = {
    "2020-03-31": ("000002.SZ", "000001.SZ"),
    "2020-06-30": ("000002.SZ",),
    "2020-09-30": (),
}
ADV500 = {
    "2020-03-31": ("600000.SH",),
    "2020-06-30": ("600001.SH",),
    "2020-09-30": ("600000.SH",),
}
DELISTS = {
    "000001.SZ": None,
    # Equality is inactive: this name appears only at the first boundary.
    "000002.SZ": "2020-07-01",
    "600000.SH": None,
    # This ADV name is removed exactly at the mark-only sentinel.
    "600001.SH": "2020-10-09",
}


def _synthetic_scopes():
    stage1 = generate_stage1_candidate_scope(
        signal_dates=SIGNALS,
        execution_dates=EXECUTIONS,
        candidate_targets=CANDIDATES,
        delist_dates=DELISTS,
    )
    stage2 = generate_stage2_all_roles_scope(
        signal_dates=SIGNALS,
        execution_dates=EXECUTIONS,
        candidate_targets=CANDIDATES,
        adv500_targets=ADV500,
        delist_dates=DELISTS,
    )
    return stage1, stage2


def test_generators_freeze_schema_sort_delist_sentinel_and_hash() -> None:
    stage1, stage2 = _synthetic_scopes()

    assert stage1.record_fields == STAGE1_RECORD_FIELDS
    assert stage2.record_fields == STAGE2_RECORD_FIELDS
    assert all(tuple(row) == STAGE1_RECORD_FIELDS for row in stage1.records)
    assert all(tuple(row) == STAGE2_RECORD_FIELDS for row in stage2.records)
    assert list(stage1.records) == sorted(
        stage1.records,
        key=lambda row: (row["execution_date"], row["ticker"]),
    )
    assert list(stage2.records) == sorted(
        stage2.records,
        key=lambda row: (row["execution_date"], row["ticker"]),
    )

    assert stage1.pair_count == 4
    assert stage1.payload_sha256 == (
        "311c6fa7f437001c8f04ed56d0dbfc26bb94818148cd9f5556d8c1a0317823b7"
    )
    assert stage2.pair_count == 8
    assert stage2.payload_sha256 == (
        "29742496f806e044101a38ec41a3cdc5863ea73f7aa8ebfbd8e7000af8f960f2"
    )

    stage1_identities = {
        (row["execution_date"], row["ticker"]) for row in stage1.records
    }
    stage2_identities = {
        (row["execution_date"], row["ticker"]) for row in stage2.records
    }
    assert ("2020-07-01", "000002.SZ") not in stage1_identities
    assert ("2020-07-01", "000002.SZ") not in stage2_identities
    assert ("2020-10-09", "600001.SH") not in stage2_identities
    assert {
        row["execution_date"] for row in stage1.records if row["mark_only"]
    } == {"2020-10-09"}
    assert {
        row["execution_date"] for row in stage2.records if row["mark_only"]
    } == {"2020-10-09"}
    assert stage1_identities <= stage2_identities
    verify_stage1_overlap(stage1, stage2)


def test_generators_are_deterministic_under_input_order_permutations() -> None:
    stage1, stage2 = _synthetic_scopes()
    candidate_reordered = {
        key: tuple(reversed(value))
        for key, value in reversed(tuple(CANDIDATES.items()))
    }
    adv_reordered = {
        key: tuple(reversed(value)) for key, value in reversed(tuple(ADV500.items()))
    }
    delists_reordered = dict(reversed(tuple(DELISTS.items())))
    actual1 = generate_stage1_candidate_scope(
        signal_dates=reversed(SIGNALS),
        execution_dates=dict(reversed(tuple(EXECUTIONS.items()))),
        candidate_targets=candidate_reordered,
        delist_dates=delists_reordered,
    )
    actual2 = generate_stage2_all_roles_scope(
        signal_dates=reversed(SIGNALS),
        execution_dates=dict(reversed(tuple(EXECUTIONS.items()))),
        candidate_targets=candidate_reordered,
        adv500_targets=adv_reordered,
        delist_dates=delists_reordered,
    )
    assert actual1.records == stage1.records
    assert actual1.payload_sha256 == stage1.payload_sha256
    assert actual2.records == stage2.records
    assert actual2.payload_sha256 == stage2.payload_sha256


def test_schema_and_overlap_verifiers_fail_closed() -> None:
    stage1, stage2 = _synthetic_scopes()
    reordered = dict(reversed(tuple(stage1.records[0].items())))
    with pytest.raises(MinuteScopeError, match="fields or field order"):
        canonical_scope_sha256(
            [reordered], record_fields=STAGE1_RECORD_FIELDS
        )

    forged_records = [dict(row) for row in stage2.records]
    identity = (
        stage1.records[0]["signal_date"],
        stage1.records[0]["execution_date"],
        stage1.records[0]["ticker"],
    )
    for row in forged_records:
        if (row["signal_date"], row["execution_date"], row["ticker"]) == identity:
            row["in_candidate_target"] = not row["in_candidate_target"]
            break
    forged = replace(stage2, records=tuple(forged_records))
    with pytest.raises(MinuteScopeError, match="overlap semantics"):
        verify_stage1_overlap(stage1, forged)

    incomplete_delists = dict(DELISTS)
    incomplete_delists.pop("000001.SZ")
    with pytest.raises(MinuteScopeError, match="absent from the delist map"):
        generate_stage1_candidate_scope(
            signal_dates=SIGNALS,
            execution_dates=EXECUTIONS,
            candidate_targets=CANDIDATES,
            delist_dates=incomplete_delists,
        )


def test_formal_scopes_reproduce_frozen_counts_hashes_and_overlap() -> None:
    scopes = build_formal_development_scopes(ROOT)
    stage1 = scopes.stage1
    stage2 = scopes.stage2

    assert stage1.signal_count == stage2.signal_count == FORMAL_SIGNAL_COUNT
    assert stage1.pair_count == FORMAL_STAGE1_PAIR_COUNT == 4_729
    assert stage1.unique_ticker_count == FORMAL_STAGE1_UNIQUE_TICKER_COUNT == 501
    assert stage1.payload_sha256 == FORMAL_STAGE1_PAYLOAD_SHA256 == (
        "5860e7321107e4fa92be044d0fd027835ac650fa0f426e5c3a76d6edb45567e4"
    )
    assert stage2.pair_count == FORMAL_STAGE2_PAIR_COUNT == 33_984
    assert stage2.unique_ticker_count == FORMAL_STAGE2_UNIQUE_TICKER_COUNT == 2_252
    assert stage2.payload_sha256 == FORMAL_STAGE2_PAYLOAD_SHA256 == (
        "520bb9267bcdf6e0ee45c2724a5fb31389cd96cfe1ccc1115fa23535ffb1aa2e"
    )

    stage1_index = {
        (row["signal_date"], row["execution_date"], row["ticker"]): row
        for row in stage1.records
    }
    stage2_index = {
        (row["signal_date"], row["execution_date"], row["ticker"]): row
        for row in stage2.records
    }
    assert stage1_index.keys() <= stage2_index.keys()
    for identity, left in stage1_index.items():
        right = stage2_index[identity]
        assert right["in_candidate_target"] == left["in_current_target"]
        assert right["mark_only"] == left["mark_only"]
    verify_stage1_overlap(stage1, stage2)

    assert {
        row["execution_date"] for row in stage1.records if row["mark_only"]
    } == {"2023-01-03"}
    assert {
        row["execution_date"] for row in stage2.records if row["mark_only"]
    } == {"2023-01-03"}
