from __future__ import annotations

import json

from factor_lab.data import (
    DEFAULT_WALK_FORWARD_REQUIRED_FIELDS,
    PITFieldLineage,
    PITLineageContract,
    audit_pit_lineage,
    conservative_default_contract,
)


_HASHES = {
    "artifact_sha256": "a" * 64,
    "execution_artifact_sha256": "e" * 64,
    "suspension_artifact_sha256": "f" * 64,
    "builder_sha256": "b" * 64,
    "calendar_sha256": "c" * 64,
    "universe_sha256": "d" * 64,
}


def _field(
    *,
    dependencies: tuple[str, ...] = (),
    availability: str = "source_available_at <= signal_time",
    status: str = "verified",
) -> PITFieldLineage:
    return PITFieldLineage(
        source="immutable_test_snapshot",
        dependencies=dependencies,
        availability=availability,
        revision_policy="immutable_versioned_values",
        pit_status=status,
    )


def _contract(fields: dict[str, PITFieldLineage], **overrides: str) -> PITLineageContract:
    hashes = {**_HASHES, **overrides}
    return PITLineageContract(fields=fields, **hashes)


def test_contract_round_trips_through_json() -> None:
    contract = _contract({"alpha": _field(dependencies=("raw",)), "raw": _field()})

    payload = json.loads(json.dumps(contract.to_dict()))
    restored = PITLineageContract.from_dict(payload)

    assert restored.to_dict() == contract.to_dict()
    assert audit_pit_lineage(restored, ["alpha"])["investment_claim_allowed"] is True


def test_recursive_unverified_dependency_blocks_claim_with_path() -> None:
    contract = _contract(
        {
            "alpha": _field(dependencies=("raw",)),
            "raw": _field(status="unverified"),
        }
    )

    audit = audit_pit_lineage(contract, ["alpha"])

    assert audit["investment_claim_allowed"] is False
    assert {
        (row["code"], row.get("field"), tuple(row.get("dependency_path") or ()))
        for row in audit["blockers"]
    } >= {("field_not_verified", "raw", ("alpha", "raw"))}


def test_missing_recursive_dependency_fails_closed() -> None:
    audit = audit_pit_lineage(
        _contract({"alpha": _field(dependencies=("missing_raw",))}),
        ["alpha"],
    )

    assert audit["investment_claim_allowed"] is False
    assert any(
        row["code"] == "missing_field_contract"
        and row["field"] == "missing_raw"
        and row["dependency_path"] == ["alpha", "missing_raw"]
        for row in audit["blockers"]
    )


def test_ingested_at_never_counts_as_historical_availability() -> None:
    contract = _contract(
        {
            "alpha": _field(
                availability="ingested_at <= signal_time",
                status="verified",
            )
        }
    )

    audit = audit_pit_lineage(contract, ["alpha"])

    assert audit["investment_claim_allowed"] is False
    assert any(
        row["code"] == "ingested_at_is_not_historical_availability"
        and row["field"] == "alpha"
        for row in audit["blockers"]
    )


def test_verified_label_cannot_override_an_explicitly_unverified_policy() -> None:
    contract = _contract(
        {
            "alpha": PITFieldLineage(
                source="current_vendor_snapshot",
                dependencies=(),
                availability="historical availability unknown",
                revision_policy="current_vintage_without_revision_time",
                pit_status="verified",
            )
        }
    )

    audit = audit_pit_lineage(contract, "alpha")

    assert audit["investment_claim_allowed"] is False
    assert any(
        row["code"] == "verified_status_conflicts_with_policy"
        and row["field"] == "alpha"
        for row in audit["blockers"]
    )


def test_invalid_contract_hash_and_dependency_cycle_are_blockers() -> None:
    contract = _contract(
        {
            "alpha": _field(dependencies=("raw",)),
            "raw": _field(dependencies=("alpha",)),
        },
        universe_sha256="not-a-sha256",
    )

    audit = audit_pit_lineage(contract, ["alpha"])

    assert audit["investment_claim_allowed"] is False
    assert {row["code"] for row in audit["blockers"]} >= {
        "invalid_contract_hash",
        "dependency_cycle",
    }


def test_feature_and_execution_artifact_identities_are_both_required() -> None:
    payload = _contract({"alpha": _field()}).to_dict()
    payload["execution_artifact_sha256"] = ""

    audit = audit_pit_lineage(payload, ["alpha"])

    assert audit["investment_claim_allowed"] is False
    assert any(
        row["code"] == "invalid_contract_hash"
        and row["field"] == "execution_artifact_sha256"
        for row in audit["blockers"]
    )


def test_missing_authoritative_suspension_artifact_fails_closed() -> None:
    payload = _contract({"alpha": _field()}).to_dict()
    payload["suspension_artifact_sha256"] = None

    audit = audit_pit_lineage(payload, ["alpha"])

    assert audit["investment_claim_allowed"] is False
    assert any(
        row["code"] == "missing_contract_artifact"
        and row["field"] == "suspension_artifact_sha256"
        for row in audit["blockers"]
    )


def test_current_default_fields_are_tracked_without_false_certification() -> None:
    contract = conservative_default_contract(**_HASHES)

    assert set(DEFAULT_WALK_FORWARD_REQUIRED_FIELDS).issubset(contract.fields)
    assert all(
        contract.fields[name].pit_status == "unverified"
        for name in DEFAULT_WALK_FORWARD_REQUIRED_FIELDS
    )
    audit = audit_pit_lineage(contract, DEFAULT_WALK_FORWARD_REQUIRED_FIELDS)
    encoded = json.dumps(contract.to_dict())

    assert audit["investment_claim_allowed"] is False
    assert "ingested_at" not in encoded
    assert {
        row["field"]
        for row in audit["blockers"]
        if row["code"] == "field_not_verified"
    } >= set(DEFAULT_WALK_FORWARD_REQUIRED_FIELDS)
    assert contract.fields["is_delisted"].dependencies == ("delist_date",)
    assert contract.fields["is_suspended"].dependencies == (
        "suspend_type",
        "suspend_timing",
        "is_delisted",
    )
    assert "delist_date" in audit["audited_fields"]
    assert {"split_ratio", "cash_dividend"}.issubset(contract.fields)
    assert contract.fields["share_split_ratio"].dependencies == ("split_ratio",)
    assert contract.fields["cash_dividend_per_share"].dependencies == (
        "cash_dividend",
    )
    assert contract.fields["open_adj"].dependencies == ()
    assert contract.fields["open_adj"].source == (
        "legacy_canonical_store:mixed_akshare_hfq_and_"
        "tushare_raw_times_adj_factor_fallback"
    )
    assert "vintages_unverified" in contract.fields["open_adj"].revision_policy
    assert contract.fields["financial_available_date"].dependencies == (
        "financial_ann_date",
    )


def test_financial_availability_recursively_audits_announcement_lineage() -> None:
    contract = conservative_default_contract(**_HASHES)

    audit = audit_pit_lineage(contract, ["financial_available_date"])

    assert audit["investment_claim_allowed"] is False
    assert "financial_ann_date" in audit["audited_fields"]
    assert any(
        row["code"] == "field_not_verified"
        and row["field"] == "financial_ann_date"
        and row["dependency_path"]
        == ["financial_available_date", "financial_ann_date"]
        for row in audit["blockers"]
    )


def test_eligibility_recursively_audits_pre_enrichment_lineage() -> None:
    contract = conservative_default_contract(**_HASHES)

    assert contract.fields["eligible"].dependencies == (
        "eligible_pre_pit",
        "is_suspended",
        "is_delisted",
        "universe_member",
        "reference_verified_pit",
        "is_st_pit",
    )
    pre_enrichment = contract.fields["eligible_pre_pit"]
    assert pre_enrichment.pit_status == "unverified"
    assert "unknown" in pre_enrichment.availability
    assert "unbound" in pre_enrichment.revision_policy

    audit = audit_pit_lineage(contract, ["eligible"])

    assert audit["investment_claim_allowed"] is False
    assert "eligible_pre_pit" in audit["audited_fields"]
    assert any(
        row["code"] == "field_not_verified"
        and row["field"] == "eligible_pre_pit"
        and row["dependency_path"] == ["eligible", "eligible_pre_pit"]
        for row in audit["blockers"]
    )


def test_runtime_security_event_projection_is_recursively_bound() -> None:
    contract = conservative_default_contract(**_HASHES)

    assert contract.fields["is_suspended"].dependencies == (
        "suspend_type",
        "suspend_timing",
        "is_delisted",
    )
    assert {"is_suspended", "is_delisted"}.issubset(
        contract.fields["eligible"].dependencies
    )
    assert {"is_suspended", "is_delisted"}.issubset(
        contract.fields["universe_member"].dependencies
    )

    suspension_audit = audit_pit_lineage(contract, ["is_suspended"])
    eligibility_audit = audit_pit_lineage(contract, ["eligible"])
    membership_audit = audit_pit_lineage(contract, ["universe_member"])

    assert all(
        audit["investment_claim_allowed"] is False
        for audit in (suspension_audit, eligibility_audit, membership_audit)
    )
    assert any(
        row["field"] == "is_delisted"
        and row["dependency_path"] == ["is_suspended", "is_delisted"]
        for row in suspension_audit["blockers"]
    )
    assert any(
        row["field"] == "is_suspended"
        and row["dependency_path"] == ["eligible", "is_suspended"]
        for row in eligibility_audit["blockers"]
    )
    assert any(
        row["field"] == "is_suspended"
        and row["dependency_path"] == ["universe_member", "is_suspended"]
        for row in membership_audit["blockers"]
    )


def test_malformed_mapping_returns_a_blocker_instead_of_raising() -> None:
    audit = audit_pit_lineage({"schema_version": 1, "fields": []}, ["alpha"])

    assert audit["investment_claim_allowed"] is False
    assert audit["blockers"][0]["code"] == "invalid_contract"


def test_malformed_typed_contract_also_fails_closed() -> None:
    contract = PITLineageContract(
        fields={"alpha": {"pit_status": "verified"}},  # type: ignore[dict-item]
        **_HASHES,
    )

    audit = audit_pit_lineage(contract, ["alpha"])

    assert audit["investment_claim_allowed"] is False
    assert audit["blockers"][0]["code"] == "invalid_contract"
