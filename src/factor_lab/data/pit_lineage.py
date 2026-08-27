"""Fail-closed point-in-time lineage contracts for investment claims.

The contract is deliberately metadata-only.  It does not infer point-in-time
safety from column names, download timestamps, or a successful backtest.  A
caller must explicitly attest every required field and each recursive
dependency before an investment claim can be allowed.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, Mapping


PIT_CONTRACT_SCHEMA_VERSION = 3
PIT_STATUS_VERIFIED = "verified"
PIT_STATUS_UNVERIFIED = "unverified"

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_INGESTION_AVAILABILITY_PATTERN = re.compile(
    r"(?:^|[^a-z0-9])ingested_at(?:$|[^a-z0-9])",
    flags=re.IGNORECASE,
)
_UNVERIFIED_POLICY_PATTERN = re.compile(
    r"(?:unverified|unavailable|unbound|not_attested|unknown|current_vintage)",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class PITFieldLineage:
    """Lineage and availability semantics for one feature-store field."""

    source: str
    dependencies: tuple[str, ...]
    availability: str
    revision_policy: str
    pit_status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "dependencies": list(self.dependencies),
            "availability": self.availability,
            "revision_policy": self.revision_policy,
            "pit_status": self.pit_status,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "PITFieldLineage":
        dependencies = payload.get("dependencies") or ()
        if (
            isinstance(dependencies, (str, bytes, Mapping))
            or not isinstance(dependencies, Iterable)
        ):
            raise ValueError("field dependencies must be an iterable of field names")
        return cls(
            source=str(payload.get("source") or "").strip(),
            dependencies=tuple(str(value).strip() for value in dependencies),
            availability=str(payload.get("availability") or "").strip(),
            revision_policy=str(payload.get("revision_policy") or "").strip(),
            pit_status=str(payload.get("pit_status") or "").strip(),
        )


@dataclass(frozen=True)
class PITLineageContract:
    """JSON-serializable lineage envelope for one decision artifact."""

    artifact_sha256: str
    execution_artifact_sha256: str
    suspension_artifact_sha256: str | None
    builder_sha256: str
    calendar_sha256: str
    universe_sha256: str
    fields: Mapping[str, PITFieldLineage]
    schema_version: int = PIT_CONTRACT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "artifact_sha256": self.artifact_sha256,
            "execution_artifact_sha256": self.execution_artifact_sha256,
            "suspension_artifact_sha256": self.suspension_artifact_sha256,
            "builder_sha256": self.builder_sha256,
            "calendar_sha256": self.calendar_sha256,
            "universe_sha256": self.universe_sha256,
            "fields": {
                name: lineage.to_dict()
                for name, lineage in sorted(self.fields.items())
            },
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "PITLineageContract":
        raw_fields = payload.get("fields")
        if not isinstance(raw_fields, Mapping):
            raise ValueError("PIT lineage contract requires a fields mapping")
        fields: dict[str, PITFieldLineage] = {}
        for raw_name, raw_lineage in raw_fields.items():
            name = str(raw_name).strip()
            if not name:
                raise ValueError("PIT lineage field names cannot be empty")
            if not isinstance(raw_lineage, Mapping):
                raise ValueError(f"PIT lineage field {name!r} must be a mapping")
            fields[name] = PITFieldLineage.from_dict(raw_lineage)
        return cls(
            schema_version=int(payload.get("schema_version") or 0),
            artifact_sha256=str(payload.get("artifact_sha256") or "").strip(),
            execution_artifact_sha256=str(
                payload.get("execution_artifact_sha256") or ""
            ).strip(),
            suspension_artifact_sha256=(
                str(payload["suspension_artifact_sha256"]).strip()
                if payload.get("suspension_artifact_sha256") is not None
                else None
            ),
            builder_sha256=str(payload.get("builder_sha256") or "").strip(),
            calendar_sha256=str(payload.get("calendar_sha256") or "").strip(),
            universe_sha256=str(payload.get("universe_sha256") or "").strip(),
            fields=fields,
        )


def _blocker(
    code: str,
    *,
    detail: str,
    field: str | None = None,
    dependency_path: Iterable[str] = (),
) -> dict[str, Any]:
    result: dict[str, Any] = {"code": code, "detail": detail}
    if field is not None:
        result["field"] = field
    path = tuple(dependency_path)
    if path:
        result["dependency_path"] = list(path)
    return result


def _availability_uses_ingested_at(value: str) -> bool:
    return bool(_INGESTION_AVAILABILITY_PATTERN.search(value))


def audit_pit_lineage(
    contract: PITLineageContract | Mapping[str, Any],
    required_fields: Iterable[str],
) -> dict[str, Any]:
    """Audit whether *required_fields* may support an investment claim.

    Every required field and recursive dependency must be explicitly marked
    ``verified``.  ``ingested_at`` is provenance metadata about the local pull;
    it is never accepted as historical market availability, even if a field is
    otherwise marked verified.  Malformed contracts return a blocker instead
    of failing open.
    """

    try:
        payload = contract.to_dict() if isinstance(contract, PITLineageContract) else contract
        resolved = PITLineageContract.from_dict(payload)
    except (AttributeError, TypeError, ValueError) as exc:
        return {
            "schema_version": PIT_CONTRACT_SCHEMA_VERSION,
            "investment_claim_allowed": False,
            "required_fields": [],
            "audited_fields": [],
            "blockers": [
                _blocker(
                    "invalid_contract",
                    detail=f"{type(exc).__name__}: {exc}",
                )
            ],
        }

    try:
        required_values = (
            (required_fields,)
            if isinstance(required_fields, (str, bytes))
            else required_fields
        )
        normalized_required = tuple(
            dict.fromkeys(
                str(value).strip() for value in required_values if str(value).strip()
            )
        )
    except TypeError as exc:
        return {
            "schema_version": PIT_CONTRACT_SCHEMA_VERSION,
            "investment_claim_allowed": False,
            "required_fields": [],
            "audited_fields": [],
            "blockers": [
                _blocker(
                    "invalid_required_fields",
                    detail=f"{type(exc).__name__}: {exc}",
                )
            ],
        }
    blockers: list[dict[str, Any]] = []
    audited: list[str] = []

    if resolved.schema_version != PIT_CONTRACT_SCHEMA_VERSION:
        blockers.append(
            _blocker(
                "unsupported_schema_version",
                detail=(
                    f"expected {PIT_CONTRACT_SCHEMA_VERSION}, "
                    f"received {resolved.schema_version}"
                ),
            )
        )
    for name, value in (
        ("artifact_sha256", resolved.artifact_sha256),
        ("execution_artifact_sha256", resolved.execution_artifact_sha256),
        ("builder_sha256", resolved.builder_sha256),
        ("calendar_sha256", resolved.calendar_sha256),
        ("universe_sha256", resolved.universe_sha256),
    ):
        if not _SHA256_PATTERN.fullmatch(value):
            blockers.append(
                _blocker(
                    "invalid_contract_hash",
                    field=name,
                    detail=f"{name} must be a lowercase 64-character SHA-256 digest",
                )
            )
    if resolved.suspension_artifact_sha256 is None:
        blockers.append(
            _blocker(
                "missing_contract_artifact",
                field="suspension_artifact_sha256",
                detail="authoritative suspension artifact is unavailable",
            )
        )
    elif not _SHA256_PATTERN.fullmatch(resolved.suspension_artifact_sha256):
        blockers.append(
            _blocker(
                "invalid_contract_hash",
                field="suspension_artifact_sha256",
                detail=(
                    "suspension_artifact_sha256 must be a lowercase "
                    "64-character SHA-256 digest"
                ),
            )
        )
    if not normalized_required:
        blockers.append(
            _blocker(
                "required_fields_empty",
                detail="an investment claim must declare at least one required field",
            )
        )

    visited: set[str] = set()

    def visit(field: str, path: tuple[str, ...]) -> None:
        if field in path:
            cycle = (*path[path.index(field) :], field)
            blockers.append(
                _blocker(
                    "dependency_cycle",
                    field=field,
                    dependency_path=cycle,
                    detail="recursive field lineage contains a dependency cycle",
                )
            )
            return
        if field in visited:
            return
        visited.add(field)
        audited.append(field)
        lineage = resolved.fields.get(field)
        current_path = (*path, field)
        if lineage is None:
            blockers.append(
                _blocker(
                    "missing_field_contract",
                    field=field,
                    dependency_path=current_path,
                    detail="required field or dependency has no lineage entry",
                )
            )
            return
        if not lineage.source:
            blockers.append(
                _blocker(
                    "missing_field_source",
                    field=field,
                    dependency_path=current_path,
                    detail="field source is empty",
                )
            )
        if not lineage.availability:
            blockers.append(
                _blocker(
                    "missing_availability_policy",
                    field=field,
                    dependency_path=current_path,
                    detail="field has no historical availability policy",
                )
            )
        elif _availability_uses_ingested_at(lineage.availability):
            blockers.append(
                _blocker(
                    "ingested_at_is_not_historical_availability",
                    field=field,
                    dependency_path=current_path,
                    detail=(
                        "ingested_at records the local download time and cannot establish "
                        "when a historical value was available to the market"
                    ),
                )
            )
        if not lineage.revision_policy:
            blockers.append(
                _blocker(
                    "missing_revision_policy",
                    field=field,
                    dependency_path=current_path,
                    detail="field has no revision/vintage policy",
                )
            )
        if (
            lineage.pit_status.casefold() == PIT_STATUS_VERIFIED
            and _UNVERIFIED_POLICY_PATTERN.search(
                f"{lineage.availability} {lineage.revision_policy}"
            )
        ):
            blockers.append(
                _blocker(
                    "verified_status_conflicts_with_policy",
                    field=field,
                    dependency_path=current_path,
                    detail=(
                        "a verified field cannot retain an unavailable, unbound, "
                        "unknown, current-vintage, or unattested policy"
                    ),
                )
            )
        if lineage.pit_status.casefold() != PIT_STATUS_VERIFIED:
            blockers.append(
                _blocker(
                    "field_not_verified",
                    field=field,
                    dependency_path=current_path,
                    detail=f"pit_status={lineage.pit_status!r}; expected 'verified'",
                )
            )
        for dependency in lineage.dependencies:
            if not dependency:
                blockers.append(
                    _blocker(
                        "empty_dependency_name",
                        field=field,
                        dependency_path=current_path,
                        detail="field contains an empty dependency name",
                    )
                )
                continue
            visit(dependency, current_path)

    for field in normalized_required:
        visit(field, ())

    unique_blockers: list[dict[str, Any]] = []
    seen_blockers: set[tuple[Any, ...]] = set()
    for blocker in blockers:
        key = (
            blocker.get("code"),
            blocker.get("field"),
            tuple(blocker.get("dependency_path") or ()),
            blocker.get("detail"),
        )
        if key not in seen_blockers:
            seen_blockers.add(key)
            unique_blockers.append(blocker)
    return {
        "schema_version": PIT_CONTRACT_SCHEMA_VERSION,
        "investment_claim_allowed": not unique_blockers,
        "required_fields": list(normalized_required),
        "audited_fields": audited,
        "blockers": unique_blockers,
    }


DEFAULT_WALK_FORWARD_REQUIRED_FIELDS = (
    "earnings_yield",
    "pb",
    "book_yield",
    "volatility_20",
    "turnover_rate",
    "universe_member",
    "eligible",
    "open_adj",
    "adv_20",
    "is_one_price_limit_up",
    "is_one_price_limit_down",
    "is_suspended",
    "is_delisted",
)


def conservative_default_field_lineage() -> dict[str, PITFieldLineage]:
    """Return tracked, deliberately unverified lineage for the 4.0 defaults.

    These entries document the expected construction without attesting facts
    that the current copy-only canonical store cannot prove.  In particular,
    current-vintage vendor downloads and local ingestion timestamps never turn
    a field into a verified historical observation.
    """

    unverified = PIT_STATUS_UNVERIFIED
    return {
        "raw_open": PITFieldLineage(
            source="legacy_canonical_store:expected_tushare_daily",
            dependencies=(),
            availability="trade_date_open_expected_but_source_version_unbound",
            revision_policy="historical_vendor_revision_timestamp_unavailable",
            pit_status=unverified,
        ),
        "adj_factor": PITFieldLineage(
            source="legacy_canonical_store:expected_tushare_adj_factor",
            dependencies=(),
            availability="trade_date_eod_expected_but_source_version_unbound",
            revision_policy="adjustment_factor_vintage_unverified",
            pit_status=unverified,
        ),
        "open_adj": PITFieldLineage(
            source=(
                "legacy_canonical_store:mixed_akshare_hfq_and_"
                "tushare_raw_times_adj_factor_fallback"
            ),
            dependencies=(),
            availability=(
                "mixed_adjusted_open_source_and_effective_adjustment_state_"
                "not_attested"
            ),
            revision_policy=(
                "akshare_hfq_and_tushare_adjustment_factor_vintages_unverified"
            ),
            pit_status=unverified,
        ),
        "amount_rmb": PITFieldLineage(
            source="legacy_canonical_store:expected_tushare_daily",
            dependencies=(),
            availability="trade_date_eod_expected_but_source_version_unbound",
            revision_policy="historical_vendor_revision_timestamp_unavailable",
            pit_status=unverified,
        ),
        "adv_20": PITFieldLineage(
            source="legacy_canonical_store:derived_feature",
            dependencies=("amount_rmb",),
            availability="rolling_window_strictly_before_execution_open_not_attested",
            revision_policy="inherits_dependency_vintage",
            pit_status=unverified,
        ),
        "pe_ttm": PITFieldLineage(
            source="legacy_canonical_store:expected_tushare_daily_basic",
            dependencies=(),
            availability="trade_date_eod_expected_but_source_version_unbound",
            revision_policy="historical_vendor_revision_timestamp_unavailable",
            pit_status=unverified,
        ),
        "pb": PITFieldLineage(
            source="legacy_canonical_store:expected_tushare_daily_basic",
            dependencies=(),
            availability="trade_date_eod_expected_but_source_version_unbound",
            revision_policy="historical_vendor_revision_timestamp_unavailable",
            pit_status=unverified,
        ),
        "earnings_yield": PITFieldLineage(
            source="legacy_canonical_store:derived_feature",
            dependencies=("pe_ttm",),
            availability="derived_from_dependencies_not_attested",
            revision_policy="inherits_dependency_vintage",
            pit_status=unverified,
        ),
        "book_yield": PITFieldLineage(
            source="legacy_canonical_store:derived_feature",
            dependencies=("pb",),
            availability="derived_from_dependencies_not_attested",
            revision_policy="inherits_dependency_vintage",
            pit_status=unverified,
        ),
        "close_adj": PITFieldLineage(
            source=(
                "legacy_canonical_store:mixed_akshare_hfq_and_"
                "tushare_raw_times_adj_factor_fallback"
            ),
            dependencies=(),
            availability="trade_date_adjusted_close_source_vintage_not_attested",
            revision_policy=(
                "akshare_hfq_and_tushare_adjustment_factor_vintages_unverified"
            ),
            pit_status=unverified,
        ),
        "return_1d": PITFieldLineage(
            source="legacy_canonical_store:derived_feature",
            dependencies=("close_adj",),
            availability="derived_from_dependencies_not_attested",
            revision_policy="inherits_dependency_vintage",
            pit_status=unverified,
        ),
        "momentum_120": PITFieldLineage(
            source="legacy_canonical_store:derived_feature",
            dependencies=("close_adj",),
            availability="rolling_window_ends_at_signal_date_not_attested",
            revision_policy="inherits_dependency_vintage",
            pit_status=unverified,
        ),
        "volatility_20": PITFieldLineage(
            source="legacy_canonical_store:derived_feature",
            dependencies=("return_1d",),
            availability="rolling_window_ends_at_signal_date_not_attested",
            revision_policy="inherits_dependency_vintage",
            pit_status=unverified,
        ),
        "turnover_rate": PITFieldLineage(
            source="legacy_canonical_store:expected_tushare_daily_basic",
            dependencies=(),
            availability="trade_date_eod_expected_but_source_version_unbound",
            revision_policy="historical_vendor_revision_timestamp_unavailable",
            pit_status=unverified,
        ),
        "financial_ann_date": PITFieldLineage(
            source="tushare_fina_indicator:ann_date",
            dependencies=(),
            availability="vendor_announcement_date_not_attested",
            revision_policy="historical_financial_revision_timestamp_unavailable",
            pit_status=unverified,
        ),
        "financial_available_date": PITFieldLineage(
            source="canonical_enrichment:next_trading_session_availability",
            dependencies=("financial_ann_date",),
            availability="first_trading_session_after_announcement_not_attested",
            revision_policy="inherits_financial_announcement_vintage",
            pit_status=unverified,
        ),
        "as_of_date": PITFieldLineage(
            source="legacy_canonical_store:monthly_top500_membership",
            dependencies=(),
            availability="membership_snapshot_date_not_attested",
            revision_policy="legacy_membership_builder_lineage_unbound",
            pit_status=unverified,
        ),
        "liquidity_window_end": PITFieldLineage(
            source="legacy_canonical_store:monthly_top500_membership",
            dependencies=("as_of_date",),
            availability="membership_selection_window_not_attested",
            revision_policy="legacy_membership_builder_lineage_unbound",
            pit_status=unverified,
        ),
        "effective_start_date": PITFieldLineage(
            source="legacy_canonical_store:monthly_top500_membership",
            dependencies=("as_of_date", "liquidity_window_end"),
            availability="membership_effective_interval_not_attested",
            revision_policy="legacy_membership_builder_lineage_unbound",
            pit_status=unverified,
        ),
        "effective_end_date": PITFieldLineage(
            source="legacy_canonical_store:monthly_top500_membership",
            dependencies=("effective_start_date",),
            availability="membership_effective_interval_not_attested",
            revision_policy="legacy_membership_builder_lineage_unbound",
            pit_status=unverified,
        ),
        "universe_member": PITFieldLineage(
            source=(
                "canonical_membership_and_execution:"
                "interval_join_plus_runtime_event_only_projection"
            ),
            # Execution event-only rows are explicitly projected out of the
            # canonical membership base for both delists and suspensions.
            dependencies=(
                "as_of_date",
                "liquidity_window_end",
                "effective_start_date",
                "effective_end_date",
                "is_suspended",
                "is_delisted",
            ),
            availability="decision_date_within_membership_interval_not_attested",
            revision_policy="inherits_membership_vintage",
            pit_status=unverified,
        ),
        "state_available_date": PITFieldLineage(
            source="tushare_bak_basic:monthly_reference_snapshot",
            dependencies=(),
            availability="prior_month_state_effective_next_month_not_attested",
            revision_policy="historical_snapshot_revision_timestamp_unavailable",
            pit_status=unverified,
        ),
        "reference_verified_pit": PITFieldLineage(
            source="canonical_enrichment:monthly_reference_join",
            dependencies=("state_available_date",),
            availability="derived_from_monthly_state_not_attested",
            revision_policy="inherits_reference_snapshot_vintage",
            pit_status=unverified,
        ),
        "is_st_pit": PITFieldLineage(
            source="canonical_enrichment:monthly_name_marker",
            dependencies=("state_available_date",),
            availability="monthly_state_only_daily_events_unavailable",
            revision_policy="daily_security_event_history_unavailable",
            pit_status=unverified,
        ),
        "eligible_pre_pit": PITFieldLineage(
            source="legacy_canonical_store:pre_enrichment_eligibility",
            dependencies=(),
            availability="historical_eligibility_inputs_unknown",
            revision_policy="legacy_eligibility_builder_lineage_unbound",
            pit_status=unverified,
        ),
        "eligible": PITFieldLineage(
            source=(
                "canonical_enrichment_and_execution:"
                "eligibility_filter_plus_runtime_event_only_projection"
            ),
            # Feature, execution, and membership enrichment apply slightly
            # different masks, while execution event-only rows are projected
            # out for delists and suspensions.  This union conservatively binds
            # every input used by any path without attesting it.
            dependencies=(
                "eligible_pre_pit",
                "is_suspended",
                "is_delisted",
                "universe_member",
                "reference_verified_pit",
                "is_st_pit",
            ),
            availability="derived_from_dependencies_not_attested",
            revision_policy="inherits_dependency_vintage",
            pit_status=unverified,
        ),
        "is_one_price_limit_up": PITFieldLineage(
            source="legacy_canonical_store:daily_price_limit_state",
            dependencies=("raw_open",),
            availability="execution_session_open_state_not_attested",
            revision_policy="source_version_unbound",
            pit_status=unverified,
        ),
        "is_one_price_limit_down": PITFieldLineage(
            source="legacy_canonical_store:daily_price_limit_state",
            dependencies=("raw_open",),
            availability="execution_session_open_state_not_attested",
            revision_policy="source_version_unbound",
            pit_status=unverified,
        ),
        "suspend_type": PITFieldLineage(
            source="tushare_suspend_d:suspend_type",
            dependencies=(),
            availability="trade_date_event_state_not_attested",
            revision_policy="historical_suspend_d_vintage_unverified",
            pit_status=unverified,
        ),
        "suspend_timing": PITFieldLineage(
            source="tushare_suspend_d:suspend_timing",
            dependencies=(),
            availability="same_session_intraday_interval_not_attested",
            revision_policy="historical_suspend_d_vintage_unverified",
            pit_status=unverified,
        ),
        "is_suspended": PITFieldLineage(
            source="runtime_projection:tushare_suspend_d_open_state",
            # A delist dominates a same-session suspension event and forces
            # the projected suspension flag false.
            dependencies=("suspend_type", "suspend_timing", "is_delisted"),
            availability="S_event_blocks_open_only_if_full_day_or_covering_09:30_not_attested",
            revision_policy="inherits_suspend_d_artifact_vintage",
            pit_status=unverified,
        ),
        "delist_date": PITFieldLineage(
            source="legacy_canonical_store:current_listing_vintage",
            dependencies=(),
            availability="exchange_event_date_not_attested",
            revision_policy="current_vintage_listing_status_unverified",
            pit_status=unverified,
        ),
        "is_delisted": PITFieldLineage(
            source="feature_store:delist_date_event_projection",
            dependencies=("delist_date",),
            availability="first_session_on_or_after_event_date_not_attested",
            revision_policy="inherits_delist_date_vintage",
            pit_status=unverified,
        ),
        "split_ratio": PITFieldLineage(
            source="security_events:split_ratio",
            dependencies=(),
            availability="effective_session_security_event_not_attested",
            revision_policy="historical_event_source_unbound",
            pit_status=unverified,
        ),
        "cash_dividend": PITFieldLineage(
            source="security_events:cash_dividend_per_share",
            dependencies=(),
            availability="effective_session_security_event_not_attested",
            revision_policy="historical_event_source_unbound",
            pit_status=unverified,
        ),
    }


_EXECUTION_FIELD_ALIASES = {
    "open_price": "open_adj",
    "open": "open_adj",
    "amount_20d_avg": "adv_20",
    "adv": "adv_20",
    "average_daily_value": "adv_20",
    "volatility": "volatility_20",
    "vol_20": "volatility_20",
    "one_price_limit_up": "is_one_price_limit_up",
    "limit_up": "is_one_price_limit_up",
    "is_limit_up": "is_one_price_limit_up",
    "up_limit_locked": "is_one_price_limit_up",
    "one_price_limit_down": "is_one_price_limit_down",
    "limit_down": "is_one_price_limit_down",
    "is_limit_down": "is_one_price_limit_down",
    "down_limit_locked": "is_one_price_limit_down",
    "suspended": "is_suspended",
    "is_pause": "is_suspended",
    "paused": "is_suspended",
    "delisted": "is_delisted",
    "delist_flag": "is_delisted",
    "share_split_ratio": "split_ratio",
    "cash_dividend_per_share": "cash_dividend",
}


def _with_execution_alias_lineage(
    fields: Mapping[str, PITFieldLineage],
) -> dict[str, PITFieldLineage]:
    """Add explicit fail-closed entries for execution-column aliases."""

    result = dict(fields)
    for alias, canonical in _EXECUTION_FIELD_ALIASES.items():
        result[alias] = PITFieldLineage(
            source=f"execution_store:alias:{alias}",
            dependencies=(canonical,),
            availability="inherits_canonical_field_availability_not_attested",
            revision_policy="inherits_canonical_field_vintage",
            pit_status=PIT_STATUS_UNVERIFIED,
        )
    return result


def conservative_default_contract(
    *,
    artifact_sha256: str,
    execution_artifact_sha256: str,
    suspension_artifact_sha256: str | None,
    builder_sha256: str,
    calendar_sha256: str,
    universe_sha256: str,
) -> PITLineageContract:
    """Build a tracked contract that intentionally blocks the current defaults."""

    return PITLineageContract(
        artifact_sha256=artifact_sha256,
        execution_artifact_sha256=execution_artifact_sha256,
        suspension_artifact_sha256=suspension_artifact_sha256,
        builder_sha256=builder_sha256,
        calendar_sha256=calendar_sha256,
        universe_sha256=universe_sha256,
        fields=_with_execution_alias_lineage(
            conservative_default_field_lineage()
        ),
    )


__all__ = [
    "DEFAULT_WALK_FORWARD_REQUIRED_FIELDS",
    "PIT_CONTRACT_SCHEMA_VERSION",
    "PIT_STATUS_UNVERIFIED",
    "PIT_STATUS_VERIFIED",
    "PITFieldLineage",
    "PITLineageContract",
    "audit_pit_lineage",
    "conservative_default_contract",
    "conservative_default_field_lineage",
]
