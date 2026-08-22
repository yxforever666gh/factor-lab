"""Typed, fail-closed data incidents and non-fictional de-risking intents.

The incident ledger deliberately separates *desired* 100% cash from actual
fills.  A source/Silver/DQ/Gold failure can freeze research and persist a cash
target immediately, but it cannot pretend that positions were liquidated when
trusted opening observations are unavailable.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from .catalog import LifecycleEvent, ResearchCatalog, ShadowEventInput
from .contracts import DataQualityStatus, LifecycleState, SnapshotTier
from .fingerprint import content_fingerprint
from .lifecycle import SleeveLifecycleRecord, SleeveState


class DataPipelineStage(str, Enum):
    SOURCE = "source"
    SILVER = "silver"
    DATA_QUALITY = "data_quality"
    GOLD = "gold"


@dataclass(frozen=True)
class DataIncident:
    stage: DataPipelineStage
    partition_key: str
    error_code: str
    message: str
    occurred_at: datetime
    source_ids: tuple[str, ...] = ()
    evidence_hashes: tuple[str, ...] = ()
    incident_id: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.stage, DataPipelineStage):
            object.__setattr__(self, "stage", DataPipelineStage(self.stage))
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("data incident occurred_at must include a timezone")
        if not self.partition_key.strip() or not self.error_code.strip() or not self.message.strip():
            raise ValueError("partition_key, error_code and message are required")
        sources = tuple(sorted({str(value).strip() for value in self.source_ids if str(value).strip()}))
        hashes = tuple(sorted({str(value).strip().lower() for value in self.evidence_hashes if str(value).strip()}))
        for value in hashes:
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError("evidence_hashes must contain lowercase SHA-256 digests")
        object.__setattr__(self, "source_ids", sources)
        object.__setattr__(self, "evidence_hashes", hashes)
        expected = "dinc_" + content_fingerprint(
            self.identity_payload(),
            domain="factor-lab/research-os/v1/data-incident",
        )[:32]
        if self.incident_id and self.incident_id != expected:
            raise ValueError("incident_id differs from typed incident content")
        object.__setattr__(self, "incident_id", expected)

    def identity_payload(self) -> dict[str, Any]:
        return {
            "stage": self.stage.value,
            "partition_key": self.partition_key,
            "error_code": self.error_code,
            "message": self.message,
            "occurred_at": self.occurred_at.astimezone(timezone.utc),
            "source_ids": self.source_ids,
            "evidence_hashes": self.evidence_hashes,
        }

    def to_dict(self) -> dict[str, Any]:
        return {"incident_id": self.incident_id, **self.identity_payload()}


@dataclass(frozen=True)
class CashTargetIntent:
    incident_id: str
    generated_at: datetime
    target_weights: Mapping[str, float]
    cash_weight: float
    execution_state: str
    actual_positions_unchanged: bool
    intent_id: str

    @classmethod
    def for_incident(cls, incident: DataIncident) -> "CashTargetIntent":
        payload = {
            "incident_id": incident.incident_id,
            "generated_at": incident.occurred_at,
            "target_weights": {},
            "cash_weight": 1.0,
            "execution_state": "awaiting_trusted_execution",
            "actual_positions_unchanged": True,
        }
        return cls(
            **payload,
            intent_id="cash_" + content_fingerprint(
                payload,
                domain="factor-lab/research-os/v1/cash-target-intent",
            )[:32],
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DataIncidentResult:
    incident: DataIncident
    cash_target_intent: CashTargetIntent
    lifecycle_records: tuple[SleeveLifecycleRecord, ...]
    shadow_account_ids: tuple[str, ...]


@dataclass(frozen=True)
class DataRevalidation:
    incident_id: str
    snapshot_id: str
    snapshot_content_hash: str
    occurred_at: datetime
    revalidation_id: str = ""

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("revalidation occurred_at must include a timezone")
        if not self.incident_id or not self.snapshot_id:
            raise ValueError("incident_id and snapshot_id are required")
        if len(self.snapshot_content_hash) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.snapshot_content_hash
        ):
            raise ValueError("snapshot_content_hash must be a lowercase SHA-256 digest")
        expected = "dreval_" + content_fingerprint(
            {
                "incident_id": self.incident_id,
                "snapshot_id": self.snapshot_id,
                "snapshot_content_hash": self.snapshot_content_hash,
                "occurred_at": self.occurred_at,
            },
            domain="factor-lab/research-os/v1/data-revalidation",
        )[:32]
        if self.revalidation_id and self.revalidation_id != expected:
            raise ValueError("revalidation_id differs from evidence content")
        object.__setattr__(self, "revalidation_id", expected)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DataIncidentCoordinator:
    """Persist one data-plane failure across lifecycle and shadow evidence."""

    def __init__(self, catalog: ResearchCatalog) -> None:
        self.catalog = catalog

    @staticmethod
    def _frozen_record(
        record: SleeveLifecycleRecord,
        incident: DataIncident,
    ) -> SleeveLifecycleRecord:
        if record.state is SleeveState.FROZEN_DATA:
            return replace(record, effective_weight=0.0)
        from .lifecycle import LifecycleTransition

        transition = LifecycleTransition(
            from_state=record.state,
            to_state=SleeveState.FROZEN_DATA,
            as_of_date=incident.occurred_at.date(),
            reasons=(f"{incident.stage.value}_data_incident", incident.error_code),
        )
        return replace(
            record,
            state=SleeveState.FROZEN_DATA,
            effective_weight=0.0,
            transitions=record.transitions + (transition,),
        )

    def report(
        self,
        incident: DataIncident,
        *,
        lifecycle_records: Sequence[SleeveLifecycleRecord],
        shadow_accounts: Mapping[str, Sequence[str]] | None = None,
    ) -> DataIncidentResult:
        if not lifecycle_records:
            raise ValueError("at least one Sleeve lifecycle record is required")
        intent = CashTargetIntent.for_incident(incident)
        shadow_accounts = shadow_accounts or {}
        updated: list[SleeveLifecycleRecord] = []
        account_ids: set[str] = set()
        for record in sorted(lifecycle_records, key=lambda item: item.sleeve_id):
            frozen = self._frozen_record(record, incident)
            updated.append(frozen)
            evidence = {
                "data_incident": incident.to_dict(),
                "cash_target_intent": intent.to_dict(),
            }
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=f"data-incident:{incident.incident_id}:{record.sleeve_id}",
                    sleeve_id=record.sleeve_id,
                    from_state=LifecycleState(record.state.value),
                    to_state=LifecycleState.FROZEN_DATA,
                    cause="data_integrity_failure",
                    occurred_at=incident.occurred_at,
                    evidence=evidence,
                )
            )
            for account_id in shadow_accounts.get(record.sleeve_id, ()):
                account_ids.add(str(account_id))

        for account_id in sorted(account_ids):
            account = self.catalog.get_shadow_account(account_id)
            if account is None:
                raise ValueError(f"shadow account {account_id!r} was not found")
            if incident.occurred_at < account.as_of:
                raise ValueError("data incident cannot be backdated before account evidence")
            existing = self.catalog.list_shadow_events_by_type(
                account_id=account_id,
                event_type="cash_target_intent",
                since=None,
                through=None,
                limit=1_000,
            )
            if any(
                event.event_type == "cash_target_intent"
                and event.payload.get("intent_id") == intent.intent_id
                for event in existing
            ):
                continue
            self.catalog.append_shadow_events_atomic(
                account_id=account_id,
                expected_previous_hash=account.last_event_hash,
                events=(
                    ShadowEventInput(
                        event_type="data_incident",
                        occurred_at=incident.occurred_at,
                        payload=incident.to_dict(),
                    ),
                    ShadowEventInput(
                        event_type="cash_target_intent",
                        occurred_at=incident.occurred_at,
                        payload=intent.to_dict(),
                    ),
                ),
            )
        return DataIncidentResult(
            incident=incident,
            cash_target_intent=intent,
            lifecycle_records=tuple(updated),
            shadow_account_ids=tuple(sorted(account_ids)),
        )

    def revalidate(
        self,
        evidence: DataRevalidation,
        *,
        lifecycle_records: Sequence[SleeveLifecycleRecord],
        shadow_accounts: Mapping[str, Sequence[str]] | None = None,
    ) -> tuple[SleeveLifecycleRecord, ...]:
        snapshot = self.catalog.get_snapshot(evidence.snapshot_id)
        if (
            snapshot is None
            or snapshot.reference.tier is not SnapshotTier.GOLD
            or snapshot.reference.quality_status is not DataQualityStatus.ACCEPTED
            or snapshot.reference.content_hash != evidence.snapshot_content_hash
        ):
            raise ValueError("revalidation requires the matching accepted Gold snapshot")
        if any(
            marker in str(label).lower()
            for label in snapshot.reference.trust_labels
            for marker in ("unverified", "disputed", "quarantined", "frozen")
        ):
            raise ValueError("revalidation snapshot still carries blocking trust labels")
        shadow_accounts = shadow_accounts or {}
        updated: list[SleeveLifecycleRecord] = []
        account_ids: set[str] = set()
        for record in sorted(lifecycle_records, key=lambda item: item.sleeve_id):
            if record.state is not SleeveState.FROZEN_DATA:
                raise ValueError("only frozen_data Sleeves can be explicitly revalidated")
            from .lifecycle import LifecycleTransition

            transition = LifecycleTransition(
                from_state=SleeveState.FROZEN_DATA,
                to_state=SleeveState.DORMANT,
                as_of_date=evidence.occurred_at.date(),
                reasons=("data_revalidation_passed",),
            )
            restored = replace(
                record,
                state=SleeveState.DORMANT,
                effective_weight=0.0,
                dormant_since=evidence.occurred_at.date(),
                transitions=record.transitions + (transition,),
            )
            updated.append(restored)
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=(
                        f"data-revalidation:{evidence.revalidation_id}:{record.sleeve_id}"
                    ),
                    sleeve_id=record.sleeve_id,
                    from_state=LifecycleState.FROZEN_DATA,
                    to_state=LifecycleState.DORMANT,
                    cause="data_revalidation_passed",
                    occurred_at=evidence.occurred_at,
                    evidence=evidence.to_dict(),
                )
            )
            account_ids.update(map(str, shadow_accounts.get(record.sleeve_id, ())))
        for account_id in sorted(account_ids):
            account = self.catalog.get_shadow_account(account_id)
            if account is None:
                raise ValueError(f"shadow account {account_id!r} was not found")
            existing = self.catalog.list_shadow_events_by_type(
                account_id=account_id,
                event_type="data_revalidated",
                since=None,
                through=None,
                limit=1_000,
            )
            if any(
                event.event_type == "data_revalidated"
                and event.payload.get("revalidation_id") == evidence.revalidation_id
                for event in existing
            ):
                continue
            self.catalog.append_shadow_event(
                account_id=account_id,
                event_type="data_revalidated",
                occurred_at=evidence.occurred_at,
                payload=evidence.to_dict(),
                expected_previous_hash=account.last_event_hash,
            )
        return tuple(updated)


__all__ = [
    "CashTargetIntent",
    "DataIncident",
    "DataIncidentCoordinator",
    "DataIncidentResult",
    "DataPipelineStage",
    "DataRevalidation",
]
