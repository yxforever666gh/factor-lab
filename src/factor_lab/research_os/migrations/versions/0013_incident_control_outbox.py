"""Add leased, fenced outbox actions for incident control materialization.

Revision ID: 0013_incident_control_outbox
Revises: 0012_partition_repair_gen
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json

from alembic import context, op
import sqlalchemy as sa


revision = "0013_incident_control_outbox"
down_revision = "0012_partition_repair_gen"
branch_labels = None
depends_on = None


def _normalize_legacy_payload(
    value: object, *, incident_id: str
) -> dict[str, object] | None:
    """Return a typed legacy JSON object or fail closed on invalid JSON."""

    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            value = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError(
                f"Cannot normalize payload_json for OPEN incident {incident_id}"
            ) from exc
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Cannot normalize payload_json for OPEN incident {incident_id}"
            ) from exc
        if isinstance(decoded, Mapping):
            return {str(key): item for key, item in decoded.items()}
        return None
    if value is None:
        return None
    raise RuntimeError(
        "Cannot normalize payload_json for OPEN incident "
        f"{incident_id}: unsupported value type {type(value).__name__}"
    )


def _normalize_legacy_occurred_at(
    value: object, *, incident_id: str
) -> datetime:
    """Normalize SQLite text and PostgreSQL datetime values to aware UTC."""

    if isinstance(value, datetime):
        occurred_at = value
    elif isinstance(value, str):
        candidate = value.strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            occurred_at = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise RuntimeError(
                f"Cannot normalize occurred_at for OPEN incident {incident_id}"
            ) from exc
    else:
        raise RuntimeError(
            "Cannot normalize occurred_at for OPEN incident "
            f"{incident_id}: unsupported value type {type(value).__name__}"
        )
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)
    return occurred_at.astimezone(timezone.utc)


def _normalize_legacy_sequence(
    value: object, *, incident_id: str, field_name: str
) -> tuple[str, ...]:
    """Normalize a legacy JSON string/list without accepting scalar poison."""

    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            value = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError(
                f"Cannot normalize {field_name} for OPEN incident {incident_id}"
            ) from exc
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Cannot normalize {field_name} for OPEN incident {incident_id}"
            ) from exc
    if not isinstance(value, (list, tuple)):
        raise RuntimeError(
            f"Cannot normalize {field_name} for OPEN incident {incident_id}"
        )
    return tuple(str(item) for item in value)


def upgrade() -> None:
    from factor_lab.research_os.data_incidents import (
        DataIncident,
        DataPipelineStage,
    )
    from factor_lab.research_os.fingerprint import content_fingerprint

    if context.is_offline_mode():
        raise RuntimeError(
            "Refusing offline upgrade 0013_incident_control_outbox: "
            "typed OPEN incidents require an online preflight and backfill"
        )

    # Preflight every legacy OPEN row before emitting online DDL.  An OPEN
    # incident without a typed domain identity cannot be materialized by the
    # new outbox and would otherwise remain a permanent, invisible risk latch.
    # Resolve/supersede such legacy rows explicitly before upgrading instead
    # of silently reclassifying them here.
    prepared_open_incidents: list[tuple[str, datetime]] = []
    connection = None
    if not context.is_offline_mode():
        connection = op.get_bind()
        incidents = sa.table(
            "ros_data_incidents",
            sa.column("incident_id", sa.String(96)),
            sa.column("status", sa.String(24)),
            sa.column("partition_key", sa.String(32)),
            sa.column("stage", sa.String(32)),
            sa.column("error_code", sa.String(160)),
            sa.column("message", sa.Text()),
            sa.column("source_ids_json", sa.JSON()),
            sa.column("evidence_hashes_json", sa.JSON()),
            sa.column("occurred_at", sa.DateTime(timezone=True)),
            sa.column("payload_json", sa.JSON()),
        )
        rows = connection.execute(
            sa.select(
                incidents.c.incident_id,
                incidents.c.partition_key,
                incidents.c.stage,
                incidents.c.error_code,
                incidents.c.message,
                incidents.c.source_ids_json,
                incidents.c.evidence_hashes_json,
                incidents.c.occurred_at,
                incidents.c.payload_json,
            ).where(incidents.c.status == "open")
        ).mappings()
        for row in rows:
            incident_id = str(row["incident_id"])
            payload = _normalize_legacy_payload(
                row["payload_json"], incident_id=incident_id
            )
            occurred_at = _normalize_legacy_occurred_at(
                row["occurred_at"], incident_id=incident_id
            )
            try:
                domain_incident = DataIncident(
                    stage=DataPipelineStage(str(row["stage"])),
                    partition_key=str(row["partition_key"]),
                    error_code=str(row["error_code"]),
                    message=str(row["message"]),
                    occurred_at=occurred_at,
                    source_ids=_normalize_legacy_sequence(
                        row["source_ids_json"],
                        incident_id=incident_id,
                        field_name="source_ids_json",
                    ),
                    evidence_hashes=_normalize_legacy_sequence(
                        row["evidence_hashes_json"],
                        incident_id=incident_id,
                        field_name="evidence_hashes_json",
                    ),
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Cannot reconstruct typed OPEN incident {incident_id}"
                ) from exc
            if not (
                isinstance(payload, dict)
                and str(payload.get("domain_incident_id") or "").strip()
                == domain_incident.incident_id
                and str(payload.get("dagster_run_id") or "").strip()
                and str(payload.get("failed_step_key") or "").strip()
            ):
                raise RuntimeError(
                    "Cannot migrate OPEN incident "
                    f"{incident_id}: canonical domain identity or Dagster "
                    "failure lineage is missing; resolve or supersede the "
                    "legacy incident first"
                )
            prepared_open_incidents.append(
                (
                    incident_id,
                    occurred_at,
                )
            )

    op.create_table(
        "ros_incident_control_actions",
        sa.Column("action_id", sa.String(96), primary_key=True),
        sa.Column(
            "incident_id",
            sa.String(96),
            sa.ForeignKey("ros_data_incidents.incident_id"),
            nullable=False,
        ),
        sa.Column("action_kind", sa.String(32), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "fencing_token", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("lease_owner", sa.String(160), nullable=True),
        sa.Column("lease_token", sa.String(64), nullable=True),
        sa.Column(
            "lease_expires_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("result_hash", sa.String(64), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=False),
        sa.Column("last_error_code", sa.String(160), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "incident_id",
            "action_kind",
            name="uq_ros_incident_control_incident_kind",
        ),
        sa.CheckConstraint(
            "action_kind IN ('freeze_fleet','revalidate_incident')",
            name="ck_ros_incident_control_kind",
        ),
        sa.CheckConstraint(
            "status IN ('pending','running','succeeded')",
            name="ck_ros_incident_control_status",
        ),
        sa.CheckConstraint(
            "attempts >= 0 AND fencing_token >= 0",
            name="ck_ros_incident_control_counters",
        ),
        sa.CheckConstraint(
            "((status = 'pending' AND lease_owner IS NULL "
            "AND lease_token IS NULL AND lease_expires_at IS NULL "
            "AND result_hash IS NULL AND completed_at IS NULL) OR "
            "(status = 'running' AND lease_owner IS NOT NULL "
            "AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND result_hash IS NULL AND completed_at IS NULL) OR "
            "(status = 'succeeded' AND lease_owner IS NULL "
            "AND lease_token IS NULL AND lease_expires_at IS NULL "
            "AND result_hash IS NOT NULL AND completed_at IS NOT NULL))",
            name="ck_ros_incident_control_lease_state",
        ),
    )
    op.create_index(
        "ix_ros_incident_control_status_expiry",
        "ros_incident_control_actions",
        ["status", "lease_expires_at"],
    )

    # Existing OPEN rows predate the outbox but must remain recoverable after
    # an in-place upgrade.  Generate the same content-addressed identity as the
    # application; no optional PostgreSQL crypto extension is required.
    assert connection is not None
    actions = sa.table(
        "ros_incident_control_actions",
        sa.column("action_id", sa.String(96)),
        sa.column("incident_id", sa.String(96)),
        sa.column("action_kind", sa.String(32)),
        sa.column("status", sa.String(24)),
        sa.column("attempts", sa.Integer()),
        sa.column("fencing_token", sa.Integer()),
        sa.column("result_json", sa.JSON()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    for incident_id, occurred_at in prepared_open_incidents:
        already_enqueued = connection.execute(
            sa.select(actions.c.action_id).where(
                actions.c.incident_id == incident_id,
                actions.c.action_kind == "freeze_fleet",
            )
        ).first()
        if already_enqueued is not None:
            continue
        connection.execute(
            actions.insert().values(
                {
                    "action_id": "ica_"
                    + content_fingerprint(
                        {
                            "incident_id": incident_id,
                            "action_kind": "freeze_fleet",
                        },
                        domain=(
                            "factor-lab/research-os/v1/incident-control-action"
                        ),
                    )[:64],
                    "incident_id": incident_id,
                    "action_kind": "freeze_fleet",
                    "status": "pending",
                    "attempts": 0,
                    "fencing_token": 0,
                    "result_json": {},
                    "created_at": occurred_at,
                    "updated_at": occurred_at,
                }
            )
        )


def downgrade() -> None:
    # Incident-control actions are the durable fencing and terminal-result
    # authority for OPEN production data incidents.  Dropping a populated
    # outbox would silently erase both running leases and completed control
    # evidence.  Refuse before emitting or executing any DDL, matching the
    # immutable repair-evidence policy in revision 0012.
    if context.is_offline_mode():
        raise RuntimeError(
            "Refusing offline downgrade 0013_incident_control_outbox: "
            "an online immutable incident-control preflight is required"
        )

    connection = op.get_bind()
    action_count = int(
        connection.execute(
            sa.text("SELECT COUNT(*) FROM ros_incident_control_actions")
        ).scalar_one()
    )
    if action_count:
        raise RuntimeError(
            "Refusing downgrade 0013_incident_control_outbox: immutable "
            f"incident-control evidence exists (actions={action_count})"
        )

    op.drop_index(
        "ix_ros_incident_control_status_expiry",
        table_name="ros_incident_control_actions",
    )
    op.drop_table("ros_incident_control_actions")
