"""Remove empty Dagster bootstrap tables accidentally created in public.

Revision ID: 0003_cleanup_dagster
Revises: 0002_dagster_schema
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0003_cleanup_dagster"
down_revision = "0002_dagster_schema"
branch_labels = None
depends_on = None


_DAGSTER_TABLES = (
    "asset_check_executions",
    "asset_daemon_asset_evaluations",
    "asset_event_tags",
    "asset_keys",
    "backfill_tags",
    "bulk_actions",
    "concurrency_limits",
    "concurrency_slots",
    "daemon_heartbeats",
    "dynamic_partitions",
    "event_logs",
    "instance_info",
    "instigators",
    "job_ticks",
    "jobs",
    "kvs",
    "pending_steps",
    "run_tags",
    "runs",
    "secondary_indexes",
    "snapshots",
)


def upgrade() -> None:
    connection = op.get_bind()
    if connection.dialect.name != "postgresql" or op.get_context().as_sql:
        return
    existing = {
        str(row[0])
        for row in connection.execute(
            sa.text(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = ANY(:names)"
            ),
            {"names": list(_DAGSTER_TABLES)},
        )
    }
    nonempty = []
    for table in sorted(existing):
        count = connection.execute(
            sa.text(f'SELECT count(*) FROM public."{table}"')
        ).scalar_one()
        if int(count) > 0:
            nonempty.append(table)
    if nonempty:
        raise RuntimeError(
            "refusing to remove populated legacy public Dagster tables; "
            f"archive/migrate them explicitly first: {nonempty}"
        )
    for table in sorted(existing):
        op.execute(sa.text(f'DROP TABLE public."{table}" CASCADE'))


def downgrade() -> None:
    # This migration only removes verified-empty accidental bootstrap tables.
    # Dagster recreates its owned tables in the isolated schema on startup.
    pass
