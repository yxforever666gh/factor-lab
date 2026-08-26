"""Give post-Gold shadow execution failures their own incident stage.

Revision ID: 0011_shadow_execution_incidents
Revises: 0010_evidence_epoch_versions
"""

from __future__ import annotations

from alembic import op


revision = "0011_shadow_execution_incidents"
down_revision = "0010_evidence_epoch_versions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ros_data_incidents") as batch:
        batch.drop_constraint("ck_ros_data_incident_stage", type_="check")
        batch.create_check_constraint(
            "ck_ros_data_incident_stage",
            "stage IN "
            "('source','silver','data_quality','gold','shadow_execution')",
        )


def downgrade() -> None:
    # Downgrade intentionally fails while shadow-execution incidents exist;
    # deleting immutable safety evidence is never an automatic migration step.
    with op.batch_alter_table("ros_data_incidents") as batch:
        batch.drop_constraint("ck_ros_data_incident_stage", type_="check")
        batch.create_check_constraint(
            "ck_ros_data_incident_stage",
            "stage IN ('source','silver','data_quality','gold')",
        )
