"""Optional SQLAlchemy 2.x ORM model set for the Research OS catalog.

The main package remains importable on lightweight research workers that do not
have infrastructure extras installed.  PostgreSQL use is explicit and fails
with a focused dependency error in :mod:`factor_lab.research_os.catalog`.
"""

from __future__ import annotations


try:  # pragma: no branch - the two paths are exercised in separate environments.
    from sqlalchemy import (
        JSON,
        Boolean,
        CheckConstraint,
        DateTime,
        Float,
        ForeignKey,
        Index,
        Integer,
        String,
        Text,
        UniqueConstraint,
        func,
        text,
    )
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    SQLALCHEMY_AVAILABLE = True
except ImportError:  # pragma: no cover - covered by catalog fallback tests.
    SQLALCHEMY_AVAILABLE = False


if SQLALCHEMY_AVAILABLE:

    class Base(DeclarativeBase):
        pass


    class SchemaMetadataModel(Base):
        __tablename__ = "ros_schema_metadata"

        component: Mapped[str] = mapped_column(String(80), primary_key=True)
        schema_version: Mapped[str] = mapped_column(String(80), nullable=False)
        installed_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False, server_default=func.now()
        )


    class RuntimeAuthorityModel(Base):
        """Alembic-seeded singleton identifying the database's authority mode."""

        __tablename__ = "ros_runtime_authority"

        marker_key: Mapped[str] = mapped_column(String(32), primary_key=True)
        environment: Mapped[str] = mapped_column(String(16), nullable=False)
        authority_schema: Mapped[str] = mapped_column(String(80), nullable=False)
        marker_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        installed_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False, server_default=func.now()
        )

        __table_args__ = (
            CheckConstraint(
                "marker_key = 'research_os'",
                name="ck_ros_runtime_authority_singleton",
            ),
            CheckConstraint(
                "environment IN ('production','test')",
                name="ck_ros_runtime_authority_environment",
            ),
        )


    class EvidenceEpochModel(Base):
        __tablename__ = "ros_evidence_epochs"

        epoch_slot: Mapped[str] = mapped_column(String(80), primary_key=True)
        epoch_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True)
        schema_version: Mapped[str] = mapped_column(String(80), nullable=False)
        architecture_version: Mapped[str] = mapped_column(String(160), nullable=False)
        frozen_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        code_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        configuration_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        dependency_lock_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        dirty_patch_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        epoch_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        first_forward_session: Mapped[str | None] = mapped_column(String(10), nullable=True)
        calendar_snapshot_id: Mapped[str | None] = mapped_column(
            String(160), ForeignKey("ros_data_snapshots.snapshot_id"), nullable=True
        )
        calendar_snapshot_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
        calendar_content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
        evidence_window_hash: Mapped[str | None] = mapped_column(
            String(64), nullable=True, unique=True
        )
        activated_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        closed_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        superseded_by_epoch_id: Mapped[str | None] = mapped_column(
            String(80), ForeignKey("ros_evidence_epochs.epoch_id"), nullable=True
        )

        __table_args__ = (
            CheckConstraint(
                "((closed_at IS NULL AND superseded_by_epoch_id IS NULL) OR "
                "(closed_at IS NOT NULL AND superseded_by_epoch_id IS NOT NULL))",
                name="ck_ros_evidence_epoch_closure_complete",
            ),
            CheckConstraint(
                "((first_forward_session IS NULL AND calendar_snapshot_id IS NULL "
                "AND calendar_snapshot_hash IS NULL AND calendar_content_hash IS NULL "
                "AND evidence_window_hash IS NULL AND activated_at IS NULL) OR "
                "(first_forward_session IS NOT NULL AND calendar_snapshot_id IS NOT NULL "
                "AND calendar_snapshot_hash IS NOT NULL AND calendar_content_hash IS NOT NULL "
                "AND evidence_window_hash IS NOT NULL AND activated_at IS NOT NULL))",
                name="ck_ros_evidence_epoch_activation_complete",
            ),
            Index(
                "uq_ros_evidence_epoch_one_active",
                text("(1)"),
                unique=True,
                postgresql_where=text(
                    "activated_at IS NOT NULL AND closed_at IS NULL"
                ),
                sqlite_where=text("activated_at IS NOT NULL AND closed_at IS NULL"),
            ),
            Index("ix_ros_evidence_epochs_frozen_at", "frozen_at"),
        )


    class EvidenceEpochPointerModel(Base):
        __tablename__ = "ros_evidence_epoch_active_pointer"

        pointer_key: Mapped[str] = mapped_column(String(32), primary_key=True)
        epoch_id: Mapped[str] = mapped_column(
            String(80),
            ForeignKey("ros_evidence_epochs.epoch_id"),
            nullable=False,
            unique=True,
        )
        updated_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False
        )

        __table_args__ = (
            CheckConstraint(
                "pointer_key = 'research_os'",
                name="ck_ros_evidence_epoch_pointer_singleton",
            ),
        )


    class DataSnapshotModel(Base):
        __tablename__ = "ros_data_snapshots"

        snapshot_id: Mapped[str] = mapped_column(String(160), primary_key=True)
        schema_version: Mapped[str] = mapped_column(String(80), nullable=False)
        tier: Mapped[str] = mapped_column(String(32), nullable=False)
        uri: Mapped[str] = mapped_column(Text, nullable=False)
        content_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        as_of: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        quality_status: Mapped[str] = mapped_column(String(32), nullable=False)
        ref_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        created_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False, server_default=func.now()
        )

        __table_args__ = (
            Index("ix_ros_data_snapshots_as_of", "as_of"),
            Index("ix_ros_data_snapshots_quality", "quality_status"),
        )


    class ExperimentModel(Base):
        __tablename__ = "ros_experiments"

        experiment_id: Mapped[str] = mapped_column(String(80), primary_key=True)
        fingerprint: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        snapshot_id: Mapped[str] = mapped_column(
            String(160), ForeignKey("ros_data_snapshots.snapshot_id"), nullable=False
        )
        candidate_kind: Mapped[str] = mapped_column(String(24), nullable=False)
        candidate_id: Mapped[str] = mapped_column(String(160), nullable=False)
        family: Mapped[str] = mapped_column(String(160), nullable=False)
        status: Mapped[str] = mapped_column(String(32), nullable=False)
        spec_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        registered_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False, server_default=func.now()
        )
        updated_at: Mapped[object] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        )

        __table_args__ = (
            Index("ix_ros_experiments_snapshot", "snapshot_id"),
            Index("ix_ros_experiments_candidate", "candidate_kind", "candidate_id"),
            Index("ix_ros_experiments_family", "family"),
        )


    class ExperimentResultModel(Base):
        __tablename__ = "ros_experiment_results"

        result_id: Mapped[str] = mapped_column(String(80), primary_key=True)
        experiment_id: Mapped[str] = mapped_column(
            String(80), ForeignKey("ros_experiments.experiment_id"), nullable=False
        )
        result_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        outcome: Mapped[str] = mapped_column(String(40), nullable=False)
        metrics_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        artifact_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
        authoritative: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
        completed_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (
            UniqueConstraint(
                "experiment_id", "result_hash", name="uq_ros_result_experiment_hash"
            ),
            Index("ix_ros_results_experiment", "experiment_id"),
            Index(
                "uq_ros_results_one_authoritative",
                "experiment_id",
                unique=True,
                postgresql_where=text("authoritative"),
                sqlite_where=text("authoritative = 1"),
            ),
        )


    class TrialLedgerModel(Base):
        __tablename__ = "ros_trial_ledger"

        trial_id: Mapped[str] = mapped_column(String(80), primary_key=True)
        experiment_id: Mapped[str | None] = mapped_column(
            String(80), ForeignKey("ros_experiments.experiment_id"), nullable=True
        )
        family: Mapped[str] = mapped_column(String(160), nullable=False)
        candidate_id: Mapped[str] = mapped_column(String(160), nullable=False)
        outcome: Mapped[str] = mapped_column(String(40), nullable=False)
        reason: Mapped[str] = mapped_column(Text, nullable=False)
        p_value: Mapped[float | None] = mapped_column(Float, nullable=True)
        alpha_spent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
        metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        occurred_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        admission_status: Mapped[str] = mapped_column(
            String(16), nullable=False, default="admitted"
        )
        experiment_fingerprint: Mapped[str | None] = mapped_column(
            String(64), nullable=True
        )
        research_equivalence_hash: Mapped[str | None] = mapped_column(
            String(64), nullable=True
        )
        completed_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        updated_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False, server_default=func.now()
        )

        __table_args__ = (
            CheckConstraint(
                "admission_status IN ('admitted', 'rejected')",
                name="ck_ros_trials_admission_status",
            ),
            Index("ix_ros_trials_family_time", "family", "occurred_at"),
            Index("ix_ros_trials_candidate", "candidate_id"),
            Index(
                "ix_ros_trials_admission_month_family",
                "admission_status",
                "occurred_at",
                "family",
            ),
            Index("ix_ros_trials_fingerprint", "experiment_fingerprint"),
            Index("ix_ros_trials_equivalence", "research_equivalence_hash"),
            Index(
                "uq_ros_trials_admitted_fingerprint",
                "experiment_fingerprint",
                unique=True,
                postgresql_where=text(
                    "admission_status = 'admitted' AND experiment_fingerprint IS NOT NULL"
                ),
                sqlite_where=text(
                    "admission_status = 'admitted' AND experiment_fingerprint IS NOT NULL"
                ),
            ),
            Index(
                "uq_ros_trials_admitted_equivalence",
                "research_equivalence_hash",
                unique=True,
                postgresql_where=text(
                    "admission_status = 'admitted' AND research_equivalence_hash IS NOT NULL"
                ),
                sqlite_where=text(
                    "admission_status = 'admitted' AND research_equivalence_hash IS NOT NULL"
                ),
            ),
        )


    class ResearchFamilyModel(Base):
        __tablename__ = "ros_research_families"

        family_id: Mapped[str] = mapped_column(String(160), primary_key=True)
        registry_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        definition_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
        created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)


    class ResearchSubmissionModel(Base):
        __tablename__ = "ros_research_submissions"

        submission_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        proposal_decision_id: Mapped[str] = mapped_column(String(96), nullable=False)
        family_id: Mapped[str] = mapped_column(
            String(160), ForeignKey("ros_research_families.family_id"), nullable=False
        )
        recovery_case_id: Mapped[str | None] = mapped_column(
            String(160), ForeignKey("ros_recovery_cases.recovery_case_id"), nullable=True
        )
        status: Mapped[str] = mapped_column(String(24), nullable=False)
        research_equivalence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        experiment_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
        trial_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True)
        spec_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        lease_owner: Mapped[str | None] = mapped_column(String(160), nullable=True)
        lease_expires_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
        experiment_id: Mapped[str | None] = mapped_column(
            String(80), ForeignKey("ros_experiments.experiment_id"), nullable=True
        )
        error: Mapped[str | None] = mapped_column(Text, nullable=True)
        created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (
            CheckConstraint(
                "status IN ('reviewed','reserved','running','completed','failed','missing_data')",
                name="ck_ros_submission_status",
            ),
            UniqueConstraint(
                "proposal_decision_id", "research_equivalence_hash",
                name="uq_ros_submission_decision_equivalence",
            ),
            Index("ix_ros_submission_status_time", "status", "updated_at"),
            Index("ix_ros_submission_family", "family_id"),
        )


    class LifecycleEventModel(Base):
        __tablename__ = "ros_lifecycle_events"

        event_id: Mapped[str] = mapped_column(String(80), primary_key=True)
        idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False, unique=True)
        sleeve_id: Mapped[str] = mapped_column(String(160), nullable=False)
        from_state: Mapped[str | None] = mapped_column(String(32), nullable=True)
        to_state: Mapped[str] = mapped_column(String(32), nullable=False)
        cause: Mapped[str] = mapped_column(Text, nullable=False)
        evidence_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        occurred_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (Index("ix_ros_lifecycle_sleeve_time", "sleeve_id", "occurred_at"),)


    class RecoveryCaseModel(Base):
        __tablename__ = "ros_recovery_cases"

        recovery_case_id: Mapped[str] = mapped_column(String(160), primary_key=True)
        sleeve_id: Mapped[str] = mapped_column(String(160), nullable=False)
        status: Mapped[str] = mapped_column(String(32), nullable=False)
        lifecycle_state: Mapped[str] = mapped_column(String(32), nullable=False)
        case_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        triggered_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        updated_at: Mapped[object] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        )

        __table_args__ = (
            Index("ix_ros_recovery_sleeve_status", "sleeve_id", "status"),
        )


    class RunModel(Base):
        __tablename__ = "ros_runs"

        run_id: Mapped[str] = mapped_column(String(160), primary_key=True)
        run_type: Mapped[str] = mapped_column(String(80), nullable=False)
        status: Mapped[str] = mapped_column(String(32), nullable=False)
        input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
        metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        error: Mapped[str | None] = mapped_column(Text, nullable=True)
        started_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        completed_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)

        __table_args__ = (
            Index("ix_ros_runs_type_time", "run_type", "started_at"),
            Index("ix_ros_runs_status", "status"),
        )


    class LegacyEvidenceModel(Base):
        __tablename__ = "ros_legacy_evidence"

        evidence_id: Mapped[str] = mapped_column(String(80), primary_key=True)
        source_uri: Mapped[str] = mapped_column(Text, nullable=False)
        content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        trust_label: Mapped[str] = mapped_column(String(80), nullable=False)
        reasons_json: Mapped[list] = mapped_column(JSON, nullable=False)
        imported_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (
            UniqueConstraint(
                "source_uri", "content_hash", name="uq_ros_legacy_source_content"
            ),
            Index("ix_ros_legacy_evidence_trust_time", "trust_label", "imported_at"),
        )


    class ShadowAccountModel(Base):
        __tablename__ = "ros_shadow_accounts"

        account_id: Mapped[str] = mapped_column(String(160), primary_key=True)
        name: Mapped[str] = mapped_column(String(240), nullable=False)
        currency: Mapped[str] = mapped_column(String(12), nullable=False)
        initial_capital: Mapped[float] = mapped_column(Float, nullable=False)
        cash: Mapped[float] = mapped_column(Float, nullable=False)
        nav: Mapped[float] = mapped_column(Float, nullable=False)
        benchmark_nav: Mapped[float] = mapped_column(Float, nullable=False)
        status: Mapped[str] = mapped_column(String(32), nullable=False)
        as_of: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        last_event_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
        last_event_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (
            CheckConstraint(
                "initial_capital > 0", name="ck_ros_shadow_initial_capital"
            ),
        )


    class ShadowEventModel(Base):
        __tablename__ = "ros_shadow_events"

        event_id: Mapped[str] = mapped_column(String(80), primary_key=True)
        account_id: Mapped[str] = mapped_column(
            String(160), ForeignKey("ros_shadow_accounts.account_id"), nullable=False
        )
        sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
        event_type: Mapped[str] = mapped_column(String(80), nullable=False)
        occurred_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        previous_event_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        event_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)

        __table_args__ = (
            UniqueConstraint(
                "account_id", "sequence_number", name="uq_ros_shadow_account_sequence"
            ),
            Index("ix_ros_shadow_events_account_time", "account_id", "occurred_at"),
        )


    class ShadowPositionModel(Base):
        __tablename__ = "ros_shadow_positions"

        account_id: Mapped[str] = mapped_column(
            String(160),
            ForeignKey("ros_shadow_accounts.account_id"),
            primary_key=True,
        )
        ticker: Mapped[str] = mapped_column(String(32), primary_key=True)
        quantity: Mapped[float] = mapped_column(Float, nullable=False)
        average_cost: Mapped[float] = mapped_column(Float, nullable=False)
        market_price: Mapped[float] = mapped_column(Float, nullable=False)
        market_value: Mapped[float] = mapped_column(Float, nullable=False)
        updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        last_event_sequence: Mapped[int] = mapped_column(Integer, nullable=False)

        __table_args__ = (
            CheckConstraint(
                "quantity >= 0", name="ck_ros_shadow_position_long_only"
            ),
        )


    class SourceCapabilityModel(Base):
        __tablename__ = "ros_source_capabilities"

        source_id: Mapped[str] = mapped_column(String(80), primary_key=True)
        dataset: Mapped[str] = mapped_column(String(80), primary_key=True)
        status: Mapped[str] = mapped_column(String(24), nullable=False)
        contract_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        probe_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
        fields_json: Mapped[list] = mapped_column(JSON, nullable=False)
        detail: Mapped[str] = mapped_column(Text, nullable=False)
        probed_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (
            CheckConstraint(
                "status IN ('accepted','degraded','unavailable','disputed')",
                name="ck_ros_source_capability_status",
            ),
            Index("ix_ros_source_capabilities_status", "status", "updated_at"),
        )


    class PartitionRunModel(Base):
        __tablename__ = "ros_partition_runs"

        partition_run_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        source_id: Mapped[str] = mapped_column(String(80), nullable=False)
        dataset: Mapped[str] = mapped_column(String(80), nullable=False)
        partition_key: Mapped[str] = mapped_column(String(32), nullable=False)
        generation: Mapped[str] = mapped_column(
            String(80), nullable=False, default="base"
        )
        status: Mapped[str] = mapped_column(String(24), nullable=False)
        lease_owner: Mapped[str | None] = mapped_column(String(160), nullable=True)
        lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
        lease_expires_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
        run_id: Mapped[str | None] = mapped_column(
            String(160), ForeignKey("ros_runs.run_id"), nullable=True
        )
        output_snapshot_id: Mapped[str | None] = mapped_column(
            String(160), ForeignKey("ros_data_snapshots.snapshot_id"), nullable=True
        )
        input_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
        output_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
        vendor_revision: Mapped[str | None] = mapped_column(String(160), nullable=True)
        details_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        error_code: Mapped[str | None] = mapped_column(String(120), nullable=True)
        error: Mapped[str | None] = mapped_column(Text, nullable=True)
        created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        started_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        completed_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        repair_incident_id: Mapped[str | None] = mapped_column(
            String(96), ForeignKey("ros_data_incidents.incident_id"), nullable=True
        )
        repair_parent_partition_run_id: Mapped[str | None] = mapped_column(
            String(96), ForeignKey("ros_partition_runs.partition_run_id"), nullable=True
        )
        repair_parent_hash: Mapped[str | None] = mapped_column(
            String(64), nullable=True
        )
        repair_fingerprint: Mapped[str | None] = mapped_column(
            String(64), nullable=True
        )

        __table_args__ = (
            CheckConstraint(
                "status IN ('pending','running','succeeded','disputed','quarantined','failed')",
                name="ck_ros_partition_status",
            ),
            CheckConstraint("attempts >= 0", name="ck_ros_partition_attempts"),
            CheckConstraint(
                "((status = 'running' AND lease_owner IS NOT NULL "
                "AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL "
                "AND started_at IS NOT NULL) OR "
                "(status <> 'running' AND lease_owner IS NULL "
                "AND lease_token IS NULL AND lease_expires_at IS NULL))",
                name="ck_ros_partition_lease_state",
            ),
            UniqueConstraint(
                "source_id",
                "dataset",
                "partition_key",
                "generation",
                name="uq_ros_partition_source_dataset_key_generation",
            ),
            CheckConstraint(
                "((generation = 'base' AND repair_incident_id IS NULL "
                "AND repair_parent_partition_run_id IS NULL "
                "AND repair_parent_hash IS NULL AND repair_fingerprint IS NULL) OR "
                "(generation <> 'base' "
                "AND repair_parent_partition_run_id IS NOT NULL "
                "AND repair_parent_hash IS NOT NULL AND repair_fingerprint IS NOT NULL))",
                name="ck_ros_partition_repair_generation",
            ),
            Index("ix_ros_partition_status_key", "status", "partition_key"),
            Index(
                "ix_ros_partition_source_dataset",
                "source_id",
                "dataset",
                "partition_key",
            ),
            Index(
                "ix_ros_partition_lease_expiry", "status", "lease_expires_at"
            ),
        )


    class DataIncidentModel(Base):
        __tablename__ = "ros_data_incidents"

        incident_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        incident_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        partition_run_id: Mapped[str | None] = mapped_column(
            String(96), ForeignKey("ros_partition_runs.partition_run_id"), nullable=True
        )


        partition_key: Mapped[str] = mapped_column(String(32), nullable=False)
        stage: Mapped[str] = mapped_column(String(24), nullable=False)
        status: Mapped[str] = mapped_column(String(24), nullable=False)
        error_code: Mapped[str] = mapped_column(String(120), nullable=False)
        message: Mapped[str] = mapped_column(Text, nullable=False)
        source_ids_json: Mapped[list] = mapped_column(JSON, nullable=False)
        evidence_hashes_json: Mapped[list] = mapped_column(JSON, nullable=False)
        payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        occurred_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        resolved_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        resolution_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

        __table_args__ = (
            CheckConstraint(
                "stage IN ('source','silver','data_quality','gold','shadow_execution')",
                name="ck_ros_data_incident_stage",
            ),
            CheckConstraint(
                "status IN ('open','resolved','superseded')",
                name="ck_ros_data_incident_status",
            ),
            CheckConstraint(
                "((status = 'open' AND resolved_at IS NULL AND resolution_hash IS NULL) "
                "OR (status <> 'open' AND resolved_at IS NOT NULL "
                "AND resolution_hash IS NOT NULL))",
                name="ck_ros_data_incident_resolution",
            ),
            Index("ix_ros_data_incidents_status_time", "status", "occurred_at"),
            Index("ix_ros_data_incidents_partition", "partition_key", "stage"),
        )


    class PartitionRepairAuthorityModel(Base):
        __tablename__ = "ros_partition_repair_authorities"

        authority_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        scope_key: Mapped[str] = mapped_column(String(160), nullable=False)
        incident_id: Mapped[str | None] = mapped_column(
            String(96), ForeignKey("ros_data_incidents.incident_id"), nullable=True
        )
        source_id: Mapped[str] = mapped_column(String(80), nullable=False)
        dataset: Mapped[str] = mapped_column(String(80), nullable=False)
        partition_key: Mapped[str] = mapped_column(String(32), nullable=False)
        generation: Mapped[str] = mapped_column(String(80), nullable=False)
        parent_partition_run_id: Mapped[str] = mapped_column(
            String(96), ForeignKey("ros_partition_runs.partition_run_id"), nullable=False
        )
        parent_terminal_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        successor_partition_run_id: Mapped[str] = mapped_column(
            String(96), ForeignKey("ros_partition_runs.partition_run_id"), nullable=False
        )
        repair_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
        created_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False
        )

        __table_args__ = (
            UniqueConstraint(
                "parent_partition_run_id",
                name="uq_ros_partition_repair_parent",
            ),
            UniqueConstraint(
                "successor_partition_run_id",
                name="uq_ros_partition_repair_successor",
            ),
            Index(
                "ix_ros_partition_repair_incident",
                "incident_id",
                "partition_key",
                "dataset",
            ),
            Index(
                "ix_ros_partition_repair_scope_slot",
                "scope_key",
                "source_id",
                "dataset",
                "partition_key",
            ),
        )


    class IncidentControlActionModel(Base):
        __tablename__ = "ros_incident_control_actions"

        action_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        incident_id: Mapped[str] = mapped_column(
            String(96), ForeignKey("ros_data_incidents.incident_id"), nullable=False
        )
        action_kind: Mapped[str] = mapped_column(String(32), nullable=False)
        status: Mapped[str] = mapped_column(String(24), nullable=False)
        attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
        fencing_token: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
        lease_owner: Mapped[str | None] = mapped_column(String(160), nullable=True)
        lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
        lease_expires_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        result_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
        result_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        last_error_code: Mapped[str | None] = mapped_column(
            String(160), nullable=True
        )
        created_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False
        )
        updated_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False
        )
        completed_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )

        __table_args__ = (
            UniqueConstraint(
                "incident_id",
                "action_kind",
                name="uq_ros_incident_control_incident_kind",
            ),
            CheckConstraint(
                "action_kind IN ('freeze_fleet','revalidate_incident')",
                name="ck_ros_incident_control_kind",
            ),
            CheckConstraint(
                "status IN ('pending','running','succeeded')",
                name="ck_ros_incident_control_status",
            ),
            CheckConstraint(
                "attempts >= 0 AND fencing_token >= 0",
                name="ck_ros_incident_control_counters",
            ),
            CheckConstraint(
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
            Index(
                "ix_ros_incident_control_status_expiry",
                "status",
                "lease_expires_at",
            ),
        )


    class SleeveClusterModel(Base):
        __tablename__ = "ros_sleeve_clusters"

        cluster_record_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        manifest_id: Mapped[str] = mapped_column(String(96), nullable=False)
        cluster_id: Mapped[str] = mapped_column(String(160), nullable=False)
        family_id: Mapped[str] = mapped_column(String(160), nullable=False)
        representative_experiment_id: Mapped[str] = mapped_column(
            String(80), ForeignKey("ros_experiments.experiment_id"), nullable=False
        )
        member_experiment_ids_json: Mapped[list] = mapped_column(JSON, nullable=False)
        active_returns_hash: Mapped[str] = mapped_column(String(64), nullable=False)
        decision_json: Mapped[dict] = mapped_column(JSON, nullable=False)
        created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (
            UniqueConstraint(
                "manifest_id",
                "cluster_id",
                name="uq_ros_sleeve_cluster_manifest",
            ),
            Index("ix_ros_sleeve_clusters_family", "family_id", "created_at"),
        )


    class ShadowRoleBindingModel(Base):
        __tablename__ = "ros_shadow_role_bindings"

        binding_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        binding_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        role: Mapped[str] = mapped_column(String(24), nullable=False)
        role_key: Mapped[str] = mapped_column(String(160), nullable=False)
        account_id: Mapped[str] = mapped_column(
            String(160), ForeignKey("ros_shadow_accounts.account_id"), nullable=False
        )
        sleeve_id: Mapped[str | None] = mapped_column(String(160), nullable=True)
        experiment_id: Mapped[str | None] = mapped_column(
            String(80), ForeignKey("ros_experiments.experiment_id"), nullable=True
        )
        epoch_id: Mapped[str | None] = mapped_column(
            String(80), ForeignKey("ros_evidence_epochs.epoch_id"), nullable=True
        )
        active: Mapped[bool] = mapped_column(Boolean, nullable=False)
        bound_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
        unbound_at: Mapped[object | None] = mapped_column(
            DateTime(timezone=True), nullable=True
        )
        metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False)

        __table_args__ = (
            CheckConstraint(
                "role IN ('champion','challenger','sleeve')",
                name="ck_ros_shadow_binding_role",
            ),
            CheckConstraint(
                "((active AND unbound_at IS NULL) OR "
                "((NOT active) AND unbound_at IS NOT NULL))",
                name="ck_ros_shadow_binding_active",
            ),
            Index(
                "uq_ros_shadow_binding_active_role",
                "role",
                "role_key",
                unique=True,
                postgresql_where=text("active"),
                sqlite_where=text("active = 1"),
            ),
            Index("ix_ros_shadow_binding_account", "account_id", "active"),
        )


    class ShadowSessionModel(Base):
        __tablename__ = "ros_shadow_sessions"

        account_id: Mapped[str] = mapped_column(
            String(160),
            ForeignKey("ros_shadow_accounts.account_id"),
            primary_key=True,
        )
        trade_date: Mapped[str] = mapped_column(String(10), primary_key=True)
        role_binding_id: Mapped[str | None] = mapped_column(
            String(96), ForeignKey("ros_shadow_role_bindings.binding_id"), nullable=True
        )
        epoch_id: Mapped[str | None] = mapped_column(
            String(80), ForeignKey("ros_evidence_epochs.epoch_id"), nullable=True
        )
        evidence_window_hash: Mapped[str | None] = mapped_column(
            String(64), nullable=True
        )
        evidence_class: Mapped[str] = mapped_column(String(24), nullable=False)
        decision_snapshot_id: Mapped[str | None] = mapped_column(
            String(160), ForeignKey("ros_data_snapshots.snapshot_id"), nullable=True
        )
        execution_snapshot_id: Mapped[str] = mapped_column(
            String(160), ForeignKey("ros_data_snapshots.snapshot_id"), nullable=False
        )
        mark_snapshot_id: Mapped[str] = mapped_column(
            String(160), ForeignKey("ros_data_snapshots.snapshot_id"), nullable=False
        )
        rebalanced: Mapped[bool] = mapped_column(Boolean, nullable=False)
        cash: Mapped[float] = mapped_column(Float, nullable=False)
        positions_value: Mapped[float] = mapped_column(Float, nullable=False)
        nav: Mapped[float] = mapped_column(Float, nullable=False)
        benchmark_nav: Mapped[float] = mapped_column(Float, nullable=False)
        position_count: Mapped[int] = mapped_column(Integer, nullable=False)
        account_event_hash: Mapped[str] = mapped_column(
            String(64), nullable=False, unique=True
        )
        account_event_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
        session_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

        __table_args__ = (
            CheckConstraint(
                "evidence_class IN ('engineering','forward')",
                name="ck_ros_shadow_session_evidence_class",
            ),
            CheckConstraint(
                "((evidence_class = 'engineering' AND epoch_id IS NULL "
                "AND evidence_window_hash IS NULL) OR "
                "(evidence_class = 'forward' AND epoch_id IS NOT NULL "
                "AND evidence_window_hash IS NOT NULL))",
                name="ck_ros_shadow_session_epoch_binding",
            ),
            CheckConstraint(
                "cash >= 0 AND positions_value >= 0 AND nav > 0 "
                "AND benchmark_nav > 0 AND position_count >= 0",
                name="ck_ros_shadow_session_account_values",
            ),
            CheckConstraint(
                "ABS((cash + positions_value) - nav) <= 0.01",
                name="ck_ros_shadow_session_account_equation",
            ),
            CheckConstraint(
                "execution_snapshot_id <> mark_snapshot_id",
                name="ck_ros_shadow_session_snapshot_roles",
            ),
            CheckConstraint(
                "decision_snapshot_id IS NULL OR "
                "(decision_snapshot_id <> execution_snapshot_id AND "
                "decision_snapshot_id <> mark_snapshot_id)",
                name="ck_ros_shadow_session_decision_snapshot_role",
            ),
            Index("ix_ros_shadow_sessions_epoch_date", "epoch_id", "trade_date"),
            Index(
                "ix_ros_shadow_sessions_binding_date", "role_binding_id", "trade_date"
            ),
        )


    class ShadowFleetClosureModel(Base):
        __tablename__ = "ros_shadow_fleet_closures"

        closure_id: Mapped[str] = mapped_column(String(96), primary_key=True)
        trade_date: Mapped[str] = mapped_column(String(10), nullable=False, unique=True)
        evidence_class: Mapped[str] = mapped_column(String(24), nullable=False)
        epoch_id: Mapped[str | None] = mapped_column(
            String(80), ForeignKey("ros_evidence_epochs.epoch_id"), nullable=True
        )
        evidence_window_hash: Mapped[str | None] = mapped_column(
            String(64), nullable=True
        )
        member_count: Mapped[int] = mapped_column(Integer, nullable=False)
        members_json: Mapped[list] = mapped_column(JSON, nullable=False)
        closure_hash: Mapped[str] = mapped_column(
            String(64), nullable=False, unique=True
        )
        closed_at: Mapped[object] = mapped_column(
            DateTime(timezone=True), nullable=False
        )

        __table_args__ = (
            CheckConstraint(
                "evidence_class IN ('engineering','forward')",
                name="ck_ros_shadow_fleet_evidence_class",
            ),
            CheckConstraint(
                "member_count > 0",
                name="ck_ros_shadow_fleet_member_count",
            ),
            CheckConstraint(
                "((evidence_class = 'engineering' AND epoch_id IS NULL "
                "AND evidence_window_hash IS NULL) OR "
                "(evidence_class = 'forward' AND epoch_id IS NOT NULL "
                "AND evidence_window_hash IS NOT NULL))",
                name="ck_ros_shadow_fleet_epoch_binding",
            ),
            Index(
                "ix_ros_shadow_fleet_epoch_date",
                "epoch_id",
                "trade_date",
            ),
        )

else:
    Base = None
    SchemaMetadataModel = None
    RuntimeAuthorityModel = None
    EvidenceEpochModel = None
    EvidenceEpochPointerModel = None
    DataSnapshotModel = None
    ExperimentModel = None
    ExperimentResultModel = None
    TrialLedgerModel = None
    ResearchFamilyModel = None
    ResearchSubmissionModel = None
    LifecycleEventModel = None
    RecoveryCaseModel = None
    RunModel = None
    LegacyEvidenceModel = None
    ShadowAccountModel = None
    ShadowEventModel = None
    ShadowPositionModel = None
    SourceCapabilityModel = None
    PartitionRunModel = None
    DataIncidentModel = None
    PartitionRepairAuthorityModel = None
    IncidentControlActionModel = None
    SleeveClusterModel = None
    ShadowRoleBindingModel = None
    ShadowSessionModel = None
    ShadowFleetClosureModel = None


__all__ = [
    "SQLALCHEMY_AVAILABLE",
    "Base",
    "DataSnapshotModel",
    "EvidenceEpochModel",
    "EvidenceEpochPointerModel",
    "ExperimentModel",
    "ExperimentResultModel",
    "LifecycleEventModel",
    "LegacyEvidenceModel",
    "RecoveryCaseModel",
    "RunModel",
    "RuntimeAuthorityModel",
    "SchemaMetadataModel",
    "TrialLedgerModel",
    "ResearchFamilyModel",
    "ResearchSubmissionModel",
    "ShadowAccountModel",
    "ShadowEventModel",
    "ShadowPositionModel",
    "SourceCapabilityModel",
    "PartitionRunModel",
    "DataIncidentModel",
    "PartitionRepairAuthorityModel",
    "IncidentControlActionModel",
    "SleeveClusterModel",
    "ShadowRoleBindingModel",
    "ShadowSessionModel",
    "ShadowFleetClosureModel",
]
