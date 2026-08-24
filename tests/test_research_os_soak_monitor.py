from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

import factor_lab.research_os.soak_monitor as soak_monitor
from factor_lab.research_os.catalog import RunRecord
from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.production_ledger import ProductionLedger
from factor_lab.research_os.readiness_audit import (
    DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE,
    dagster_code_location_soak_evidence_hash,
)
from factor_lab.research_os.soak_monitor import (
    DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
    DagsterCodeLocationSoakMonitor,
    DagsterSoakError,
    DagsterSoakIncomplete,
    LocalCodeServerRuntimeIdentity,
    bind_current_code_server_to_host_attestation,
)


BUILD_HASH = "b" * 64
OCI_IMAGE_ID = "sha256:" + "d" * 64
PROCESS_A = "a" * 64
PROCESS_B = "c" * 64
CONTAINER_ID = "1" * 64
CONTAINER_IDENTITY = CONTAINER_ID[:12]
HOST_ATTESTATION_HASH = "2" * 64
RUNTIME_CONTRACT_HASH = "3" * 64
REPO_DIGEST = "factor-lab-research-os@sha256:" + "4" * 64
BASE_DIGEST = "sha256:" + "5" * 64
COMPOSE_CONFIG_HASH = "compose-config-v1"
CONTAINER_STARTED_AT = datetime(2026, 8, 23, 6, 36, 54, tzinfo=timezone.utc)


def _host_attestation_run(*, source_tree_hash: str = "6" * 64) -> RunRecord:
    started_at = datetime(2026, 8, 23, 6, 37, 34, tzinfo=timezone.utc)
    completed_at = datetime(2026, 8, 23, 6, 38, 1, tzinfo=timezone.utc)
    return RunRecord(
        run_id=f"docker_attestation_{HOST_ATTESTATION_HASH}",
        run_type="host_docker_runtime_attestation",
        status="succeeded",
        input_fingerprint=HOST_ATTESTATION_HASH,
        started_at=started_at,
        completed_at=completed_at,
        metadata={
            "attestation_hash": HOST_ATTESTATION_HASH,
            "container_id": CONTAINER_ID,
            "oci_image_id": OCI_IMAGE_ID,
            "oci_repo_digests": [REPO_DIGEST],
            "oci_base_digest": BASE_DIGEST,
            "runtime_contract_hash": RUNTIME_CONTRACT_HASH,
            "docker_authority_hash": "7" * 64,
            "container_started_at": CONTAINER_STARTED_AT.isoformat(),
            "deployment_verified_at": completed_at.isoformat(),
            "source_bundle_manifest_hash": "8" * 64,
            "source_tree_hash": source_tree_hash,
            "configuration_tree_hash": "9" * 64,
            "dependency_lock_hash": "a" * 64,
            "service_labels": {
                "com.docker.compose.project": "factor-lab-research-os",
                "com.docker.compose.service": "dagster-code-server",
                "com.docker.compose.config-hash": COMPOSE_CONFIG_HASH,
            },
        },
    )


def _provenance() -> SimpleNamespace:
    return SimpleNamespace(
        image_source_digest="8" * 64,
        code_hash="6" * 64,
        configuration_hash="9" * 64,
        dependency_lock_hash="a" * 64,
    )


def _mock_attestation_dependencies(
    monkeypatch,
    *,
    process_identity: str = PROCESS_A,
):
    local = LocalCodeServerRuntimeIdentity(
        container_identity=CONTAINER_IDENTITY,
        process_identity=process_identity,
    )
    monkeypatch.setattr(
        soak_monitor, "local_code_server_runtime_identity", lambda: local
    )
    monkeypatch.setattr(
        soak_monitor,
        "bind_verified_oci_deployment",
        lambda *_args, **_kwargs: SimpleNamespace(
            formal_epoch_eligible=True,
            build_identity_hash=BUILD_HASH,
        ),
    )
    captured: dict[str, object] = {}

    def validate(**kwargs):
        captured.update(kwargs)
        return ()

    monkeypatch.setattr(
        soak_monitor, "persisted_attestation_binding_errors", validate
    )
    return captured


def test_host_attestation_binds_to_current_container_pid1_and_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _mock_attestation_dependencies(monkeypatch)
    run = _host_attestation_run()

    verified = bind_current_code_server_to_host_attestation(
        run,
        provenance=_provenance(),
    )

    assert verified.build_identity_hash == BUILD_HASH
    assert verified.oci_image_id == OCI_IMAGE_ID
    assert verified.container_id == CONTAINER_ID
    assert verified.host_attestation_hash == HOST_ATTESTATION_HASH
    assert verified.process_identity == PROCESS_A
    proof = captured["proof"]
    assert isinstance(proof, dict)
    assert proof["executing_container_identity"] == CONTAINER_IDENTITY
    assert proof["executing_process_identity_scheme"] == (
        "linux-boot-id-pid1-start-ticks-v1"
    )
    assert proof["executing_process_identity"] == PROCESS_A
    assert "executing_container_started_at" not in proof
    assert proof["executing_root_matches_init_root"] is True
    assert captured["run"] == run


def test_host_attestation_rejects_container_or_source_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_attestation_dependencies(monkeypatch)
    wrong_container = _host_attestation_run()
    wrong_container.metadata["container_id"] = "b" * 64
    with pytest.raises(DagsterSoakError, match="current code-server container"):
        bind_current_code_server_to_host_attestation(
            wrong_container,
            provenance=_provenance(),
        )

    _mock_attestation_dependencies(monkeypatch)
    with pytest.raises(DagsterSoakError, match="source bundle differs"):
        bind_current_code_server_to_host_attestation(
            _host_attestation_run(source_tree_hash="c" * 64),
            provenance=_provenance(),
        )


def test_host_binding_uses_kernel_process_identity_not_reconstructed_wall_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_capture = _mock_attestation_dependencies(
        monkeypatch,
        process_identity=PROCESS_A,
    )
    first = bind_current_code_server_to_host_attestation(
        _host_attestation_run(),
        provenance=_provenance(),
    )

    # A real PID-1 restart produces another boot-id/start-tick identity even
    # when Docker keeps the same container ID.  Binding may start a new soak,
    # but the monitor's process-identity filter must never join the old span.
    second_capture = _mock_attestation_dependencies(
        monkeypatch,
        process_identity=PROCESS_B,
    )
    second = bind_current_code_server_to_host_attestation(
        _host_attestation_run(),
        provenance=_provenance(),
    )

    assert first.process_identity == PROCESS_A
    assert second.process_identity == PROCESS_B
    assert first.deployment_identity_hash == second.deployment_identity_hash
    assert first_capture["proof"]["executing_process_identity"] == PROCESS_A
    assert second_capture["proof"]["executing_process_identity"] == PROCESS_B


def test_local_process_identity_is_stable_across_btime_correction_and_changes_on_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {
        "start_ticks": 123_456,
        "boot_id": b"11111111-2222-3333-4444-555555555555",
        "btime": 1_700_000_000,
    }
    reads: list[str] = []

    class FakePath:
        def __init__(self, value: str) -> None:
            self.value = value

        def is_file(self) -> bool:
            return self.value in {
                "/proc/1/stat",
                "/proc/sys/kernel/random/boot_id",
                "/etc/hostname",
                "/proc/stat",
            }

        def read_text(self, *, encoding: str) -> str:
            assert encoding == "utf-8"
            reads.append(self.value)
            if self.value == "/proc/1/stat":
                fields = ["S", *("0" for _ in range(18)), str(state["start_ticks"])]
                return "1 (python init) " + " ".join(fields)
            if self.value == "/etc/hostname":
                return CONTAINER_IDENTITY
            if self.value == "/proc/stat":
                return f"btime {state['btime']}\n"
            raise AssertionError(self.value)

        def read_bytes(self) -> bytes:
            reads.append(self.value)
            assert self.value == "/proc/sys/kernel/random/boot_id"
            return state["boot_id"]

    root_stat = SimpleNamespace(st_dev=10, st_ino=20)
    monkeypatch.setattr(soak_monitor, "Path", FakePath)
    monkeypatch.setattr(soak_monitor.socket, "gethostname", lambda: CONTAINER_IDENTITY)
    monkeypatch.setattr(soak_monitor.os, "stat", lambda _path: root_stat)

    first = soak_monitor.local_code_server_runtime_identity()
    state["btime"] += 3_600  # Simulate a WSL/Linux wall-clock anchor correction.
    second = soak_monitor.local_code_server_runtime_identity()

    assert first == second
    assert "/proc/stat" not in reads

    state["start_ticks"] += 1
    restarted = soak_monitor.local_code_server_runtime_identity()
    assert restarted.process_identity != first.process_identity


def test_host_attestation_rejects_failed_content_addressed_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_attestation_dependencies(monkeypatch)
    monkeypatch.setattr(
        soak_monitor,
        "persisted_attestation_binding_errors",
        lambda **_kwargs: ("runtime_attestation_run_invalid",),
    )

    with pytest.raises(DagsterSoakError, match="run_invalid"):
        bind_current_code_server_to_host_attestation(
            _host_attestation_run(),
            provenance=_provenance(),
        )


def _harness(tmp_path, monkeypatch):
    catalog = ResearchCatalog(tmp_path / "catalog.db")
    catalog.initialize_schema()
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE daemon_heartbeats ("
                "daemon_type TEXT PRIMARY KEY, timestamp FLOAT NOT NULL)"
            )
        )
    ledger = ProductionLedger(engine)
    clock = {"now": datetime(2026, 8, 23, tzinfo=timezone.utc)}
    monkeypatch.setattr(catalog, "database_now", lambda: clock["now"])

    def heartbeat(value: datetime) -> None:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM daemon_heartbeats"))
            connection.execute(
                text(
                    "INSERT INTO daemon_heartbeats(daemon_type, timestamp) "
                    "VALUES ('SENSOR', :timestamp)"
                ),
                {"timestamp": value.timestamp()},
            )

    return catalog, ledger, clock, heartbeat


def test_naive_dagster_postgres_heartbeat_is_interpreted_as_utc(
    tmp_path, monkeypatch
) -> None:
    catalog = ResearchCatalog(tmp_path / "catalog.db")
    catalog.initialize_schema()
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE daemon_heartbeats ("
                "daemon_type TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO daemon_heartbeats(daemon_type, timestamp) "
                "VALUES ('SENSOR', :timestamp)"
            ),
            {"timestamp": "2026-08-23 07:40:48.321165"},
        )
    ledger = ProductionLedger(engine)
    observed_at = datetime(2026, 8, 23, 7, 41, tzinfo=timezone.utc)
    monkeypatch.setattr(catalog, "database_now", lambda: observed_at)
    monitor = DagsterCodeLocationSoakMonitor(
        catalog,
        ledger,
        build_identity_hash=BUILD_HASH,
        oci_image_id=OCI_IMAGE_ID,
        process_identity=PROCESS_A,
    )

    sample = monitor.record_sample()

    assert sample.metadata["dagster_heartbeat_at"] == (
        "2026-08-23T07:40:48.321165+00:00"
    )
    catalog.close()
    ledger.close()


def test_soak_is_derived_from_24_hours_of_physical_samples(
    tmp_path, monkeypatch
) -> None:
    catalog, ledger, clock, heartbeat = _harness(tmp_path, monkeypatch)
    monitor = DagsterCodeLocationSoakMonitor(
        catalog,
        ledger,
        build_identity_hash=BUILD_HASH,
        oci_image_id=OCI_IMAGE_ID,
        process_identity=PROCESS_A,
    )
    for index in range(145):
        clock["now"] = datetime(2026, 8, 23, tzinfo=timezone.utc) + timedelta(
            minutes=10 * index
        )
        heartbeat(clock["now"])
        monitor.record_sample()

    provenance = SimpleNamespace(
        formal_epoch_eligible=True,
        build_identity_hash=BUILD_HASH,
        oci_image_id=OCI_IMAGE_ID,
    )
    with pytest.raises(DagsterSoakError, match="sampled image"):
        monitor.finalize(
            provenance=SimpleNamespace(
                formal_epoch_eligible=True,
                build_identity_hash=BUILD_HASH,
                oci_image_id="sha256:" + "e" * 64,
            )
        )
    soak = monitor.finalize(provenance=provenance)

    assert soak.run_type == DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE
    assert soak.metadata["health_sample_count"] == 145
    assert soak.metadata["maximum_sample_gap_seconds"] == 600.0
    assert soak.metadata["restart_count"] == 0
    assert soak.metadata["soak_evidence_hash"] == dagster_code_location_soak_evidence_hash(
        soak.metadata
    )
    assert len(
        catalog.list_runs(
            limit=1_000,
            run_type=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
        )
    ) == 145
    catalog.close()
    ledger.close()


def test_process_restart_resets_the_soak_window(tmp_path, monkeypatch) -> None:
    catalog, ledger, clock, heartbeat = _harness(tmp_path, monkeypatch)
    first = DagsterCodeLocationSoakMonitor(
        catalog,
        ledger,
        build_identity_hash=BUILD_HASH,
        oci_image_id=OCI_IMAGE_ID,
        process_identity=PROCESS_A,
    )
    # The pre-restart segment is deliberately one sample short of a valid
    # soak.  Adding the first post-restart sample would yield 145 rows over 24
    # hours if process identities were accidentally stitched together.
    for _index in range(144):
        heartbeat(clock["now"])
        first.record_sample()
        clock["now"] += timedelta(minutes=10)
    heartbeat(clock["now"])
    restarted = DagsterCodeLocationSoakMonitor(
        catalog,
        ledger,
        build_identity_hash=BUILD_HASH,
        oci_image_id=OCI_IMAGE_ID,
        process_identity=PROCESS_B,
    )
    restarted.record_sample()

    with pytest.raises(DagsterSoakIncomplete, match="fewer than 145"):
        restarted.finalize(
            provenance=SimpleNamespace(
                formal_epoch_eligible=True,
                build_identity_hash=BUILD_HASH,
                oci_image_id=OCI_IMAGE_ID,
            )
        )
    assert len(
        catalog.list_runs(
            limit=1_000,
            run_type=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
        )
    ) == 145
    catalog.close()
    ledger.close()


def test_sample_rejects_stale_daemon_heartbeat(tmp_path, monkeypatch) -> None:
    catalog, ledger, clock, heartbeat = _harness(tmp_path, monkeypatch)
    monitor = DagsterCodeLocationSoakMonitor(
        catalog,
        ledger,
        build_identity_hash=BUILD_HASH,
        oci_image_id=OCI_IMAGE_ID,
        process_identity=PROCESS_A,
    )
    heartbeat(clock["now"] - timedelta(minutes=11))

    with pytest.raises(DagsterSoakError, match="stale"):
        monitor.record_sample()
    assert catalog.list_runs(
        limit=10, run_type=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE
    ) == []
    catalog.close()
    ledger.close()
