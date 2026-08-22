from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess

import pytest

import factor_lab.research_os.application_services as application_services_module
import factor_lab.research_os.build_provenance as build_provenance_module
from factor_lab.research_os.application_services import (
    APPLICATION_SERVICES_SCHEMA_VERSION,
    ORCHESTRATION_CONFIG_ENV,
    ApplicationServices,
    create_services,
)
from factor_lab.research_os.build_provenance import (
    SOURCE_BUNDLE_MANIFEST_ENV,
    SourceBundleProvenanceError,
    capture_epoch_provenance,
    capture_source_bundle_environment,
    verify_source_bundle_manifest,
    write_source_bundle_manifest,
)
from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.orchestration import ServiceNotConfigured
from factor_lab.research_os.runtime import ResearchOSSettings


ROOT = Path(__file__).resolve().parents[1]


def _bundle(tmp_path: Path) -> tuple[Path, Path, Path, dict]:
    root = tmp_path / "bundle"
    source = root / "src" / "factor_lab"
    configs = root / "configs"
    runtime = root / "infra" / "research_os"
    source.mkdir(parents=True)
    configs.mkdir(parents=True)
    runtime.mkdir(parents=True)
    (source / "engine.py").write_text("BUILD = 'exact'\n", encoding="utf-8")
    config = {
        "schema_version": APPLICATION_SERVICES_SCHEMA_VERSION,
        "repository": ".",
        "path_base": ".",
        "dependency_lock": "uv.lock",
        "iceberg": {"catalog_name": "factorlab"},
    }
    config_path = configs / "orchestration.json"
    config_path.write_text(json.dumps(config, sort_keys=True), encoding="utf-8")
    lock = root / "uv.lock"
    lock.write_text("version = 1\n", encoding="utf-8")
    (runtime / "container-entrypoint.sh").write_text(
        "#!/bin/sh\nexec \"$@\"\n", encoding="utf-8"
    )
    manifest = root / ".factor-lab-source-bundle.json"
    write_source_bundle_manifest(
        manifest, bundle_root=root, runtime_root="infra/research_os"
    )
    return root, config_path, manifest, config


def test_source_bundle_records_and_reverifies_exact_build_inputs(tmp_path: Path) -> None:
    root, config_path, manifest, _ = _bundle(tmp_path)

    provenance = verify_source_bundle_manifest(manifest, bundle_root=root)
    payload = json.loads(manifest.read_text(encoding="utf-8"))

    assert provenance.source_tree_hash == payload["source"]["tree_hash"]
    assert provenance.configuration_tree_hash == payload["configuration"]["tree_hash"]
    assert provenance.dependency_lock_hash == hashlib.sha256(
        (root / "uv.lock").read_bytes()
    ).hexdigest()
    assert provenance.manifest_hash == payload["manifest_hash"]
    assert provenance.runtime_tree_hash == payload["runtime"]["tree_hash"]
    assert config_path.relative_to(provenance.configuration_root).as_posix() in {
        entry["path"] for entry in payload["configuration"]["files"]
    }

    capture = capture_source_bundle_environment(
        manifest,
        bundle_root=root,
        dependency_lock=root / "uv.lock",
        configuration_path=config_path,
        evaluator_build="canonical-v2",
    )
    environment = capture.environment
    assert capture.provenance == provenance
    assert environment.code_hash == provenance.source_tree_hash
    assert environment.configuration_hash == provenance.configuration_tree_hash
    assert environment.dependency_lock_hash == provenance.dependency_lock_hash
    assert environment.dirty_patch_hash is None


def test_epoch_provenance_uses_verified_image_manifest_not_operator_hashes(
    tmp_path: Path,
) -> None:
    root, config_path, manifest, _ = _bundle(tmp_path)
    proof = verify_source_bundle_manifest(manifest, bundle_root=root)

    epoch = capture_epoch_provenance(
        configuration_path=config_path,
        repository=root,
        manifest_path=manifest,
    )

    assert epoch.provenance_kind == "immutable_source_bundle"
    assert epoch.code_hash == proof.source_tree_hash
    assert epoch.configuration_hash == proof.configuration_tree_hash
    assert epoch.dependency_lock_hash == proof.dependency_lock_hash
    assert epoch.dirty_patch_hash == proof.manifest_hash
    assert epoch.image_source_digest == proof.manifest_hash
    assert epoch.formal_epoch_eligible is False


def test_epoch_provenance_binds_daemon_inspected_oci_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, config_path, manifest, _ = _bundle(tmp_path)
    image_id = "sha256:" + "9" * 64
    repo_digest = "factor-lab-research-os@sha256:" + "8" * 64
    base_digest = "sha256:" + "7" * 64
    monkeypatch.setattr(
        build_provenance_module,
        "_inspect_oci_image",
        lambda reference: (image_id, (repo_digest,), (base_digest,)),
    )

    epoch = capture_epoch_provenance(
        configuration_path=config_path,
        repository=root,
        manifest_path=manifest,
        image_reference="factor-lab-research-os:verified",
    )

    assert epoch.formal_epoch_eligible is True
    assert epoch.oci_image_id == image_id
    assert epoch.oci_repo_digests == (repo_digest,)
    assert epoch.oci_base_digests == (base_digest,)
    assert epoch.dirty_patch_hash != epoch.image_source_digest


def test_local_compose_image_id_is_formal_without_registry_repo_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, config_path, manifest, _ = _bundle(tmp_path)
    image_id = "sha256:" + "9" * 64
    base_digest = "sha256:" + "7" * 64
    monkeypatch.setattr(
        build_provenance_module,
        "_inspect_oci_image",
        lambda reference: (image_id, (), (base_digest,)),
    )

    epoch = capture_epoch_provenance(
        configuration_path=config_path,
        repository=root,
        manifest_path=manifest,
        image_reference="factor-lab-research-os:local",
    )

    assert epoch.formal_epoch_eligible is True
    assert epoch.oci_image_id == image_id
    assert epoch.oci_repo_digests == ()


def test_epoch_provenance_measures_git_dirty_worktree(tmp_path: Path) -> None:
    root, config_path, _, _ = _bundle(tmp_path)
    subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "research-os@example.invalid"],
        cwd=root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Research OS test"], cwd=root, check=True
    )
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(
        ["git", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True
    )

    clean = capture_epoch_provenance(
        configuration_path=config_path,
        repository=root,
    )
    assert clean.provenance_kind == "git_worktree"
    assert clean.dirty_patch_hash == hashlib.sha256(b"").hexdigest()

    (root / "src" / "factor_lab" / "engine.py").write_text(
        "BUILD = 'dirty'\n", encoding="utf-8"
    )
    dirty = capture_epoch_provenance(
        configuration_path=config_path,
        repository=root,
    )
    assert dirty.git_commit == clean.git_commit
    assert dirty.code_hash != clean.code_hash
    assert dirty.dirty_patch_hash != clean.dirty_patch_hash
    assert dirty.build_identity_hash != clean.build_identity_hash


def test_epoch_provenance_hashes_untracked_file_contents(tmp_path: Path) -> None:
    root, config_path, _, _ = _bundle(tmp_path)
    subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "research-os@example.invalid"],
        cwd=root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Research OS test"], cwd=root, check=True
    )
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(
        ["git", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True
    )

    untracked = root / "runtime-note.txt"
    untracked.write_text("first payload\n", encoding="utf-8")
    first = capture_epoch_provenance(
        configuration_path=config_path,
        repository=root,
    )
    untracked.write_text("second payload\n", encoding="utf-8")
    second = capture_epoch_provenance(
        configuration_path=config_path,
        repository=root,
    )

    assert first.git_commit == second.git_commit
    assert first.code_hash == second.code_hash
    assert first.configuration_hash == second.configuration_hash
    assert first.dirty_patch_hash != second.dirty_patch_hash
    assert first.build_identity_hash != second.build_identity_hash


@pytest.mark.parametrize(
    "target", ["source", "configuration", "lock", "runtime", "added"]
)
def test_source_bundle_fails_closed_for_any_build_input_change(
    tmp_path: Path, target: str
) -> None:
    root, config_path, manifest, _ = _bundle(tmp_path)
    if target == "source":
        (root / "src" / "factor_lab" / "engine.py").write_text(
            "BUILD = 'mutated'\n", encoding="utf-8"
        )
    elif target == "configuration":
        config_path.write_text("{}", encoding="utf-8")
    elif target == "lock":
        (root / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    elif target == "runtime":
        (root / "infra" / "research_os" / "container-entrypoint.sh").write_text(
            "#!/bin/sh\nexit 9\n", encoding="utf-8"
        )
    else:
        (root / "src" / "factor_lab" / "unrecorded.py").write_text(
            "VALUE = 1\n", encoding="utf-8"
        )

    with pytest.raises(SourceBundleProvenanceError, match="does not match"):
        verify_source_bundle_manifest(manifest, bundle_root=root)


def test_source_bundle_requires_selected_config_and_lock_from_verified_bundle(
    tmp_path: Path,
) -> None:
    root, config_path, manifest, _ = _bundle(tmp_path)
    external = tmp_path / "external.json"
    external.write_text(config_path.read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(SourceBundleProvenanceError, match="outside the verified"):
        capture_source_bundle_environment(
            manifest,
            bundle_root=root,
            dependency_lock=root / "uv.lock",
            configuration_path=external,
            evaluator_build="canonical-v2",
        )
    other_lock = tmp_path / "uv.lock"
    other_lock.write_bytes((root / "uv.lock").read_bytes())
    with pytest.raises(SourceBundleProvenanceError, match="not the dependency lock"):
        capture_source_bundle_environment(
            manifest,
            bundle_root=root,
            dependency_lock=other_lock,
            configuration_path=config_path,
            evaluator_build="canonical-v2",
        )


def test_application_services_accepts_verified_bundle_without_git(tmp_path: Path) -> None:
    root, config_path, manifest, config = _bundle(tmp_path)
    settings = ResearchOSSettings(
        database_url=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        lake_root=tmp_path / "lake",
        snapshot_root=tmp_path / "snapshots",
        environment="test",
    )
    service = ApplicationServices(
        config,
        settings=settings,
        catalog=ResearchCatalog(settings.database_url),
        iceberg_publisher=object(),
        config_base=root,
        source_bundle_manifest=manifest,
        configuration_path=config_path,
    )
    provenance = verify_source_bundle_manifest(manifest, bundle_root=root)

    assert not (root / ".git").exists()
    assert service._environment_hashes == {
        "code_hash": provenance.source_tree_hash,
        "dependency_lock_hash": provenance.dependency_lock_hash,
        "config_hash": provenance.configuration_tree_hash,
        "dirty_patch_hash": hashlib.sha256(b"").hexdigest(),
        "build_provenance_hash": provenance.manifest_hash,
    }


def test_application_services_without_git_or_bundle_proof_fails_closed(
    tmp_path: Path,
) -> None:
    root, _, _, config = _bundle(tmp_path)
    settings = ResearchOSSettings(
        database_url=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        lake_root=tmp_path / "lake",
        snapshot_root=tmp_path / "snapshots",
        environment="test",
    )
    with pytest.raises(ServiceNotConfigured, match="exact build provenance"):
        ApplicationServices(
            config,
            settings=settings,
            catalog=ResearchCatalog(settings.database_url),
            iceberg_publisher=object(),
            config_base=root,
        )


def test_create_services_passes_config_file_and_build_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, config_path, manifest, _ = _bundle(tmp_path)
    sentinel_catalog = object()
    sentinel_publisher = object()
    sentinel_archive = object()
    captured: dict = {}

    monkeypatch.setenv(ORCHESTRATION_CONFIG_ENV, str(config_path))
    monkeypatch.setenv(SOURCE_BUNDLE_MANIFEST_ENV, str(manifest))
    monkeypatch.setattr(
        application_services_module, "ResearchCatalog", lambda _url: sentinel_catalog
    )
    monkeypatch.setattr(
        application_services_module,
        "PyIcebergGoldPublisher",
        lambda **_kwargs: sentinel_publisher,
    )
    monkeypatch.setattr(
        application_services_module.S3ImmutableArchive,
        "from_connection",
        lambda **_kwargs: sentinel_archive,
    )

    class FakeSettings:
        database_url = "sqlite://"
        object_store_endpoint = "http://object-store"
        object_store_bucket = "factor-lab"
        object_store_access_key = "access"
        object_store_secret_key = "secret"

    monkeypatch.setattr(
        application_services_module.ResearchOSSettings,
        "from_env",
        lambda *_args, **_kwargs: FakeSettings(),
    )

    def fake_application_services(config, **kwargs):
        captured["config"] = config
        captured.update(kwargs)
        return "services"

    monkeypatch.setattr(
        application_services_module, "ApplicationServices", fake_application_services
    )

    assert create_services() == "services"
    assert captured["source_bundle_manifest"] == str(manifest)
    assert captured["configuration_path"] == config_path.resolve()
    assert captured["catalog"] is sentinel_catalog
    assert captured["iceberg_publisher"] is sentinel_publisher
    assert captured["object_store_archive"] is sentinel_archive


def test_dagster_image_generates_and_requires_source_bundle_manifest() -> None:
    dockerfile = (ROOT / "infra" / "research_os" / "Dockerfile.dagster").read_text(
        encoding="utf-8"
    )
    assert (
        f"{SOURCE_BUNDLE_MANIFEST_ENV}=/opt/factor-lab/.factor-lab-source-bundle.json"
        in dockerfile
    )
    assert "build_provenance.py" in dockerfile
    assert "--source-root src" in dockerfile
    assert "--configuration-root configs" in dockerfile
    assert "--dependency-lock uv.lock" in dockerfile
    assert "--runtime-root infra/research_os" in dockerfile
    assert dockerfile.rindex("uv sync --frozen") < dockerfile.index(
        "RUN python /opt/factor-lab/src/factor_lab/research_os/build_provenance.py"
    )
    assert "chmod -R a-w /opt/factor-lab/src /opt/factor-lab/configs" in dockerfile
    assert "chown -R dagster:dagster /opt/dagster /opt/factor-lab" not in dockerfile
