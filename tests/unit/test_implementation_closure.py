from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import factor_lab.implementation_closure as closure_module
from factor_lab.implementation_closure import (
    ImplementationClosureError,
    verify_implementation_closure,
)


ROOT = Path(__file__).resolve().parents[2]
UPDATE_SCRIPT = ROOT / "scripts/update-runtime-closure.py"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _fixture(
    root: Path,
    *,
    working_implementation: bytes = b"GENERATOR = 'fixed'\n",
    published_implementation: bytes | None = None,
) -> tuple[Path, dict[str, bytes]]:
    implementation = root / "src/factor_lab/generator.py"
    implementation.parent.mkdir(parents=True)
    implementation.write_bytes(working_implementation)
    published_bytes = (
        working_implementation
        if published_implementation is None
        else published_implementation
    )
    payload = {
        "schema_version": 1,
        "python_version": "3.10.16",
        "python_implementation": "CPython",
        "python_runtime": sys.version,
        "platform_system": "Windows",
        "platform_machine": "AMD64",
        "platform_tag": "win-amd64",
        "distributions": {"numeric-runtime": "1.2.3"},
        "files": [
            {
                "path": "src/factor_lab/generator.py",
                "sha256": hashlib.sha256(published_bytes).hexdigest(),
            }
        ],
    }
    manifest = {
        "generator_id": "generator/5.2",
        "generator_entrypoint": "factor_lab.generator:generate",
        "runtime_closure": {
            **payload,
            "payload_sha256": hashlib.sha256(_canonical(payload)).hexdigest(),
        },
    }
    manifest_path = root / "protocols/implementation.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(json.dumps(manifest, indent=2).encode("utf-8"))
    return manifest_path, {
        "protocols/implementation.json": manifest_path.read_bytes(),
        "src/factor_lab/generator.py": published_bytes,
    }


def _runtime(monkeypatch: pytest.MonkeyPatch, root: Path, published: dict[str, bytes]) -> None:
    monkeypatch.setattr(closure_module.platform, "python_version", lambda: "3.10.16")
    monkeypatch.setattr(
        closure_module.platform, "python_implementation", lambda: "CPython"
    )
    monkeypatch.setattr(closure_module.platform, "system", lambda: "Windows")
    monkeypatch.setattr(closure_module.platform, "machine", lambda: "AMD64")
    monkeypatch.setattr(
        closure_module.sysconfig, "get_platform", lambda: "win-amd64"
    )
    monkeypatch.setattr(
        closure_module.importlib.metadata,
        "distributions",
        lambda: [
            SimpleNamespace(
                metadata={"Name": "numeric-runtime"}, version="1.2.3"
            )
        ],
    )

    def fake_run(argv, *, cwd, check, capture_output):
        assert Path(cwd) == root
        assert check is True and capture_output is True
        relative = str(argv[-1]).split(":", maxsplit=1)[1]
        return subprocess.CompletedProcess(argv, 0, stdout=published[relative], stderr=b"")

    monkeypatch.setattr(closure_module.subprocess, "run", fake_run)


def test_runtime_closure_binds_local_files_versions_and_published_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, published = _fixture(tmp_path)
    _runtime(monkeypatch, tmp_path, published)

    result = verify_implementation_closure(
        tmp_path,
        manifest_path=manifest,
        manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        implementation_commit_oid="a" * 40,
        generator_id="generator/5.2",
        generator_entrypoint="factor_lab.generator:generate",
    )
    assert result["files"][0]["path"] == "src/factor_lab/generator.py"


def test_runtime_closure_accepts_crlf_worktree_for_published_lf_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, published = _fixture(
        tmp_path,
        working_implementation=b"GENERATOR = 'fixed'\r\nNEXT = True\r\n",
        published_implementation=b"GENERATOR = 'fixed'\nNEXT = True\n",
    )
    _runtime(monkeypatch, tmp_path, published)

    result = verify_implementation_closure(
        tmp_path,
        manifest_path=manifest,
        manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        implementation_commit_oid="a" * 40,
        generator_id="generator/5.2",
        generator_entrypoint="factor_lab.generator:generate",
    )

    assert result["files"][0]["sha256"] == hashlib.sha256(
        b"GENERATOR = 'fixed'\nNEXT = True\n"
    ).hexdigest()


def test_runtime_closure_rejects_post_release_source_edit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, published = _fixture(tmp_path)
    _runtime(monkeypatch, tmp_path, published)
    (tmp_path / "src/factor_lab/generator.py").write_bytes(b"GENERATOR = 'changed'\n")

    with pytest.raises(ImplementationClosureError, match="running implementation file"):
        verify_implementation_closure(
            tmp_path,
            manifest_path=manifest,
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
            implementation_commit_oid="a" * 40,
            generator_id="generator/5.2",
            generator_entrypoint="factor_lab.generator:generate",
        )


def test_runtime_closure_rejects_an_extra_installed_distribution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, published = _fixture(tmp_path)
    _runtime(monkeypatch, tmp_path, published)
    monkeypatch.setattr(
        closure_module.importlib.metadata,
        "distributions",
        lambda: [
            SimpleNamespace(
                metadata={"Name": "numeric-runtime"}, version="1.2.3"
            ),
            SimpleNamespace(metadata={"Name": "unlocked"}, version="9.9"),
        ],
    )

    with pytest.raises(ImplementationClosureError, match="distribution set differs"):
        verify_implementation_closure(
            tmp_path,
            manifest_path=manifest,
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
            implementation_commit_oid="a" * 40,
            generator_id="generator/5.2",
            generator_entrypoint="factor_lab.generator:generate",
        )


def test_runtime_closure_rejects_published_blob_outside_declared_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, published = _fixture(tmp_path)
    published["src/factor_lab/generator.py"] = b"GENERATOR = 'other'\n"
    _runtime(monkeypatch, tmp_path, published)

    with pytest.raises(ImplementationClosureError, match="published implementation file"):
        verify_implementation_closure(
            tmp_path,
            manifest_path=manifest,
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
            implementation_commit_oid="a" * 40,
            generator_id="generator/5.2",
            generator_entrypoint="factor_lab.generator:generate",
        )


def test_runtime_closure_rejects_manifest_not_in_published_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, published = _fixture(tmp_path)
    published["protocols/implementation.json"] += b"\n"
    _runtime(monkeypatch, tmp_path, published)

    with pytest.raises(ImplementationClosureError, match="manifest differs"):
        verify_implementation_closure(
            tmp_path,
            manifest_path=manifest,
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
            implementation_commit_oid="a" * 40,
            generator_id="generator/5.2",
            generator_entrypoint="factor_lab.generator:generate",
        )


def test_runtime_closure_rejects_boolean_schema_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, published = _fixture(tmp_path)
    value = json.loads(manifest.read_text(encoding="utf-8"))
    value["runtime_closure"]["schema_version"] = True
    manifest.write_bytes(json.dumps(value, indent=2).encode("utf-8"))
    published["protocols/implementation.json"] = manifest.read_bytes()
    _runtime(monkeypatch, tmp_path, published)

    with pytest.raises(ImplementationClosureError, match="unsupported runtime closure schema"):
        verify_implementation_closure(
            tmp_path,
            manifest_path=manifest,
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
            implementation_commit_oid="a" * 40,
            generator_id="generator/5.2",
            generator_entrypoint="factor_lab.generator:generate",
        )


def test_update_script_hashes_the_git_lf_form_of_crlf_closure_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = importlib.util.spec_from_file_location(
        "factor_lab_update_runtime_closure_test", UPDATE_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)

    root = tmp_path / "project"
    manifest = root / "protocols/implementation.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text('{"generator_id":"generator/5.2"}\n', encoding="utf-8")
    sources = {
        "pyproject.toml": b'[project]\r\nversion = "5.2.0"\r\n',
        "configs/data.json": b'{\r\n  "schema_version": 1\r\n}\r\n',
        "protocols/5.2-runtime-lock.txt": b"numeric-runtime==1.2.3\r\n",
        "src/factor_lab/__init__.py": b'__version__ = "5.2.0"\r\n',
    }
    for relative, raw in sources.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    monkeypatch.setattr(script, "ROOT", root)
    monkeypatch.setattr(script, "DISTRIBUTIONS", ("numeric-runtime",))
    monkeypatch.setattr(script.platform, "python_version", lambda: "3.10.16")
    monkeypatch.setattr(script.platform, "python_implementation", lambda: "CPython")
    monkeypatch.setattr(script.platform, "system", lambda: "Windows")
    monkeypatch.setattr(script.platform, "machine", lambda: "AMD64")
    monkeypatch.setattr(script.sysconfig, "get_platform", lambda: "win-amd64")
    monkeypatch.setattr(
        script.importlib.metadata,
        "version",
        lambda name: "1.2.3" if name == "numeric-runtime" else "unexpected",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["update-runtime-closure.py", "--manifest", str(manifest)],
    )

    assert script.main() == 0
    closure = json.loads(manifest.read_text(encoding="utf-8"))["runtime_closure"]
    hashes = {row["path"]: row["sha256"] for row in closure["files"]}
    for relative, raw in sources.items():
        assert hashes[relative] == hashlib.sha256(
            raw.replace(b"\r\n", b"\n")
        ).hexdigest()


def test_update_script_requires_the_release_environment_to_match_the_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = importlib.util.spec_from_file_location(
        "factor_lab_update_runtime_lock_test", UPDATE_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)

    runtime_lock = tmp_path / "protocols/5.2-runtime-lock.txt"
    runtime_lock.parent.mkdir(parents=True)
    runtime_lock.write_text(
        "numeric-runtime==1.2.3 --hash=sha256:" + "a" * 64 + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(script, "ROOT", tmp_path)

    exact = {"numeric-runtime": "1.2.3"}
    script._verify_environment_matches_lock(exact)
    with pytest.raises(SystemExit, match="differs from runtime lock"):
        script._verify_environment_matches_lock({**exact, "unlocked": "9.9"})


def test_closure_text_types_are_declared_git_lf() -> None:
    attributes = (ROOT / ".gitattributes").read_text(encoding="utf-8").splitlines()

    assert "*.py text eol=lf" in attributes
    assert "*.toml text eol=lf" in attributes
    assert "*.json text eol=lf" in attributes
    assert "protocols/*-runtime-lock.txt text eol=lf" in attributes


def test_release_manifest_declares_the_frozen_positive_amount_median() -> None:
    manifest = json.loads(
        (ROOT / "protocols/5.2-target-generator.json").read_text(encoding="utf-8")
    )

    assert manifest["membership_contract"] == {
        "rule_id": "prospective_top500_membership_5_2",
        "effective_month": "2026-09",
        "liquidity_session_count": 60,
        "minimum_positive_amount_observations": 20,
        "membership_size": 500,
        "amount_unit": "Tushare_thousand_CNY_times_1000",
        "liquidity_statistic": "median_positive_amount_rmb",
        "rank_order": "median_amount_60d_desc_then_ticker_asc",
        "ineligible_member_policy": (
            "retain_member_and_set_eligible_false_without_replacement"
        ),
        "artifact_pattern": (
            "runtime/prospective/5.0/membership/<YYYY-MM>/<artifact_sha256>"
        ),
    }
