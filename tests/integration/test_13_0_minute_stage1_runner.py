from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-13.0-minute-stage1.py"


def _module():
    spec = importlib.util.spec_from_file_location(
        "factor_lab_13_0_minute_stage1_runner", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stage1_runner_binds_protocol_and_suspension_artifact() -> None:
    module = _module()
    protocol = module._read_protocol()
    suspensions = module._load_suspensions()
    assert protocol["payload_sha256"] == module.PROTOCOL_PAYLOAD_SHA256
    assert len(suspensions) == module.SUSPENSIONS_ROW_COUNT == 170_703
    assert tuple(suspensions.columns) == (
        "ticker",
        "date",
        "suspend_type",
        "suspend_timing",
    )
    implementation = module._implementation_files()
    assert set(implementation) == {
        path.as_posix() for path in module.IMPLEMENTATION_FILES
    }
    assert all(len(value) == 64 for value in implementation.values())


def test_stage1_runner_requires_external_minute_manifest_anchors(
    tmp_path: Path,
) -> None:
    module = _module()
    output = tmp_path / "result"
    with pytest.raises(ValueError, match="minute external manifest anchors"):
        module.run_stage1(
            output,
            minute_root=tmp_path / "minutes",
            minute_manifest_payload_sha256="",
            minute_manifest_file_sha256="",
            actions_root=tmp_path / "actions",
            actions_manifest_payload_sha256="0" * 64,
            actions_manifest_file_sha256="1" * 64,
        )
    assert not output.exists()


def test_stage1_verifier_requires_external_manifest_file_anchor(
    tmp_path: Path,
) -> None:
    module = _module()
    with pytest.raises(ValueError, match="external manifest file"):
        module.verify_stage1(
            tmp_path / "absent",
            expected_manifest_payload_sha256="0" * 64,
            expected_manifest_file_sha256="1" * 64,
        )
