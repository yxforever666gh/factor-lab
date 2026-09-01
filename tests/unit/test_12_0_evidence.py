from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "protocols" / "evidence" / "12.0" / "development-screening-failure.json"


def _git_bytes(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, stdout=subprocess.PIPE
    ).stdout


def test_terminal_evidence_payload_and_file_hashes() -> None:
    value = json.loads(PATH.read_text(encoding="utf-8"))
    assert value["payload_sha256"] == canonical_payload_sha256(value)
    assert file_sha256(PATH) == "e79349c142fc1bad4cfc9a587ca9abd38c04a1c0fd05947e543079dbbe95f20a"
    assert value["formal_development_panel"]["manifest_payload_sha256"] == "5b4815885c500656af0c597f1cf4b0030932b7ea82556a052a376b494f61a870"
    assert value["formal_screening"]["manifest_payload_sha256"] == "f65869e25012efd6720378984652568b49dc8fbcd183b390237c54e64df3d25f"


def test_terminal_evidence_binds_exact_implementation_commit_bytes() -> None:
    value = json.loads(PATH.read_text(encoding="utf-8"))
    implementation = value["implementation"]
    commit = implementation["git_commit"]
    assert _git_bytes("merge-base", "--is-ancestor", commit, "HEAD") == b""
    for relative, expected in implementation["files"].items():
        actual = hashlib.sha256(_git_bytes("show", f"{commit}:{relative}")).hexdigest()
        assert actual == expected


def test_terminal_result_is_single_gate_failure_and_keeps_later_data_closed() -> None:
    value = json.loads(PATH.read_text(encoding="utf-8"))
    assert value["status"] == "development_screening_falsified_max_drawdown_selection_unopened"
    assert value["gate"]["failed_checks"] == [
        "base_max_drawdown_at_least_negative_0_35"
    ]
    assert value["gate"]["all_other_frozen_screening_checks_passed"] is True
    boundary = value["evidence_boundary"]
    assert boundary["selection_market_partitions_read"] is False
    assert boundary["real_share_100_lot_gate_opened"] is False
    assert boundary["selected_candidate_id"] is None
    assert value["metrics"]["candidate_base"]["max_drawdown"] < -0.35
