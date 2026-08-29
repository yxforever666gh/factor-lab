from __future__ import annotations

import json
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import factor_lab.adaptive_shadow_runtime as runtime
import factor_lab.cli as cli
from factor_lab.adaptive_shadow_store import ShadowStoreIntegrityError


FORMAL_HEAD = "f" * 64
TAG_OBJECT = "a" * 40
TAG_COMMIT = "b" * 40
SOURCE_SHA = "1" * 64
FORMAL_INPUT_SHA = "2" * 64
FORMAL_DECISION = "3" * 64


def _protocol_bytes() -> bytes:
    path = Path(__file__).resolve().parents[2] / "protocols/5.9-adaptive-shadow.json"
    return path.read_bytes()


def _write_protocol(project: Path) -> tuple[Path, bytes]:
    path = project / "protocols/5.9-adaptive-shadow.json"
    path.parent.mkdir(parents=True)
    raw = _protocol_bytes()
    path.write_bytes(raw)
    return path, raw


def _valid_formal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime,
        "audit_ledger",
        lambda _root, **_kwargs: {
            "valid": True,
            "record_count": 12,
            "head_sequence": 12,
            "head_record_sha256": FORMAL_HEAD,
            "state": {"decision_count": 0},
        },
    )
    monkeypatch.setattr(
        runtime,
        "ledger_status",
        lambda _root, **_kwargs: {
            "valid": True,
            "status": "awaiting_new_data",
            "record_count": 12,
            "head_record_sha256": FORMAL_HEAD,
            "decision_count": 0,
        },
    )


def _protocol() -> dict[str, object]:
    return json.loads(_protocol_bytes().decode("utf-8"))


def _planning_input() -> SimpleNamespace:
    rows = [
        {
            "date": "2026-09-10",
            "ticker": f"T{index:03d}",
            "shadow_eligible": True,
            "low_turnover_20_v1": float(100 - index),
            "low_volatility_252_v1": float(index),
        }
        for index in range(16)
    ]
    return SimpleNamespace(
        signal_date="2026-09-10",
        trade_date="2026-09-11",
        snapshot_sha256=SOURCE_SHA,
        shadow_target_rows_sha256="4" * 64,
        shadow_target_frame=rows,
    )


def _formal_plan() -> dict[str, object]:
    route = {
        "route": "fixed_core_full",
        "input_snapshot_sha256": FORMAL_INPUT_SHA,
        "signal_date": "2026-09-10",
        "trade_date": "2026-09-11",
        "calendar_index": 20,
        "due_offset": 0,
    }
    return {
        "decision_session": "2026-09-11",
        "source_data_snapshot_sha256": SOURCE_SHA,
        "admission_deadline_utc": "2026-09-11T01:15:00Z",
        "route_target_plan": route,
        "route_target_plan_sha256": runtime.canonical_sha256(route),
    }


def _activate_store(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    protocol = _protocol()
    raw_sha = hashlib.sha256(_protocol_bytes()).hexdigest()
    registry = runtime.build_registry_from_protocol(
        protocol,
        release_tag="5.9",
        commit_oid=TAG_COMMIT,
        released_at_utc="2026-09-01T00:00:00Z",
        start_after="2026-09-01",
    )
    runtime.activate_shadow_store(
        root,
        registry=registry,
        release_tag_object_oid=TAG_OBJECT,
        release_commit_oid=TAG_COMMIT,
        protocol_sha256=raw_sha,
        formal_head_record_sha256=FORMAL_HEAD,
        released_at_utc="2026-09-01T00:00:00Z",
        start_after="2026-09-01",
        recorded_at_utc="2026-09-01T00:00:01Z",
    )
    monkeypatch.setattr(
        runtime,
        "load_release_bound_protocol",
        lambda *args, **kwargs: (
            protocol,
            raw_sha,
            "protocols/5.9-adaptive-shadow.json",
        ),
    )
    monkeypatch.setattr(
        runtime,
        "_load_formal_decision_plan",
        lambda *args, **kwargs: (
            _formal_plan(),
            {
                "activation_head_record_sha256": FORMAL_HEAD,
                "decision_record_sha256": FORMAL_DECISION,
            },
        ),
    )
    monkeypatch.setattr(
        runtime,
        "load_prospective_input_snapshot",
        lambda _path: _planning_input(),
    )


def test_activate_verifies_tagged_protocol_and_binds_formal_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protocol_path, raw = _write_protocol(tmp_path)
    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=raw),
    )
    _valid_formal(monkeypatch)
    captured: dict[str, object] = {}

    def activate(root: Path, **kwargs: object) -> dict[str, object]:
        captured["root"] = Path(root)
        captured.update(kwargs)
        return {"created": True, "record_sha256": "c" * 64}

    monkeypatch.setattr(runtime, "activate_shadow_store", activate)
    shadow_root = tmp_path / "runtime/adaptive-shadow/1"
    result = runtime.activate_shadow_runtime(
        tmp_path,
        shadow_root,
        tmp_path / "runtime/prospective/5.0",
        protocol_path=protocol_path,
        release_tag="5.9",
        release_tag_object_oid=TAG_OBJECT,
        release_commit_oid=TAG_COMMIT,
        start_after="2026-08-30",
        released_at_utc="2026-08-30T10:00:00Z",
        recorded_at_utc="2026-08-30T10:00:01Z",
    )

    assert result["status"] == "activated"
    assert result["formal_binding"]["head_record_sha256"] == FORMAL_HEAD
    assert result["protocol_path"] == "protocols/5.9-adaptive-shadow.json"
    assert captured["formal_head_record_sha256"] == FORMAL_HEAD
    assert captured["protocol_sha256"] == result["protocol_sha256"]
    assert captured["registry"].release_tag == "5.9"
    assert captured["registry"].commit_oid == TAG_COMMIT


def test_release_protocol_must_match_tagged_blob_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protocol_path, raw = _write_protocol(tmp_path)
    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=raw + b"\n"),
    )

    with pytest.raises(runtime.AdaptiveShadowRuntimeError, match="bytes differ"):
        runtime.load_release_bound_protocol(
            tmp_path,
            protocol_path,
            release_commit_oid=TAG_COMMIT,
        )


@pytest.mark.parametrize(
    ("audit_valid", "audit_decisions", "status_decisions", "message"),
    [
        (False, 0, 0, "pass both audit"),
        (True, 1, 1, "forbidden after the first formal decision"),
        (True, 0, 1, "forbidden after the first formal decision"),
    ],
)
def test_formal_ledger_must_be_valid_and_predecision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    audit_valid: bool,
    audit_decisions: int,
    status_decisions: int,
    message: str,
) -> None:
    monkeypatch.setattr(
        runtime,
        "audit_ledger",
        lambda _root, **_kwargs: {
            "valid": audit_valid,
            "record_count": 12,
            "head_sequence": 12,
            "head_record_sha256": FORMAL_HEAD,
            "state": {"decision_count": audit_decisions},
        },
    )
    monkeypatch.setattr(
        runtime,
        "ledger_status",
        lambda _root, **_kwargs: {
            "valid": True,
            "status": "awaiting_new_data",
            "record_count": 12,
            "head_record_sha256": FORMAL_HEAD,
            "decision_count": status_decisions,
        },
    )

    with pytest.raises(runtime.AdaptiveShadowRuntimeError, match=message):
        runtime._formal_activation_binding(tmp_path)


def test_cli_activate_uses_release_verifier_and_default_runtime_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        cli,
        "_published_tag_metadata",
        lambda root, tag: (TAG_OBJECT, TAG_COMMIT, "2020-08-30T10:00:00Z"),
    )

    def activate(project: Path, shadow: Path, formal: Path, **kwargs: object) -> dict[str, object]:
        captured.update(
            {"project": project, "shadow": shadow, "formal": formal, **kwargs}
        )
        return {"schema_version": 1, "status": "activated"}

    monkeypatch.setattr(cli, "activate_shadow_runtime", activate)
    assert (
        cli.main(
            [
                "--root",
                str(tmp_path),
                "adaptive-shadow",
                "activate",
                "--release-tag",
                "5.9",
                "--protocol",
                "protocols/5.9-adaptive-shadow.json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "activated"
    assert captured["shadow"] == (tmp_path / "runtime/adaptive-shadow/1").resolve()
    assert captured["formal"] == (tmp_path / "runtime/prospective/5.0").resolve()
    assert captured["release_tag_object_oid"] == TAG_OBJECT
    assert captured["release_commit_oid"] == TAG_COMMIT


def test_cli_status_and_audit_emit_json_with_integrity_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "shadow_store_status",
        lambda _root: {"status": "active", "integrity_valid": True},
    )
    assert cli.main(["--root", str(tmp_path), "adaptive-shadow", "status"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "active",
        "integrity_valid": True,
    }

    def broken(*_args: object) -> dict[str, object]:
        raise ShadowStoreIntegrityError("tampered record")

    monkeypatch.setattr(cli, "audit_adaptive_shadow_runtime", broken)
    assert cli.main(["--root", str(tmp_path), "adaptive-shadow", "audit"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "invalid"
    assert report["integrity_valid"] is False
    assert report["issues"][0]["code"] == "invalid_shadow_store"


def test_plan_seals_two_candidates_and_repeated_call_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shadow_root = tmp_path / "runtime/adaptive-shadow/1"
    _activate_store(shadow_root, monkeypatch)

    first = runtime.plan_shadow_runtime(
        tmp_path,
        shadow_root,
        tmp_path / "runtime/prospective/5.0",
        formal_plan_path=tmp_path / "formal-plan.json",
        formal_decision_record_sha256=FORMAL_DECISION,
        input_snapshot_path=tmp_path / SOURCE_SHA,
        created_at_utc="2026-09-10T12:00:00Z",
    )
    second = runtime.plan_shadow_runtime(
        tmp_path,
        shadow_root,
        tmp_path / "runtime/prospective/5.0",
        formal_plan_path=tmp_path / "formal-plan.json",
        formal_decision_record_sha256=FORMAL_DECISION,
        input_snapshot_path=tmp_path / SOURCE_SHA,
        created_at_utc="2026-09-10T13:00:00Z",
    )
    audit = runtime._shadow_store.audit_shadow_store(shadow_root)

    assert first["status"] == second["status"] == "planned"
    assert len(first["plans"]) == len(second["plans"]) == 2
    assert all(row["created"] is True for row in first["plans"])
    assert all(row["created"] is False for row in second["plans"])
    assert first["planning_intent"]["created"] is True
    assert second["planning_intent"]["created"] is False
    assert audit["planning_intent_count"] == 1
    assert audit["plan_count"] == 2
    assert audit["missed_count"] == 0


def test_plan_after_deadline_records_two_permanent_misses_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shadow_root = tmp_path / "runtime/adaptive-shadow/1"
    _activate_store(shadow_root, monkeypatch)

    first = runtime.plan_shadow_runtime(
        tmp_path,
        shadow_root,
        tmp_path / "runtime/prospective/5.0",
        formal_plan_path=tmp_path / "formal-plan.json",
        formal_decision_record_sha256=FORMAL_DECISION,
        input_snapshot_path=tmp_path / SOURCE_SHA,
        created_at_utc="2026-09-11T01:15:01Z",
    )
    second = runtime.plan_shadow_runtime(
        tmp_path,
        shadow_root,
        tmp_path / "runtime/prospective/5.0",
        formal_plan_path=tmp_path / "formal-plan.json",
        formal_decision_record_sha256=FORMAL_DECISION,
        input_snapshot_path=tmp_path / SOURCE_SHA,
        created_at_utc="2026-09-11T02:00:00Z",
    )
    audit = runtime._shadow_store.audit_shadow_store(shadow_root)

    assert first["status"] == second["status"] == "missed"
    assert len(first["missed"]) == len(second["missed"]) == 2
    assert all(row["created"] is True for row in first["missed"])
    assert all(row["created"] is False for row in second["missed"])
    assert audit["plan_count"] == 0
    assert audit["missed_count"] == 2


def test_prior_targets_use_latest_same_offset_plan() -> None:
    candidates = ("low_turnover_20_v1", "low_volatility_252_v1")
    state = SimpleNamespace(
        plans={
            (candidates[0], "2026-09-01", 0): (
                "a" * 64,
                "b" * 64,
                {"targets_ppm": {"OLD": 1_000_000}},
            ),
            (candidates[0], "2026-09-09", 0): (
                "c" * 64,
                "d" * 64,
                {"targets_ppm": {"LATEST": 1_000_000}},
            ),
            (candidates[1], "2026-09-08", 0): (
                "e" * 64,
                "f" * 64,
                {"targets_ppm": {"REV": 1_000_000}},
            ),
            (candidates[0], "2026-09-09", 1): (
                "1" * 64,
                "2" * 64,
                {"targets_ppm": {"OTHER_OFFSET": 1_000_000}},
            ),
        }
    )

    prior = runtime._prior_targets(
        state,
        candidate_ids=candidates,
        signal_date="2026-09-10",
        due_offset=0,
    )

    assert prior[candidates[0]][0] == ["LATEST"]
    assert prior[candidates[1]][0] == ["REV"]


def test_formal_source_cross_binding_is_required_even_for_missed_cycle() -> None:
    bad = _formal_plan()
    bad["source_data_snapshot_sha256"] = "9" * 64

    with pytest.raises(runtime.AdaptiveShadowRuntimeError, match="not cross-bound"):
        runtime._formal_plan_coordinates(
            bad,
            _planning_input(),
            formal_decision_record_sha256=FORMAL_DECISION,
        )


@pytest.mark.parametrize(("status", "exit_code"), [("planned", 0), ("missed", 2)])
def test_cli_plan_reports_planned_or_missed_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: str,
    exit_code: int,
) -> None:
    captured: dict[str, object] = {}

    def plan(*args: object, **kwargs: object) -> dict[str, object]:
        captured["args"] = args
        captured.update(kwargs)
        return {"schema_version": 1, "status": status}

    monkeypatch.setattr(cli, "plan_shadow_runtime", plan)
    result = cli.main(
        [
            "--root",
            str(tmp_path),
            "adaptive-shadow",
            "plan",
            "--formal-plan",
            str(tmp_path / "formal.json"),
            "--formal-decision",
            FORMAL_DECISION,
            "--input",
            str(tmp_path / SOURCE_SHA),
        ]
    )

    assert result == exit_code
    assert json.loads(capsys.readouterr().out)["status"] == status
    assert captured["formal_decision_record_sha256"] == FORMAL_DECISION
    assert captured["created_at_utc"] is None
