from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-multi-asset-evidence.py"


def _load_runner() -> ModuleType:
    spec = importlib.util.spec_from_file_location("factor_lab_multi_asset_runner", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _contracts(module: ModuleType) -> tuple[dict, dict, dict]:
    protocol = {
        "payload_sha256": "p" * 64,
        "train_gate": {"net_cagr_strictly_positive": True},
        "validation_gate": {"net_cagr_strictly_positive": True},
        "audit_gate": {"net_cagr_strictly_positive": True},
        "claim_contract": {"profit_claim_allowed": False},
    }
    closure = {"payload_sha256": "c" * 64}
    selection = {"payload_sha256": "s" * 64}
    return closure, protocol, selection


def _phase(module: ModuleType, stage: str, passed: bool) -> dict:
    spec = module.STAGES[stage]
    return {
        "source_manifest_payload_sha256": "a" * 64,
        "stage_binding_payload_sha256": "b" * 64,
        "evaluation_payload_sha256": "e" * 64,
        "evaluation_file_sha256": "f" * 64,
        "metrics": {
            "start_date": spec["performance_start"],
            "end_date": spec["performance_end"],
            "cagr": 0.1 if passed else -0.1,
        },
        "gate": {"passed": passed, "checks": {}},
    }


def _prepare_runner(module: ModuleType, tmp_path: Path, monkeypatch, head: str) -> None:
    work = tmp_path / "runtime"
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "WORK_ROOT", work)
    monkeypatch.setattr(module, "SOURCE_ROOT", work / "sources")
    monkeypatch.setattr(module, "EVALUATION_ROOT", work / "evaluations")
    monkeypatch.setattr(module, "BINDING_ROOT", work / "stage-bindings")
    monkeypatch.setattr(module, "_require_clean_main", lambda: head)
    monkeypatch.setattr(module, "_verify_closure", lambda: _contracts(module))
    monkeypatch.setattr(module, "_require_head_pushed_and_ci_success", lambda _head: None)

    def git(*args: str, **_: object) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return head.encode("ascii") + b"\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)


def test_selection_train_failure_never_opens_validation(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "a" * 40)
    calls: list[str] = []

    def evaluate(stage_name: str, **_: object) -> dict:
        calls.append(stage_name)
        return _phase(module, stage_name, False)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)

    assert module.run_selection() == 0
    assert calls == ["train"]
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    assert freeze["status"] == "selected_null_frozen_train_failed"
    assert freeze["selected_candidate_id"] is None
    assert freeze["validation"] is None
    assert freeze["validation_market_outcomes_opened"] is False
    assert freeze["audit_market_outcomes_opened"] is False


def test_selection_only_opens_validation_after_train_pass(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "b" * 40)
    calls: list[str] = []

    def evaluate(stage_name: str, **_: object) -> dict:
        calls.append(stage_name)
        return _phase(module, stage_name, True)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)

    assert module.run_selection() == 0
    assert calls == ["train", "validation"]
    freeze = json.loads(
        (tmp_path / module.WINNER_FREEZE_PATH).read_text(encoding="utf-8")
    )
    assert freeze["status"] == "selected_candidate_frozen"
    assert freeze["selected_candidate_id"] == module.CANDIDATE_ID
    assert freeze["validation_market_outcomes_opened"] is True


def test_selection_rejects_renamed_stage_injection(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "c" * 40)
    (module.SOURCE_ROOT / "stage=validation.old").mkdir(parents=True)

    with pytest.raises(RuntimeError, match="renamed formal stage"):
        module.run_selection()


def test_disclosed_train_boundary_rejects_any_downstream_open() -> None:
    module = _load_runner()
    valid = {
        "status": "train_falsified_before_preselection_closure",
        "selection": {"validation_opened": False, "audit_opened": False},
        "disclosure": {
            "validation_market_outcomes_opened": False,
            "audit_market_outcomes_opened": False,
        },
    }
    module._verify_disclosed_outcome_boundary(valid)
    for section, key in (
        ("selection", "validation_opened"),
        ("selection", "audit_opened"),
        ("disclosure", "validation_market_outcomes_opened"),
        ("disclosure", "audit_market_outcomes_opened"),
    ):
        forged = json.loads(json.dumps(valid))
        forged[section][key] = True
        with pytest.raises(ValueError, match="forbidden downstream"):
            module._verify_disclosed_outcome_boundary(forged)


def test_disclosed_failed_train_never_opens_validation(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "9" * 40)
    closure, protocol, selection = _contracts(module)
    closure["selection_returns_opened"] = True
    monkeypatch.setattr(module, "_verify_closure", lambda: (closure, protocol, selection))
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(module, "_verify_disclosed_train_replay", lambda *_args: None)
    calls: list[str] = []

    def evaluate(stage_name: str, **_: object) -> dict:
        calls.append(stage_name)
        return _phase(module, stage_name, False)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)
    assert module.run_selection() == 0
    assert calls == ["train"]
    freeze = json.loads(
        (tmp_path / module.WINNER_FREEZE_PATH).read_text(encoding="utf-8")
    )
    assert freeze["selected_candidate_id"] is None
    assert freeze["validation"] is None


def test_protocol_and_asset_selection_are_self_hashed() -> None:
    module = _load_runner()
    protocol = module._read_json(module.PROTOCOL_PATH)
    selection = module._read_json(module.ASSET_SELECTION_PATH)

    assert protocol["direction_change"] is True
    assert len(protocol["candidate_registry"]) == 1
    assert protocol["candidate_registry"][0]["candidate_id"] == module.CANDIDATE_ID
    assert selection["selected_codes"] == [
        "510300.SH",
        "159920.SZ",
        "513100.SH",
        "518880.SH",
        "511010.SH",
        "511880.SH",
    ]
    assert selection["post_cutoff_fund_daily_requested"] is False
    assert selection["fund_div_query_audit"]["unbounded_query_count"] == 0


def test_existing_stage_without_external_binding_is_rejected(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "d" * 40)
    stage_path = module.SOURCE_ROOT / "stage=train"
    stage_path.mkdir(parents=True)
    fake = module.MultiAssetStage(
        path=stage_path,
        manifest={
            "stage": "train",
            "price_start_date": module.STAGES["train"]["source_start"],
            "price_end_date": module.STAGES["train"]["source_end"],
            "payload_sha256": "m" * 64,
        },
        calendar=None,
        assets={},
    )
    monkeypatch.setattr(module, "load_multi_asset_stage", lambda *_: fake)

    with pytest.raises(ValueError, match="lacks a regular external binding"):
        module._stage(
            "train",
            closure_payload="c" * 64,
            execution_commit="d" * 40,
            run_nonce="1" * 32,
            predecessor={"kind": "preselection_closure", "payload_sha256": "c" * 64},
        )


def test_truncated_phase_reference_is_rejected() -> None:
    module = _load_runner()
    phase = _phase(module, "audit", True)
    phase["metrics"]["end_date"] = "2024-12-31"

    with pytest.raises(ValueError, match="truncated"):
        module._verify_phase_reference(
            phase,
            stage_name="audit",
            gate_config={"net_cagr_strictly_positive": True},
            closure_payload="c" * 64,
            execution_commit="d" * 40,
            run_nonce="1" * 32,
            predecessor={"kind": "winner_freeze", "payload_sha256": "w" * 64},
            verify_data=False,
        )


def test_forged_gate_boolean_is_recomputed_from_metrics(monkeypatch) -> None:
    module = _load_runner()
    closure, protocol, selection = _contracts(module)
    train = _phase(module, "train", True)
    validation = _phase(module, "validation", True)
    train["metrics"]["cagr"] = -0.10
    freeze = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_winner_freeze",
        "release": "7.0",
        "status": "selected_candidate_frozen",
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selection_execution_commit": "d" * 40,
        "run_nonce": "1" * 32,
        "candidate_registry": [module.CANDIDATE_ID],
        "selected_candidate_id": module.CANDIDATE_ID,
        "train": train,
        "validation": validation,
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    freeze["payload_sha256"] = module.canonical_payload_sha256(freeze)
    monkeypatch.setattr(module, "_verify_execution_lineage", lambda *args, **kwargs: None)

    with pytest.raises(ValueError, match="differs from metrics"):
        module._verify_winner_freeze_contract(
            freeze,
            closure=closure,
            protocol=protocol,
            selection=selection,
            verify_data=False,
        )


def test_closure_rejects_expected_keys_bound_to_one_file(monkeypatch) -> None:
    module = _load_runner()
    protocol = {
        "protocol_id": "factor-lab/7.0/fixed-multi-asset-trend-budget-v1",
        "release": "7.0",
        "candidate_registry": [{"candidate_id": module.CANDIDATE_ID}],
        "claim_contract": {"profit_claim_allowed": False},
        "preclosure_train_disclosure": {"payload_sha256": "t" * 64},
        "payload_sha256": "p" * 64,
    }
    selection = {
        "selected_codes": [
            "510300.SH", "159920.SZ", "513100.SH", "518880.SH",
            "511010.SH", "511880.SH",
        ],
        "payload_sha256": "s" * 64,
    }
    closure = {
        "schema_version": 1,
        "kind": "factor_lab_release_closure",
        "release": "7.0",
        "closure_role": "post_exposure_failure_replay_root",
        "direction_change": True,
        "route": "fixed_multi_asset_causal_trend_budget",
        "status": "implementation_frozen_after_disclosed_train_failure",
        "selection_returns_opened": True,
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "protocol": {"payload_sha256": "p" * 64, "file_sha256": "h" * 64},
        "asset_selection": {"payload_sha256": "s" * 64, "file_sha256": "h" * 64},
        "prior_release": {},
        "implementation_commit": "d" * 40,
        "implementation_tree": "tree",
        "implementation": {
            key: {"path": "pyproject.toml", "sha256": "h" * 64}
            for key in module.EXPECTED_IMPLEMENTATION_PATHS
        },
        "runtime": {},
        "formal_data": {
            "preclosure_train_disclosure": {
                "path": module.PRECLOSURE_TRAIN_PATH.as_posix(),
                "file_sha256": "h" * 64,
                "payload_sha256": "t" * 64,
                "status": "train_falsified_before_preselection_closure",
                "validation_market_outcomes_opened": False,
                "audit_market_outcomes_opened": False,
            }
        },
        "claim_contract": protocol["claim_contract"],
        "payload_sha256": "z" * 64,
    }
    values = {
        module.CLOSURE_PATH: closure,
        module.PROTOCOL_PATH: protocol,
        module.ASSET_SELECTION_PATH: selection,
        module.PRECLOSURE_TRAIN_PATH: {
            "status": "train_falsified_before_preselection_closure",
            "selection": {"validation_opened": False, "audit_opened": False},
            "disclosure": {
                "validation_market_outcomes_opened": False,
                "audit_market_outcomes_opened": False,
            },
            "payload_sha256": "t" * 64,
        },
    }
    monkeypatch.setattr(module, "_read_json", lambda path, **_: values[path])
    monkeypatch.setattr(module, "file_sha256", lambda _path: "h" * 64)
    monkeypatch.setattr(
        module,
        "_git",
        lambda *args, **kwargs: b"tree\n"
        if args == ("rev-parse", "d" * 40 + "^{tree}")
        else b"",
    )

    with pytest.raises(ValueError, match="invalid implementation binding"):
        module._verify_closure(verify_runtime=False)


def test_clean_main_requires_pushed_successful_head(monkeypatch) -> None:
    module = _load_runner()

    def git(*args: str, **_: object) -> bytes:
        return {
            ("branch", "--show-current"): b"main\n",
            ("status", "--porcelain"): b"",
            ("rev-parse", "HEAD"): b"d" * 40 + b"\n",
        }[args]

    monkeypatch.setattr(module, "_git", git)
    monkeypatch.setattr(
        module,
        "_require_head_pushed_and_ci_success",
        lambda _head: (_ for _ in ()).throw(RuntimeError("CI is not successful")),
    )
    with pytest.raises(RuntimeError, match="CI is not successful"):
        module._require_clean_main()
