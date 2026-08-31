from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pandas as pd
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
        "shared_absolute_gate": {
            "base": {
                "net_sharpe_at_least": 0.3,
                "daily_max_drawdown_at_least": -0.25,
                "positive_complete_year_ratio_at_least": 0.5,
            },
            "stress_16bp": {
                "net_sharpe_at_least": 0.25,
                "daily_max_drawdown_at_least": -0.25,
                "positive_complete_year_ratio_at_least": 0.5,
            },
            "operational": {
                "annualized_turnover_at_most": 1.0,
                "requested_notional_fill_ratio_at_least": 0.99,
                "capacity_limited_requested_notional_ratio_at_most": 0.01,
                "nav_reconciliation_error_at_most": 1e-8,
            },
        },
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
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: {})

    monkeypatch.setattr(module, "_verify_disclosed_train_replay", lambda *_args: None)

    def git(*args: str, **_: object) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return head.encode("ascii") + b"\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", git)


def _prepare_target_replay_case(
    module: ModuleType, tmp_path: Path, monkeypatch
) -> tuple[object, dict, dict[str, pd.DataFrame], list[str]]:
    protocol_order = [
        "510300.SH",
        "159920.SZ",
        "513100.SH",
        "518880.SH",
        "511010.SH",
        "511880.SH",
    ]
    weights = {
        "510300.SH": 0.30,
        "159920.SZ": 0.10,
        "513100.SH": 0.10,
        "518880.SH": 0.20,
        "511010.SH": 0.30,
        "511880.SH": 0.00,
    }

    def causal_targets(strategy_id: str) -> pd.DataFrame:
        active_weights = (
            {code: (1.0 if code == "511880.SH" else 0.0) for code in protocol_order}
            if strategy_id == module.CASH_ONLY_ID
            else weights
        )
        return pd.DataFrame(
            {
                "strategy_id": [strategy_id] * len(protocol_order),
                "signal_date": [pd.Timestamp("2015-02-27")] * len(protocol_order),
                "execution_date": [pd.Timestamp("2015-03-02")] * len(protocol_order),
                "code": protocol_order,
                "target_weight": [active_weights[code] for code in protocol_order],
                "base_budget": [active_weights[code] for code in protocol_order],
                "trend_positive_fraction": [0.0] * len(protocol_order),
                "signal_adv20_rmb": [
                    float(index + 1) * 1_000_000.0
                    for index in range(len(protocol_order))
                ],
            }
        )

    causal_by_strategy = {
        module.PRIMARY_ID: causal_targets(module.PRIMARY_ID),
        module.CASH_ONLY_ID: causal_targets(module.CASH_ONLY_ID),
    }
    persisted_by_role = {
        "primary": causal_by_strategy[module.PRIMARY_ID]
        .sort_values(["signal_date", "code"], kind="mergesort")
        .reset_index(drop=True),
        "stress": causal_by_strategy[module.PRIMARY_ID]
        .sort_values(["signal_date", "code"], kind="mergesort")
        .reset_index(drop=True),
        "cash": causal_by_strategy[module.CASH_ONLY_ID]
        .sort_values(["signal_date", "code"], kind="mergesort")
        .reset_index(drop=True),
        "cash_stress": causal_by_strategy[module.CASH_ONLY_ID]
        .sort_values(["signal_date", "code"], kind="mergesort")
        .reset_index(drop=True),
    }
    other_artifacts = {
        name: pd.DataFrame({"identity": [name]})
        for name in ("orders", "daily_nav", "holdings", "trades")
    }

    monkeypatch.setattr(module, "EVALUATION_ROOT", tmp_path / "evaluations")
    monkeypatch.setattr(
        module,
        "build_monthly_targets",
        lambda _assets, _sessions, strategy_id: causal_by_strategy[
            strategy_id
        ].copy(),
    )

    def read_parquet(path: Path) -> pd.DataFrame:
        role, artifact_file = Path(path).name.split("-", 1)
        artifact = artifact_file.removesuffix(".parquet")
        if artifact == "targets":
            return persisted_by_role[role].copy()
        return other_artifacts[artifact].copy()

    monkeypatch.setattr(module.pd, "read_parquet", read_parquet)

    def simulate(_assets, targets, _sessions, _config) -> dict[str, pd.DataFrame]:
        return {
            "targets": targets.sort_values(
                ["signal_date", "code"], kind="mergesort"
            ).reset_index(drop=True),
            **{name: frame.copy() for name, frame in other_artifacts.items()},
        }

    monkeypatch.setattr(module, "simulate_targets", simulate)
    monkeypatch.setattr(
        module, "phase_metrics", lambda *_args, **_kwargs: {"metric": 1.0}
    )
    monkeypatch.setattr(module, "_combine_static_metrics", lambda *_args: {"combined_metric": 1.0})
    monkeypatch.setattr(module, "_evaluate_static_gate", lambda *_args: {"passed": False})
    stage = module.MultiAssetStage(
        path=tmp_path / "stage=train",
        manifest={},
        calendar=pd.DataFrame(
            {"trade_date": [pd.Timestamp("2015-02-27"), pd.Timestamp("2015-03-02")]}
        ),
        assets={},
    )
    evaluation = {
        "metrics": {"combined_metric": 1.0},
        "gate": {"passed": False},
    }
    return stage, evaluation, persisted_by_role, protocol_order


def test_replay_accepts_protocol_asset_order_after_stable_key_sort(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    stage, evaluation, persisted, protocol_order = _prepare_target_replay_case(
        module, tmp_path, monkeypatch
    )
    assert protocol_order != sorted(protocol_order)
    assert persisted["primary"]["code"].tolist() == sorted(protocol_order)

    module._replay_evaluation(
        "train",
        stage=stage,
        evaluation=evaluation,
        gate_config={},
    )


def test_replay_still_rejects_exact_target_value_tamper(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    stage, evaluation, persisted, _protocol_order = _prepare_target_replay_case(
        module, tmp_path, monkeypatch
    )
    persisted["primary"].loc[0, "target_weight"] += 1e-12

    with pytest.raises(ValueError, match="targets do not match the causal builder"):
        module._replay_evaluation(
            "train",
            stage=stage,
            evaluation=evaluation,
            gate_config={},
        )


def test_calibration_train_failure_never_opens_validation(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "a" * 40)
    calls: list[str] = []

    def evaluate(stage_name: str, **_: object) -> dict:
        calls.append(stage_name)
        return _phase(module, stage_name, False)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)

    assert module.run_calibration() == 0
    assert calls == ["train"]
    admission = json.loads(
        (tmp_path / module.TRAIN_ADMISSION_PATH).read_text(encoding="utf-8")
    )
    assert admission["status"] == "train_admission_failed"
    assert admission["validation_market_outcomes_opened"] is False
    assert not (tmp_path / module.WINNER_FREEZE_PATH).exists()


def test_committed_train_admission_then_validation_selects_static_policy(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "b" * 40)
    calls: list[str] = []
    execution_identity: dict[str, dict[str, object]] = {}

    def evaluate(stage_name: str, **kwargs: object) -> dict:
        calls.append(stage_name)
        execution_identity[stage_name] = kwargs
        return _phase(module, stage_name, True)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)

    assert module.run_calibration() == 0
    admission_path = tmp_path / module.TRAIN_ADMISSION_PATH
    admission = json.loads(admission_path.read_text(encoding="utf-8"))
    monkeypatch.setattr(
        module,
        "_read_json",
        lambda path, **_: admission if path == module.TRAIN_ADMISSION_PATH else {},
    )
    monkeypatch.setattr(
        module, "_require_committed", lambda _path: admission_path.read_bytes()
    )
    monkeypatch.setattr(
        module, "_verify_train_admission_contract", lambda *args, **kwargs: None
    )
    validation_head = "c" * 40
    monkeypatch.setattr(module, "_require_clean_main", lambda: validation_head)

    def validation_git(*args: str, **_: object) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return validation_head.encode("ascii") + b"\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", validation_git)
    assert module.run_validation() == 0
    assert calls == ["train", "validation"]
    freeze = json.loads(
        (tmp_path / module.WINNER_FREEZE_PATH).read_text(encoding="utf-8")
    )
    assert freeze["status"] == "selected_policy_frozen"
    assert freeze["selected_candidate_id"] == module.PRIMARY_ID
    assert freeze["validation_market_outcomes_opened"] is True
    assert freeze["run_nonce"] != admission["run_nonce"]
    assert freeze["selection_execution_commit"] != admission[
        "calibration_execution_commit"
    ]
    assert execution_identity["train"] == {
        "gate_config": _contracts(module)[1]["shared_absolute_gate"],
        "closure_payload": "c" * 64,
        "execution_commit": "b" * 40,
        "run_nonce": admission["run_nonce"],
        "predecessor": {
            "kind": "preselection_closure",
            "payload_sha256": "c" * 64,
        },
    }
    assert execution_identity["validation"] == {
        "gate_config": _contracts(module)[1]["shared_absolute_gate"],
        "closure_payload": "c" * 64,
        "execution_commit": validation_head,
        "run_nonce": freeze["run_nonce"],
        "predecessor": {
            "kind": "train_admission",
            "payload_sha256": admission["payload_sha256"],
        },
    }


def test_failed_train_admission_validation_creates_null_without_opening_stage(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    calibration_head = "b" * 40
    _prepare_runner(module, tmp_path, monkeypatch, calibration_head)
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda stage_name, **_kwargs: _phase(module, stage_name, False),
    )
    assert module.run_calibration() == 0
    admission_path = tmp_path / module.TRAIN_ADMISSION_PATH
    admission = json.loads(admission_path.read_text(encoding="utf-8"))
    monkeypatch.setattr(
        module,
        "_read_json",
        lambda path, **_: admission if path == module.TRAIN_ADMISSION_PATH else {},
    )
    monkeypatch.setattr(
        module, "_require_committed", lambda _path: admission_path.read_bytes()
    )
    monkeypatch.setattr(
        module, "_verify_train_admission_contract", lambda *args, **kwargs: None
    )
    validation_head = "c" * 40
    monkeypatch.setattr(module, "_require_clean_main", lambda: validation_head)

    def validation_git(*args: str, **_: object) -> bytes:
        if args == ("rev-parse", "HEAD"):
            return validation_head.encode("ascii") + b"\n"
        if args == ("status", "--porcelain"):
            return b""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", validation_git)
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("failed train must not open validation")
        ),
    )

    assert module.run_validation() == 0
    freeze = json.loads(
        (tmp_path / module.WINNER_FREEZE_PATH).read_text(encoding="utf-8")
    )
    assert freeze["status"] == "selected_null_frozen_train_failed"
    assert freeze["selected_candidate_id"] is None
    assert freeze["validation"] is None
    assert freeze["validation_market_outcomes_opened"] is False
    assert not (module.SOURCE_ROOT / "stage=validation").exists()


def test_validation_rejects_calibration_execution_commit_reuse(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    head = "b" * 40
    _prepare_runner(module, tmp_path, monkeypatch, head)
    admission = {
        "calibration_execution_commit": head,
        "run_nonce": "1" * 32,
        "train": {"gate": {"passed": True}},
    }
    admission_path = tmp_path / module.TRAIN_ADMISSION_PATH
    admission_path.parent.mkdir(parents=True)
    admission_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: admission)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(
        module, "_verify_train_admission_contract", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("validation must remain unopened")
        ),
    )

    with pytest.raises(RuntimeError, match="later commit containing train admission"):
        module.run_validation()
    assert not (tmp_path / module.WINNER_FREEZE_PATH).exists()
    assert not (module.SOURCE_ROOT / "stage=validation").exists()


def test_validation_rejects_uncommitted_train_admission(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "7" * 40)
    admission_path = tmp_path / module.TRAIN_ADMISSION_PATH
    admission_path.parent.mkdir(parents=True)
    admission_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "_require_committed",
        lambda _path: (_ for _ in ()).throw(
            RuntimeError("tracked evidence is not the exact HEAD blob")
        ),
    )

    with pytest.raises(RuntimeError, match="not the exact HEAD blob"):
        module.run_validation()
    assert not (tmp_path / module.WINNER_FREEZE_PATH).exists()
    assert not (module.SOURCE_ROOT / "stage=validation").exists()


def test_validation_rejects_wrong_train_admission_contract(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "8" * 40)
    admission_path = tmp_path / module.TRAIN_ADMISSION_PATH
    admission_path.parent.mkdir(parents=True)
    admission = {
        "schema_version": 1,
        "kind": "factor_lab_static_train_admission",
        "release": "8.0",
        "status": "train_admission_passed",
        "payload_sha256": "forged",
    }
    admission_bytes = (json.dumps(admission) + "\n").encode("utf-8")
    admission_path.write_bytes(admission_bytes)
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: admission)
    monkeypatch.setattr(module, "_require_committed", lambda _path: admission_bytes)
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("validation must remain unopened")
        ),
    )

    with pytest.raises(ValueError, match="train admission contract differs"):
        module.run_validation()
    assert not (tmp_path / module.WINNER_FREEZE_PATH).exists()
    assert not (module.SOURCE_ROOT / "stage=validation").exists()


def test_audit_rejects_uncommitted_winner_freeze(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "a" * 40)
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        module,
        "_require_committed",
        lambda _path: (_ for _ in ()).throw(
            RuntimeError("tracked evidence is not the exact HEAD blob")
        ),
    )
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("audit must remain unopened")
        ),
    )

    with pytest.raises(RuntimeError, match="not the exact HEAD blob"):
        module.run_audit()
    assert not (tmp_path / module.AUDIT_PATH).exists()
    assert not (module.SOURCE_ROOT / "stage=audit").exists()


def test_audit_rejects_committed_null_winner_freeze(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "a" * 40)
    closure, protocol, selection = _contracts(module)
    freeze = {
        "selected_candidate_id": None,
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "audit_market_outcomes_opened": False,
    }
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: freeze)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(
        module, "_verify_winner_freeze_contract", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        module,
        "_evaluate_stage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("audit must remain unopened")
        ),
    )

    with pytest.raises(RuntimeError, match="frozen non-null 8.0 policy"):
        module.run_audit()
    assert not (tmp_path / module.AUDIT_PATH).exists()
    assert not (module.SOURCE_ROOT / "stage=audit").exists()


def test_audit_uses_independent_nonce_commit_and_freeze_predecessor(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    audit_head = "e" * 40
    _prepare_runner(module, tmp_path, monkeypatch, audit_head)
    closure, protocol, selection = _contracts(module)
    freeze = {
        "selected_candidate_id": module.PRIMARY_ID,
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selection_execution_commit": "d" * 40,
        "run_nonce": "1" * 32,
        "payload_sha256": "w" * 64,
        "audit_market_outcomes_opened": False,
    }
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: freeze)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(
        module, "_verify_winner_freeze_contract", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        module.uuid, "uuid4", lambda: SimpleNamespace(hex="2" * 32)
    )
    execution_identity: dict[str, object] = {}

    def evaluate(stage_name: str, **kwargs: object) -> dict:
        assert stage_name == "audit"
        execution_identity.update(kwargs)
        return _phase(module, stage_name, True)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)

    assert module.run_audit() == 0
    audit = json.loads((tmp_path / module.AUDIT_PATH).read_text(encoding="utf-8"))
    assert execution_identity == {
        "gate_config": protocol["shared_absolute_gate"],
        "closure_payload": closure["payload_sha256"],
        "execution_commit": audit_head,
        "run_nonce": "2" * 32,
        "predecessor": {
            "kind": "winner_freeze",
            "payload_sha256": freeze["payload_sha256"],
        },
    }
    assert audit["audit_execution_commit"] == audit_head
    assert audit["audit_execution_commit"] != freeze["selection_execution_commit"]
    assert audit["run_nonce"] != freeze["run_nonce"]


def test_audit_contract_rejects_validation_nonce_reuse() -> None:
    module = _load_runner()
    closure, protocol, selection = _contracts(module)
    freeze = {"payload_sha256": "w" * 64, "run_nonce": "1" * 32}
    audit = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_historical_audit",
        "release": "8.0",
        "status": "historical_audit_passed",
        "selected_candidate_id": module.PRIMARY_ID,
        "winner_freeze_payload_sha256": freeze["payload_sha256"],
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "audit_execution_commit": "e" * 40,
        "run_nonce": freeze["run_nonce"],
        "audit": _phase(module, "audit", True),
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    audit["payload_sha256"] = module.canonical_payload_sha256(audit)

    with pytest.raises(ValueError, match="historical audit contract bindings differ"):
        module._verify_audit_contract(
            audit,
            freeze=freeze,
            closure=closure,
            protocol=protocol,
            selection=selection,
            verify_data=False,
        )


def test_selection_rejects_renamed_stage_injection(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "c" * 40)
    (module.SOURCE_ROOT / "stage=validation.old").mkdir(parents=True)

    with pytest.raises(RuntimeError, match="renamed formal stage"):
        module.run_calibration()


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


def test_disclosed_failed_calibration_only_creates_admission(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "9" * 40)
    closure, protocol, selection = _contracts(module)
    monkeypatch.setattr(module, "_verify_closure", lambda: (closure, protocol, selection))
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(module, "_verify_disclosed_train_replay", lambda *_args: None)
    calls: list[str] = []

    def evaluate(stage_name: str, **_: object) -> dict:
        calls.append(stage_name)
        return _phase(module, stage_name, False)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)
    assert module.run_calibration() == 0
    assert calls == ["train"]
    admission = json.loads(
        (tmp_path / module.TRAIN_ADMISSION_PATH).read_text(encoding="utf-8")
    )
    assert admission["status"] == "train_admission_failed"
    assert not (tmp_path / module.WINNER_FREEZE_PATH).exists()


def test_protocol_and_asset_selection_are_self_hashed() -> None:
    module = _load_runner()
    protocol = module._read_json(module.PROTOCOL_PATH)
    selection = module._read_json(module.ASSET_SELECTION_PATH)

    assert protocol["direction_change"] is True
    assert len(protocol["strategy_registry"]) == 1
    assert protocol["strategy_registry"][0]["strategy_id"] == module.PRIMARY_ID
    assert protocol["cash_comparator"]["comparator_id"] == module.CASH_ONLY_ID
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


def test_any_preexisting_stage_is_rejected(tmp_path, monkeypatch) -> None:
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

    with pytest.raises(RuntimeError, match="pre-existing train source stage"):
        module._stage(
            "train",
            closure_payload="c" * 64,
            execution_commit="d" * 40,
            run_nonce="1" * 32,
            predecessor={"kind": "preselection_closure", "payload_sha256": "c" * 64},
        )


def test_truncated_train_phase_reference_is_rejected() -> None:
    module = _load_runner()
    phase = _phase(module, "train", False)
    phase["metrics"]["end_date"] = "2019-11-29"

    with pytest.raises(ValueError, match="truncated"):
        module._verify_phase_reference(
            phase,
            stage_name="train",
            gate_config={"net_cagr_strictly_positive": True},
            closure_payload="c" * 64,
            execution_commit="d" * 40,
            run_nonce="1" * 32,
            predecessor={"kind": "preselection_closure", "payload_sha256": "c" * 64},
            verify_data=False,
        )


def test_winner_freeze_rejects_validation_or_selected_candidate(monkeypatch) -> None:
    module = _load_runner()
    closure, protocol, selection = _contracts(module)
    train = _phase(module, "train", False)
    freeze = {
        "schema_version": 1,
        "kind": "factor_lab_multi_asset_winner_freeze",
        "release": "8.0",
        "status": "selected_policy_frozen",
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selection_execution_commit": "d" * 40,
        "run_nonce": "1" * 32,
        "candidate_registry": [module.PRIMARY_ID],
        "selected_candidate_id": module.PRIMARY_ID,
        "train_admission": {
            "path": module.TRAIN_ADMISSION_PATH.as_posix(),
            "file_sha256": "a" * 64,
            "payload_sha256": "q" * 64,
        },
        "train": train,
        "validation": {"forbidden": True},
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    freeze["payload_sha256"] = module.canonical_payload_sha256(freeze)
    monkeypatch.setattr(module, "_verify_execution_lineage", lambda *args, **kwargs: None)
    admission = {
        "payload_sha256": "q" * 64,
        "run_nonce": "2" * 32,
        "train": train,
    }
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: admission)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"admission")
    freeze["train_admission"]["file_sha256"] = module.hashlib.sha256(
        b"admission"
    ).hexdigest()
    freeze["payload_sha256"] = module.canonical_payload_sha256(freeze)
    monkeypatch.setattr(
        module, "_verify_train_admission_contract", lambda *args, **kwargs: None
    )

    with pytest.raises(ValueError, match="validation opened without a passed train gate"):
        module._verify_winner_freeze_contract(
            freeze,
            closure=closure,
            protocol=protocol,
            selection=selection,
            verify_data=False,
        )


def test_closure_rejects_expected_keys_bound_to_one_file(monkeypatch) -> None:
    module = _load_runner()
    forged = {
        "implementation": {
            key: {"path": "pyproject.toml", "sha256": "h" * 64}
            for key in module.EXPECTED_IMPLEMENTATION_PATHS
        }
    }
    with pytest.raises(ValueError, match="invalid implementation binding"):
        module._verify_implementation_map(forged, "d" * 40)


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


def test_static_gate_uses_all_four_roles_and_strict_protocol_switches() -> None:
    module = _load_runner()
    gate = module._read_json(module.PROTOCOL_PATH)["shared_absolute_gate"]

    def metrics(cagr: float, sharpe: float, drawdown: float) -> dict:
        return {
            "observations": 756,
            "start_date": "2020-01-02",
            "performance_start": "2020-01-02",
            "baseline_date": "2019-12-31",
            "end_date": "2022-12-30",
            "start_nav": 1_000_000.0,
            "cagr": cagr,
            "sharpe": sharpe,
            "max_drawdown": drawdown,
            "positive_complete_year_ratio": 2 / 3,
            "annualized_turnover": 0.5,
            "requested_notional_fill_ratio": 0.999,
            "capacity_limited_requested_notional_ratio": 0.0,
            "nav_reconciliation_error": 1e-10,
        }

    primary = metrics(0.06, 0.6, -0.15)
    stress = metrics(0.055, 0.55, -0.16)
    cash = metrics(0.02, 0.4, -0.01)
    cash_stress = metrics(0.019, 0.39, -0.01)
    combined = module._combine_static_metrics(primary, stress, cash, cash_stress)
    assert module._evaluate_static_gate(combined, gate)["passed"] is True

    losing_cash_hurdle = module._combine_static_metrics(
        metrics(0.01, 0.6, -0.15), stress, cash, cash_stress
    )
    assert (
        module._evaluate_static_gate(losing_cash_hurdle, gate)["checks"][
            "cash_excess_cagr_strictly_positive"
        ]
        is False
    )

    forged_gate = json.loads(json.dumps(gate))
    forged_gate["base"]["net_cagr_strictly_positive"] = False
    with pytest.raises(ValueError, match="must remain enabled"):
        module._evaluate_static_gate(combined, forged_gate)

    mismatched_cash = dict(cash, end_date="2022-12-29")
    with pytest.raises(ValueError, match="one phase identity"):
        module._combine_static_metrics(primary, stress, mismatched_cash, cash_stress)


def test_release_state_only_deep_replays_data_when_explicitly_requested(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    closure, protocol, selection = _contracts(module)
    freeze = {"selected_candidate_id": None, "validation": None}
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
    (tmp_path / module.TRAIN_ADMISSION_PATH).write_text("{}\n", encoding="utf-8")
    calls: list[bool] = []
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "WORK_ROOT", tmp_path / "runtime")
    monkeypatch.setattr(module, "SOURCE_ROOT", module.WORK_ROOT / "sources")
    monkeypatch.setattr(module, "EVALUATION_ROOT", module.WORK_ROOT / "evaluations")
    monkeypatch.setattr(module, "BINDING_ROOT", module.WORK_ROOT / "stage-bindings")
    monkeypatch.setattr(module, "PRIOR_WORK_ROOT", tmp_path / "prior-runtime")
    monkeypatch.setattr(module, "_verify_closure", lambda **_: (closure, protocol, selection))
    monkeypatch.setattr(module, "_read_json", lambda path, **_: freeze)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")

    def verify_freeze(*_args, verify_data: bool, **_kwargs) -> None:
        calls.append(verify_data)

    monkeypatch.setattr(module, "_verify_winner_freeze_contract", verify_freeze)

    state = module.verify_release_state()
    deep_state = module.verify_release_state(verify_data=True)

    assert calls == [False, True]
    assert state["status"] == "selection_frozen_no_candidate_pending_finalize"
    assert deep_state["status"] == "selection_frozen_no_candidate_pending_finalize"


def test_admission_only_failed_status_and_empty_runtime_rejection(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    closure, protocol, selection = _contracts(module)
    admission = {"train": {"gate": {"passed": False}}}
    admission_path = tmp_path / module.TRAIN_ADMISSION_PATH
    admission_path.parent.mkdir(parents=True)
    admission_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "WORK_ROOT", tmp_path / "runtime")
    monkeypatch.setattr(module, "SOURCE_ROOT", module.WORK_ROOT / "sources")
    monkeypatch.setattr(module, "EVALUATION_ROOT", module.WORK_ROOT / "evaluations")
    monkeypatch.setattr(module, "BINDING_ROOT", module.WORK_ROOT / "stage-bindings")
    monkeypatch.setattr(module, "PRIOR_WORK_ROOT", tmp_path / "prior-runtime")
    monkeypatch.setattr(
        module,
        "_verify_closure",
        lambda **_: (closure, protocol, selection),
    )
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: admission)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(
        module, "_verify_train_admission_contract", lambda *args, **kwargs: None
    )

    state = module.verify_release_state()
    assert state["status"] == "train_admission_failed_pending_null_freeze"
    assert state["freeze"] is None

    module.WORK_ROOT.mkdir()
    with pytest.raises(RuntimeError, match="closure-only state requires an absent"):
        module._assert_runtime_layout(set())


def test_release_state_rejects_freeze_without_train_admission(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    closure, protocol, selection = _contracts(module)
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(
        module,
        "_verify_closure",
        lambda **_: (closure, protocol, selection),
    )

    with pytest.raises(ValueError, match="without a train admission"):
        module.verify_release_state()


def test_null_freeze_finalize_and_default_result_state_without_runtime(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "f" * 40)
    closure, protocol, selection = _contracts(module)
    freeze = {
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selected_candidate_id": None,
        "validation": None,
        "validation_market_outcomes_opened": False,
        "payload_sha256": "w" * 64,
    }
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
    (tmp_path / module.TRAIN_ADMISSION_PATH).write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "_read_json", lambda path, **_: freeze)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(module, "_verify_winner_freeze_contract", lambda *args, **kwargs: None)

    assert module.run_finalize() == 0
    result_path = tmp_path / module.RESULT_PATH
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert result["status"] == "selection_falsified_no_candidate"
    assert result["selected_candidate_id"] is None
    assert result["historical_audit"] is None

    monkeypatch.setattr(
        module,
        "_read_json",
        lambda path, **_: result if path == module.RESULT_PATH else freeze,
    )
    monkeypatch.setattr(
        module, "_verify_closure", lambda **_: (closure, protocol, selection)
    )
    monkeypatch.setattr(module, "_verify_result_contract", lambda *args, **kwargs: None)
    state = module.verify_release_state()
    assert state["status"] == "selection_falsified_no_candidate"
    assert state["freeze"] is freeze
    assert state["result"] is result


@pytest.mark.parametrize(
    ("selected", "validation"),
    [
        ("unknown_policy", None),
    ],
)
def test_finalize_rejects_unknown_selected_policy(
    selected, validation, tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "e" * 40)
    closure, protocol, selection = _contracts(module)
    freeze = {
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selected_candidate_id": selected,
        "validation": validation,
        "validation_market_outcomes_opened": validation is not None,
    }
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
    (tmp_path / module.TRAIN_ADMISSION_PATH).write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(module, "_read_json", lambda path, **_: freeze)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(module, "_verify_winner_freeze_contract", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="unknown selected policy"):
        module.run_finalize()
    assert not (tmp_path / module.RESULT_PATH).exists()
