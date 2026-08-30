from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

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
    monkeypatch.setattr(module, "_read_json", lambda *_args, **_kwargs: {})

    def verify_disclosed(train: dict, _disclosed: dict) -> None:
        if train["gate"]["passed"] is not False:
            raise ValueError("formal train does not reproduce the disclosed failed boundary")

    monkeypatch.setattr(module, "_verify_disclosed_train_replay", verify_disclosed)

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
        return pd.DataFrame(
            {
                "strategy_id": [strategy_id] * len(protocol_order),
                "signal_date": [pd.Timestamp("2015-02-27")] * len(protocol_order),
                "execution_date": [pd.Timestamp("2015-03-02")] * len(protocol_order),
                "code": protocol_order,
                "target_weight": [weights[code] for code in protocol_order],
                "base_budget": [weights[code] for code in protocol_order],
                "trend_positive_fraction": [1.0] * len(protocol_order),
                "signal_adv20_rmb": [
                    float(index + 1) * 1_000_000.0
                    for index in range(len(protocol_order))
                ],
            }
        )

    causal_by_strategy = {
        module.CANDIDATE_ID: causal_targets(module.CANDIDATE_ID),
        module.CONTROL_ID: causal_targets(module.CONTROL_ID),
    }
    persisted_by_role = {
        "candidate": causal_by_strategy[module.CANDIDATE_ID]
        .sort_values(["signal_date", "code"], kind="mergesort")
        .reset_index(drop=True),
        "control": causal_by_strategy[module.CONTROL_ID]
        .sort_values(["signal_date", "code"], kind="mergesort")
        .reset_index(drop=True),
        "stress": causal_by_strategy[module.CANDIDATE_ID]
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
    monkeypatch.setattr(
        module,
        "combine_phase_metrics",
        lambda *_args: {"combined_metric": 1.0},
    )
    monkeypatch.setattr(
        module,
        "evaluate_phase_gate",
        lambda *_args: {"passed": False},
    )
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
    assert persisted["candidate"]["code"].tolist() == sorted(protocol_order)

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
    persisted["candidate"].loc[0, "target_weight"] += 1e-12

    with pytest.raises(ValueError, match="targets do not match the causal builder"):
        module._replay_evaluation(
            "train",
            stage=stage,
            evaluation=evaluation,
            gate_config={},
        )


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


def test_selection_unexpected_train_pass_never_opens_validation_or_freeze(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    _prepare_runner(module, tmp_path, monkeypatch, "b" * 40)
    calls: list[str] = []

    def evaluate(stage_name: str, **_: object) -> dict:
        calls.append(stage_name)
        return _phase(module, stage_name, True)

    monkeypatch.setattr(module, "_evaluate_stage", evaluate)

    with pytest.raises(ValueError, match="does not reproduce the disclosed failed boundary"):
        module.run_selection()
    assert calls == ["train"]
    assert not (tmp_path / module.WINNER_FREEZE_PATH).exists()
    assert not (module.SOURCE_ROOT / "stage=validation").exists()


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
        "release": "7.1",
        "status": "selected_candidate_frozen",
        "protocol_payload_sha256": protocol["payload_sha256"],
        "asset_selection_payload_sha256": selection["payload_sha256"],
        "implementation_closure_payload_sha256": closure["payload_sha256"],
        "selection_execution_commit": "d" * 40,
        "run_nonce": "1" * 32,
        "candidate_registry": [module.CANDIDATE_ID],
        "selected_candidate_id": module.CANDIDATE_ID,
        "train": train,
        "validation": {"forbidden": True},
        "validation_market_outcomes_opened": True,
        "audit_market_outcomes_opened": False,
        "runner_up_fallback": False,
        "claim_contract": protocol["claim_contract"],
    }
    freeze["payload_sha256"] = module.canonical_payload_sha256(freeze)
    monkeypatch.setattr(module, "_verify_execution_lineage", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_verify_phase_reference", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_verify_disclosed_train_replay", lambda *args: None)

    with pytest.raises(ValueError, match="exact train-failed null"):
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
        "release": "7.1",
        "closure_role": "corrective_train_replay_after_7_0_execution_failure",
        "direction_change": False,
        "route": "fixed_multi_asset_causal_trend_budget",
        "status": "corrective_implementation_frozen_for_exact_failed_train_replay",
        "prior_train_returns_opened": True,
        "corrective_train_returns_opened": False,
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "protocol": {"payload_sha256": "p" * 64, "file_sha256": "h" * 64},
        "asset_selection": {"payload_sha256": "s" * 64, "file_sha256": "h" * 64},
        "corrective_amendment": {
            "payload_sha256": module.AMENDMENT_PAYLOAD,
            "file_sha256": "h" * 64,
        },
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
            },
            "prior_execution_failure": {
                "path": module.PRIOR_FAILURE_PATH.as_posix(),
                "file_sha256": "h" * 64,
                "payload_sha256": module.PRIOR_FAILURE_PAYLOAD,
                "status": "selection_inconclusive_software_failure",
                "classification": "target_order_replay_false_negative",
                "validation_market_outcomes_opened": False,
                "winner_freeze_created": False,
                "audit_market_outcomes_opened": False,
                "terminal_result_created": False,
            },
        },
        "claim_contract": protocol["claim_contract"],
        "payload_sha256": "z" * 64,
    }
    values = {
        module.CLOSURE_PATH: closure,
        module.PROTOCOL_PATH: protocol,
        module.ASSET_SELECTION_PATH: selection,
        module.AMENDMENT_PATH: {"payload_sha256": module.AMENDMENT_PAYLOAD},
        module.PRECLOSURE_TRAIN_PATH: {
            "status": "train_falsified_before_preselection_closure",
            "selection": {"validation_opened": False, "audit_opened": False},
            "disclosure": {
                "validation_market_outcomes_opened": False,
                "audit_market_outcomes_opened": False,
            },
            "payload_sha256": "t" * 64,
        },
        module.PRIOR_CLOSURE_PATH: {"payload_sha256": module.PRIOR_CLOSURE_PAYLOAD},
        module.PRIOR_FAILURE_PATH: {"payload_sha256": module.PRIOR_FAILURE_PAYLOAD},
    }
    monkeypatch.setattr(module, "_read_json", lambda path, **_: values[path])
    monkeypatch.setattr(module, "_verify_corrective_amendment", lambda _value: None)
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


def test_release_state_only_deep_replays_data_when_explicitly_requested(
    tmp_path, monkeypatch
) -> None:
    module = _load_runner()
    closure, protocol, selection = _contracts(module)
    freeze = {"selected_candidate_id": None, "validation": None}
    freeze_path = tmp_path / module.WINNER_FREEZE_PATH
    freeze_path.parent.mkdir(parents=True)
    freeze_path.write_text("{}\n", encoding="utf-8")
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
        ("causal_multi_horizon_trend_budget", None),
        (None, {"forbidden": True}),
    ],
)
def test_finalize_rejects_selected_or_validation_injection(
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
    monkeypatch.setattr(module, "_read_json", lambda path, **_: freeze)
    monkeypatch.setattr(module, "_require_committed", lambda _path: b"{}\n")
    monkeypatch.setattr(module, "_verify_winner_freeze_contract", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="accepts only the committed null freeze"):
        module.run_finalize()
    assert not (tmp_path / module.RESULT_PATH).exists()
