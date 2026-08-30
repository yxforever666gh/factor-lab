from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run-wide-universe-evidence.py"
SPEC = importlib.util.spec_from_file_location("run_wide_universe_evidence", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


@pytest.mark.parametrize(
    ("physical_end", "expected"),
    [
        ("2022-12-31", ("train",)),
        ("2024-12-31", ("train", "validation")),
        ("2026-08-21", ("train", "validation", "audit")),
    ],
)
def test_evaluation_phase_bounds_only_opens_physically_available_phases(
    physical_end: str, expected: tuple[str, ...]
) -> None:
    bounds = RUNNER._evaluation_phase_bounds(physical_end)

    assert tuple(bounds) == expected
    assert all(value.end <= pd.Timestamp(physical_end) for value in bounds.values())


def test_evaluation_phase_bounds_rejects_pre_anchor_stage() -> None:
    with pytest.raises(ValueError, match="precedes the frozen anchor"):
        RUNNER._evaluation_phase_bounds("2017-01-02")


def test_pre_return_protocol_amendment_has_valid_lineage_and_self_hash() -> None:
    protocol = RUNNER._read_json(ROOT / "protocols" / "6.2-wide-universe.json")
    amendment = RUNNER._read_json(
        ROOT / "protocols" / "6.2-wide-universe-amendment-1.json"
    )

    assert protocol["payload_sha256"] == RUNNER._payload_sha256(protocol)
    assert amendment["payload_sha256"] == RUNNER._payload_sha256(amendment)
    assert amendment["wide_return_evaluation_opened_before_freeze"] is False
    assert amendment["protocol_id"] == "factor-lab/6.2/widened-opportunity-set-v2"
    assert amendment["amendment_id"] == (
        "factor-lab/6.2/widened-opportunity-set-v2/amendment-1"
    )
    assert amendment["base_protocol"]["payload_sha256"] == protocol["payload_sha256"]


def test_runner_admission_constants_match_the_frozen_protocol() -> None:
    protocol_path = ROOT / "protocols" / "6.2-wide-universe.json"
    amendment_path = ROOT / "protocols" / "6.2-wide-universe-amendment-1.json"
    corrective_path = ROOT / RUNNER.CORRECTIVE_AMENDMENT
    protocol = RUNNER._read_json(protocol_path)

    RUNNER._verify_runner_protocol_parity(protocol)
    binding = RUNNER._stage_protocol(
        protocol_path,
        amendment_path,
        corrective_path,
        SimpleNamespace(admit=lambda *_args, **_kwargs: None),
    )

    admission = protocol["common_base"]["finite_score_admission"]
    assert binding["protocol_id"] == RUNNER.WIDE_PROTOCOL_ID
    assert binding["amendment_id"] == RUNNER.WIDE_PROTOCOL_AMENDMENT_ID
    assert binding["corrective_amendment_id"] == RUNNER.CORRECTIVE_AMENDMENT_ID
    assert tuple(protocol["candidate_ids"]) == RUNNER.UNIVERSE_IDS
    assert admission == RUNNER.FROZEN_FINITE_SCORE_ADMISSION
    assert admission["coverage_diagnostics"]["role"] == "diagnostic_only"
    assert admission["coverage_diagnostics"]["may_gate_or_select"] is False
    assert admission["per_signal_per_arm"] == {
        "finite_score_count_min": RUNNER.ADMISSION_MIN_FINITE_SCORE_COUNT,
        "top25_complete_required": True,
    }
    assert RUNNER.ADMISSION_TOP_RANKING_COUNT == 25
    assert admission["source_semantics"] == (
        RUNNER.FROZEN_FINITE_SCORE_ADMISSION["source_semantics"]
    )


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("version",), "6.3"),
        (("status",), "draft"),
        (("direction_change",), True),
        (("candidate_ids",), list(reversed(RUNNER.UNIVERSE_IDS))),
        (
            (
                "common_base",
                "finite_score_admission",
                "coverage_diagnostics",
                "role",
            ),
            "gate",
        ),
        (
            (
                "common_base",
                "finite_score_admission",
                "coverage_diagnostics",
                "may_gate_or_select",
            ),
            True,
        ),
        (
            (
                "common_base",
                "finite_score_admission",
                "per_signal_per_arm",
                "finite_score_count_min",
            ),
            24,
        ),
        (
            (
                "common_base",
                "finite_score_admission",
                "per_signal_per_arm",
                "top25_complete_required",
            ),
            False,
        ),
    ],
)
def test_runner_protocol_parity_rejects_identity_or_gate_drift(
    path: tuple[str, ...], replacement: object
) -> None:
    protocol = json.loads(
        (ROOT / "protocols" / "6.2-wide-universe.json").read_text(
            encoding="utf-8"
        )
    )
    cursor = protocol
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = replacement

    with pytest.raises(ValueError, match="protocol|admission"):
        RUNNER._verify_runner_protocol_parity(protocol)


@pytest.mark.parametrize(
    "semantic",
    tuple(RUNNER.FROZEN_FINITE_SCORE_ADMISSION["source_semantics"]),
)
def test_runner_protocol_parity_rejects_each_source_semantic_drift(
    semantic: str,
) -> None:
    protocol = json.loads(
        (ROOT / "protocols" / "6.2-wide-universe.json").read_text(
            encoding="utf-8"
        )
    )
    protocol["common_base"]["finite_score_admission"]["source_semantics"][
        semantic
    ] = "drifted"

    with pytest.raises(ValueError, match="protocol|admission"):
        RUNNER._verify_runner_protocol_parity(protocol)


@pytest.mark.parametrize(
    ("constant", "replacement"),
    [
        ("ADMISSION_MIN_FINITE_SCORE_COUNT", 24),
        ("ADMISSION_TOP_RANKING_COUNT", 24),
    ],
)
def test_runner_protocol_parity_rejects_executable_constant_drift(
    monkeypatch: pytest.MonkeyPatch, constant: str, replacement: int
) -> None:
    protocol = RUNNER._read_json(ROOT / "protocols" / "6.2-wide-universe.json")
    monkeypatch.setattr(RUNNER, constant, replacement)

    with pytest.raises(ValueError, match="runner finite-score admission differs"):
        RUNNER._verify_runner_protocol_parity(protocol)


def test_selected_definition_requires_exact_frozen_contract() -> None:
    candidate_id = "daily_adv20_top1500"
    expected = RUNNER._selected_definition(candidate_id)

    assert RUNNER._selected_definition_matches(expected, candidate_id)
    for key in tuple(expected):
        tampered = dict(expected)
        tampered[key] = "tampered"
        assert not RUNNER._selected_definition_matches(tampered, candidate_id)
    with_extra = {**expected, "unregistered_override": True}
    assert not RUNNER._selected_definition_matches(with_extra, candidate_id)


def test_phase_trace_hashes_bind_period_trade_and_daily_nav_rows() -> None:
    result = SimpleNamespace(
        periods=[
            {
                "signal_date": "2022-12-20",
                "start_date": "2022-12-21",
                "end_date": "2022-12-30",
                "account_nav_path_start_sequence": 0,
                "account_nav_path_end_sequence": 1,
                "opaque_exact_field": "train",
            },
            {
                "signal_date": "2023-01-03",
                "start_date": "2023-01-04",
                "end_date": "2023-01-17",
                "account_nav_path_start_sequence": 1,
                "account_nav_path_end_sequence": 2,
                "opaque_exact_field": "validation",
            },
        ],
        trades=[
            {"date": "2022-12-21", "ticker": "A", "notional": 1.0},
            {"date": "2023-01-04", "ticker": "B", "notional": 2.0},
        ],
        account_nav_path=[
            {"sequence": 0, "nav": 100.0},
            {"sequence": 1, "nav": 101.0},
            {"sequence": 2, "nav": 102.0},
        ],
    )
    bounds = RUNNER.PhaseBounds.from_values("2017-01-03", "2022-12-31")
    baseline = RUNNER._phase_trace_sha256(result, bounds)

    result.trades[0]["notional"] = 1.01
    assert RUNNER._phase_trace_sha256(result, bounds) != baseline
    result.trades[0]["notional"] = 1.0
    result.periods[0]["opaque_exact_field"] = "tampered"
    assert RUNNER._phase_trace_sha256(result, bounds) != baseline
    result.periods[0]["opaque_exact_field"] = "train"
    result.account_nav_path[0]["nav"] = 99.0
    assert RUNNER._phase_trace_sha256(result, bounds) != baseline


def test_audit_contract_freezes_one_cutoff_and_create_only_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit_path = tmp_path / "historical-audit.json"
    monkeypatch.setattr(RUNNER, "AUDIT_EVIDENCE_PATH", str(audit_path))
    args = RUNNER._parse_args(
        [
            "--mode",
            "audit",
            "--freeze",
            str(ROOT / RUNNER.WINNER_FREEZE),
            "--audit-end",
            RUNNER.AUDIT_END.date().isoformat(),
        ]
    )
    audit_path.write_text("{}", encoding="utf-8")

    with pytest.raises(FileExistsError, match="create-only"):
        RUNNER.run_audit(args)


def test_mode_defaults_physically_separate_selection_and_audit_status() -> None:
    selection = RUNNER._parse_args(["--mode", "selection"])
    audit = RUNNER._parse_args(
        [
            "--mode",
            "audit",
            "--freeze",
            str(ROOT / RUNNER.WINNER_FREEZE),
            "--audit-end",
            "2026-08-21",
        ]
    )

    assert selection.suspensions != audit.suspensions
    assert selection.suspension_metadata != audit.suspension_metadata
    assert selection.stock_st_checkpoint != audit.stock_st_checkpoint
    assert selection.train_suspensions != selection.suspensions
    assert selection.train_suspension_metadata != selection.suspension_metadata
    assert selection.train_stock_st_checkpoint != selection.stock_st_checkpoint
    assert "train" in selection.train_stock_st_checkpoint.parts
    assert "selection" in selection.suspensions.parts
    assert "audit" in audit.suspensions.parts
    assert selection.work_root == ROOT / RUNNER.WORK_ROOT
    assert audit.work_root == ROOT / RUNNER.WORK_ROOT
    assert selection.work_root != ROOT / RUNNER.LEGACY_6_2_WORK_ROOT
    assert selection.corrective_amendment == ROOT / RUNNER.CORRECTIVE_AMENDMENT
    assert audit.corrective_amendment == ROOT / RUNNER.CORRECTIVE_AMENDMENT
    assert all(
        ROOT / RUNNER.WORK_ROOT in path.parents
        for path in (
            selection.train_suspensions,
            selection.train_suspension_metadata,
            selection.train_stock_st_checkpoint,
            selection.suspensions,
            selection.suspension_metadata,
            selection.stock_st_checkpoint,
            audit.suspensions,
            audit.suspension_metadata,
            audit.stock_st_checkpoint,
        )
    )


def test_modes_reject_the_6_2_work_root() -> None:
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "selection",
                "--work-root",
                str(ROOT / RUNNER.LEGACY_6_2_WORK_ROOT),
            ]
        )


def test_selection_rejects_a_6_2_status_artifact() -> None:
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "selection",
                "--train-stock-st-checkpoint",
                str(
                    ROOT
                    / RUNNER.LEGACY_6_2_WORK_ROOT
                    / "train/stock-st-isolated-checkpoint.json"
                ),
            ]
        )


@pytest.mark.parametrize(
    ("mode", "attribute"),
    (("selection", "train_suspensions"), ("audit", "suspensions")),
)
def test_callable_formal_boundary_rejects_a_renamed_status_artifact(
    tmp_path: Path, mode: str, attribute: str
) -> None:
    argv = ["--mode", mode]
    if mode == "audit":
        argv.extend(
            [
                "--freeze",
                str(ROOT / RUNNER.WINNER_FREEZE),
                "--audit-end",
                RUNNER.AUDIT_END.date().isoformat(),
            ]
        )
    args = RUNNER._parse_args(argv)
    setattr(args, attribute, tmp_path / Path(getattr(args, attribute)).name)

    with pytest.raises(ValueError, match="must use the frozen path"):
        RUNNER._require_formal_6_3_paths(args, mode=mode)


def test_audit_mode_rejects_selection_status_paths() -> None:
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "audit",
                "--freeze",
                str(ROOT / RUNNER.WINNER_FREEZE),
                "--audit-end",
                "2026-08-21",
                "--stock-st-checkpoint",
                str(ROOT / RUNNER.SELECTION_ST_CHECKPOINT),
            ]
        )


def test_selection_mode_rejects_shared_train_and_validation_status_path() -> None:
    shared = str(ROOT / "shared-stock-st.json")
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "selection",
                "--train-stock-st-checkpoint",
                shared,
                "--stock-st-checkpoint",
                shared,
            ]
        )


def test_audit_mode_rejects_default_train_status_path() -> None:
    with pytest.raises(SystemExit):
        RUNNER._parse_args(
            [
                "--mode",
                "audit",
                "--freeze",
                str(ROOT / RUNNER.WINNER_FREEZE),
                "--audit-end",
                "2026-08-21",
                "--stock-st-checkpoint",
                str(ROOT / RUNNER.TRAIN_ST_CHECKPOINT),
            ]
        )


def test_scope_filter_ignores_only_bj_and_rejects_unknown_sh_sz() -> None:
    date = pd.Timestamp("2024-01-02")
    frame = pd.DataFrame(
        {"ts_code": ["000001.SZ", "430001.BJ"], "value": [1, 2]}
    )

    selected, ignored = RUNNER._restrict_partition_to_security_scope(
        frame,
        identifier="ts_code",
        allowed_tickers={"000001.SZ"},
        role="stock_st",
        date=date,
    )

    assert selected["ts_code"].tolist() == ["000001.SZ"]
    assert ignored == 1
    with pytest.raises(ValueError, match="absent from the security master"):
        RUNNER._restrict_partition_to_security_scope(
            pd.DataFrame({"ts_code": ["999999.SH"]}),
            identifier="ts_code",
            allowed_tickers={"000001.SZ"},
            role="stock_st",
            date=date,
        )


def _complete_rankings(
    *, candidate_ids: tuple[str, ...] = RUNNER.UNIVERSE_IDS, row_count: int = 25
) -> pd.DataFrame:
    rows = []
    for candidate_index, candidate_id in enumerate(candidate_ids):
        for rank in range(1, row_count + 1):
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "date": pd.Timestamp("2017-01-03"),
                    "ticker": f"{candidate_index}{rank:05d}.SZ",
                    "rank": rank,
                    "score": float(row_count - rank + 1),
                }
            )
    return pd.DataFrame(rows)


def _admission_diagnostics(
    *,
    candidate_ids: tuple[str, ...] = RUNNER.UNIVERSE_IDS,
    member_count: int = 500,
    finite_score_count: int = 400,
    top25_row_count: int = 25,
    pe_ttm_null_count: int = 100,
    pb_null_count: int = 0,
    daily_basic_row_absent_with_daily_bar_count: int = 0,
    daily_basic_row_absent_with_proven_no_daily_bar_count: int = 0,
    invalid_non_null_fundamental_count: int = 0,
    expected_finite_score_count: int | None = None,
    unexpected_score_mismatch_count: int = 0,
    arithmetic_nonfinite_count: int = 0,
    classified_unscoreable_count: int | None = None,
    unclassified_unscoreable_count: int = 0,
) -> pd.DataFrame:
    expected = (
        finite_score_count
        if expected_finite_score_count is None
        else expected_finite_score_count
    )
    classified = (
        member_count - expected
        if classified_unscoreable_count is None
        else classified_unscoreable_count
    )
    return pd.DataFrame(
        [
            {
                "candidate_id": candidate_id,
                "date": pd.Timestamp("2017-01-03"),
                "member_count": member_count,
                "finite_score_count": finite_score_count,
                "finite_score_coverage": finite_score_count / member_count,
                "top25_row_count": top25_row_count,
                "daily_basic_row_absent_with_daily_bar_count": (
                    daily_basic_row_absent_with_daily_bar_count
                ),
                "daily_basic_row_absent_with_proven_no_daily_bar_count": (
                    daily_basic_row_absent_with_proven_no_daily_bar_count
                ),
                "pe_ttm_null_count": pe_ttm_null_count,
                "pb_null_count": pb_null_count,
                "invalid_non_null_fundamental_count": (
                    invalid_non_null_fundamental_count
                ),
                "expected_finite_score_count": expected,
                "unexpected_score_mismatch_count": unexpected_score_mismatch_count,
                "arithmetic_nonfinite_count": arithmetic_nonfinite_count,
                "classified_unscoreable_count": classified,
                "unclassified_unscoreable_count": unclassified_unscoreable_count,
            }
            for candidate_id in candidate_ids
        ]
    )


def _write_manifest(paths: dict[str, Path], manifest: dict[str, object]) -> None:
    manifest["payload_sha256"] = RUNNER._payload_sha256(manifest)
    paths["manifest"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _fixture_stage_lineage() -> dict[str, object]:
    protocol = RUNNER._read_json(ROOT / RUNNER.BASE_PROTOCOL_PATH)
    amendment = RUNNER._read_json(ROOT / RUNNER.BASE_PROTOCOL_AMENDMENT_PATH)
    corrective = RUNNER._read_json(ROOT / RUNNER.CORRECTIVE_AMENDMENT_PATH)
    bound_paths = (
        RUNNER.BASE_PROTOCOL_PATH,
        RUNNER.BASE_PROTOCOL_AMENDMENT_PATH,
        RUNNER.CORRECTIVE_AMENDMENT_PATH,
        RUNNER.FROZEN_IMPLEMENTATION_PATHS["wide_runner"],
        RUNNER.FROZEN_IMPLEMENTATION_PATHS["opportunity_set"],
    )
    source_bindings = {
        relative_path: RUNNER.sha256_file(ROOT / relative_path)
        for relative_path in bound_paths
    }
    source_bindings[RUNNER.PRESELECTION_CLOSURE_PATH] = "4" * 64
    return {
        "protocol_id": protocol["protocol_id"],
        "protocol_payload_sha256": protocol["payload_sha256"],
        "protocol_amendment_id": amendment["amendment_id"],
        "protocol_amendment_payload_sha256": amendment["payload_sha256"],
        "corrective_amendment_id": corrective["amendment_id"],
        "corrective_amendment_payload_sha256": corrective["payload_sha256"],
        "preselection_closure_payload_sha256": "3" * 64,
        "git_commit": "a" * 40,
        "source_bindings": source_bindings,
    }


def _write_valid_stage_fixture(
    tmp_path: Path,
    *,
    rankings: pd.DataFrame | None = None,
    diagnostics: pd.DataFrame | None = None,
) -> tuple[dict[str, Path], dict[str, object]]:
    paths = RUNNER._stage_paths(tmp_path, "train")
    ranking_frame = _complete_rankings() if rankings is None else rankings
    diagnostic_frame = (
        _admission_diagnostics() if diagnostics is None else diagnostics
    )
    ranking_frame.to_parquet(paths["rankings"], index=False)
    diagnostic_frame.to_parquet(paths["admission_diagnostics"], index=False)
    lineage = _fixture_stage_lineage()
    source_hashes = dict(lineage["source_bindings"])
    for relative_path in (
        RUNNER.TRAIN_SUSPENSIONS,
        RUNNER.TRAIN_SUSPENSION_METADATA,
        RUNNER.TRAIN_ST_CHECKPOINT,
    ):
        source_hashes.setdefault(relative_path, "5" * 64)
    source_files = [
        {"path": path, "size_bytes": 0, "sha256": sha256}
        for path, sha256 in sorted(source_hashes.items())
    ]
    source_payload = {
        "file_count": len(source_files),
        "files": source_files,
        "payload_sha256": RUNNER.canonical_sha256(source_files),
    }
    paths["source_files"].write_text(
        json.dumps(source_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    for name in RUNNER.STAGE_ARTIFACT_NAMES:
        path = paths[name]
        if path.exists():
            continue
        path.write_bytes(f"fixture:{name}\n".encode("utf-8"))
    manifest: dict[str, object] = {
        "schema_version": 1,
        "kind": "factor_lab_wide_universe_stage_manifest",
        "release": "6.3",
        "stage": "train",
        "status": "data_admission_passed",
        **{
            field: lineage[field]
            for field in RUNNER.STAGE_LINEAGE_FIELDS
        },
        "candidate_ids": list(RUNNER.UNIVERSE_IDS),
        "physical_max_date": "2022-12-31",
        "ranking_row_count": len(ranking_frame),
        "admission_diagnostic_row_count": len(diagnostic_frame),
        "signal_session_count": int(ranking_frame["date"].nunique()),
        "score_data_admission": RUNNER._score_data_admission_contract(),
        "finite_score_coverage": RUNNER._finite_score_coverage_diagnostics(
            diagnostic_frame
        ),
        "source_file_count": source_payload["file_count"],
        "source_file_payload_sha256": source_payload["payload_sha256"],
        "artifacts": {
            name: RUNNER._artifact(paths[name])
            for name in RUNNER.STAGE_ARTIFACT_NAMES
        },
    }
    _write_manifest(paths, manifest)
    return paths, manifest


def _forbid_return_work(
    monkeypatch: pytest.MonkeyPatch, paths: dict[str, Path]
) -> dict[str, bool]:
    opened = {"decisions": False, "pricing": False, "portfolio": False}
    original_read_json = RUNNER._read_json
    original_read_parquet = RUNNER.pd.read_parquet

    def guarded_read_json(path: Path) -> dict[str, object]:
        if Path(path).resolve() == paths["decisions"].resolve():
            opened["decisions"] = True
            raise AssertionError("decisions opened before admission replay")
        return original_read_json(path)

    def guarded_read_parquet(
        path: Path, *args: object, **kwargs: object
    ) -> pd.DataFrame:
        if Path(path).resolve() == paths["pricing"].resolve():
            opened["pricing"] = True
            raise AssertionError("pricing opened before admission replay")
        return original_read_parquet(path, *args, **kwargs)

    def forbidden_portfolio(*_args: object, **_kwargs: object) -> None:
        opened["portfolio"] = True
        raise AssertionError("portfolio evaluation opened before admission replay")

    monkeypatch.setattr(RUNNER, "_read_json", guarded_read_json)
    monkeypatch.setattr(RUNNER.pd, "read_parquet", guarded_read_parquet)
    monkeypatch.setattr(RUNNER, "evaluate_long_only_portfolio", forbidden_portfolio)
    return opened


def _evaluate_train_fixture(
    tmp_path: Path, expected_manifest_payload_sha256: str
) -> None:
    RUNNER.evaluate_stage(
        stage="train",
        candidates=RUNNER.UNIVERSE_IDS,
        work_root=tmp_path,
        research_config_path=(
            ROOT / RUNNER.FROZEN_IMPLEMENTATION_PATHS["research_config"]
        ),
        expected_manifest_payload_sha256=expected_manifest_payload_sha256,
        expected_lineage=_fixture_stage_lineage(),
    )


def test_provider_pe_null_coverage_is_diagnostic_only() -> None:
    rankings = _complete_rankings()
    diagnostics = _admission_diagnostics(
        member_count=500,
        finite_score_count=400,
        pe_ttm_null_count=100,
    )

    admitted = RUNNER._audit_admission_diagnostics(
        diagnostics,
        rankings,
        expected_universes=RUNNER.UNIVERSE_IDS,
    )

    assert admitted["finite_score_coverage"].eq(0.8).all()
    assert admitted["pe_ttm_null_count"].eq(100).all()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("daily_basic_row_absent_with_daily_bar_count", 1),
        ("invalid_non_null_fundamental_count", 1),
        ("unexpected_score_mismatch_count", 1),
        ("arithmetic_nonfinite_count", 1),
        ("unclassified_unscoreable_count", 1),
    ],
)
def test_hard_score_data_anomaly_blocks_before_pricing_or_evaluation(
    tmp_path: Path, field: str, value: int
) -> None:
    paths = RUNNER._stage_paths(tmp_path, "train")
    diagnostics = _admission_diagnostics()
    diagnostics.loc[0, field] = value

    with pytest.raises(ValueError, match="structural score data admission failed"):
        RUNNER._audit_admission_diagnostics(
            diagnostics,
            _complete_rankings(),
            expected_universes=RUNNER.UNIVERSE_IDS,
        )

    assert not paths["pricing"].exists()
    assert not paths["evaluation"].exists()


def test_build_stage_hard_admission_failure_precedes_all_downstream_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeLedger:
        def admit(self, *_args: object, **_kwargs: object) -> None:
            return None

        def verify_unchanged(self) -> None:
            return None

        def payload(self) -> dict[str, object]:
            return {"file_count": 0, "files": [], "payload_sha256": "0" * 64}

    class FakeUniverse:
        def __init__(self, name: str, *, invalid_count: int) -> None:
            self.name = name
            self.member_count = 500
            self.finite_score_count = 400
            self.daily_basic_row_absent_with_daily_bar_count = 0
            self.daily_basic_row_absent_with_proven_no_daily_bar_count = 0
            self.pe_ttm_null_count = 100
            self.pb_null_count = 0
            self.invalid_non_null_fundamental_count = invalid_count
            self.expected_finite_score_count = 400
            self.unexpected_score_mismatch_count = 0
            self.arithmetic_nonfinite_count = 0
            self.classified_unscoreable_count = 100
            self.unclassified_unscoreable_count = 0
            self.top25 = tuple(range(25))

        @property
        def finite_score_coverage(self) -> float:
            return self.finite_score_count / self.member_count

        def to_frame(self) -> pd.DataFrame:
            candidate_index = RUNNER.UNIVERSE_IDS.index(self.name)
            return pd.DataFrame(
                {
                    "rank": range(1, 26),
                    "ticker": [
                        f"{candidate_index}{rank:05d}.SZ" for rank in range(1, 26)
                    ],
                    "fixed_core_score": [float(26 - rank) for rank in range(1, 26)],
                    "adv20_rmb": [1_000_000_000.0] * 25,
                    "volatility_20": [0.02] * 25,
                }
            )

    universes = tuple(
        FakeUniverse(candidate_id, invalid_count=1 if index == 0 else 0)
        for index, candidate_id in enumerate(RUNNER.UNIVERSE_IDS)
    )
    opportunity_result = SimpleNamespace(
        signal_date="2017-01-03",
        universes=universes,
        base_eligible_count=500,
        history_ready=True,
        carried_suspension_evidence=(),
        inactive_stock_st_ignored_count=0,
    )

    class FakeOpportunityBuilder:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        def push_day(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
            return opportunity_result

    downstream = {"decisions": False, "pricing": False}

    def forbidden_decisions(*_args: object, **_kwargs: object) -> None:
        downstream["decisions"] = True
        raise AssertionError("decisions opened after failed admission")

    class ForbiddenPricingBuilder:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            downstream["pricing"] = True
            raise AssertionError("pricing opened after failed admission")

    monkeypatch.setattr(RUNNER, "SourceLedger", FakeLedger)
    monkeypatch.setattr(
        RUNNER,
        "_runtime_layout",
        lambda _path: (
            {"enrichment": {"security_code_aliases": []}},
            SimpleNamespace(checkpoint_path=tmp_path / "checkpoint.json"),
        ),
    )
    monkeypatch.setattr(
        RUNNER,
        "_stage_protocol",
        lambda *_args, **_kwargs: {
            "protocol_id": RUNNER.WIDE_PROTOCOL_ID,
            "base_payload_sha256": "1" * 64,
            "amendment_id": RUNNER.WIDE_PROTOCOL_AMENDMENT_ID,
            "amendment_payload_sha256": "2" * 64,
            "corrective_amendment_id": RUNNER.CORRECTIVE_AMENDMENT_ID,
            "corrective_amendment_payload_sha256": "3" * 64,
        },
    )
    monkeypatch.setattr(RUNNER, "_checkpoint", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        RUNNER,
        "_load_official_calendar",
        lambda *_args, **_kwargs: (
            (pd.Timestamp("2017-01-03"),),
            [],
            {"exchange": "SSE"},
        ),
    )
    monkeypatch.setattr(
        RUNNER,
        "_stock_st_cutoff_view",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        RUNNER,
        "audit_security_master",
        lambda _layout: {
            "status": "pass",
            "snapshot_path": str(tmp_path / "master"),
            "checkpoint_path": str(tmp_path / "master-checkpoint.json"),
        },
    )
    monkeypatch.setattr(
        RUNNER,
        "load_security_master",
        lambda _layout: pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "exchange": ["SZSE"],
                "curr_type": ["CNY"],
                "list_date": ["19910101"],
                "delist_date": [None],
            }
        ),
    )
    monkeypatch.setattr(
        RUNNER,
        "_load_suspensions",
        lambda *_args, **_kwargs: pd.DataFrame(
            {"date": pd.Series(dtype="datetime64[ns]")}
        ),
    )
    monkeypatch.setattr(
        RUNNER,
        "_read_partition",
        lambda _checkpoint, _dataset, _date, columns, _ledger: pd.DataFrame(
            columns=columns
        ),
    )
    monkeypatch.setattr(RUNNER, "DailyOpportunitySetBuilder", FakeOpportunityBuilder)
    monkeypatch.setattr(RUNNER, "build_target_decisions", forbidden_decisions)
    monkeypatch.setattr(RUNNER, "SparsePricingBuilder", ForbiddenPricingBuilder)

    work_root = tmp_path / "wide-universe"
    with pytest.raises(ValueError, match="structural score data admission failed"):
        RUNNER.build_stage(
            stage="train",
            candidates=RUNNER.UNIVERSE_IDS,
            end_date=RUNNER.TRAIN_END,
            config_path=ROOT / RUNNER.FROZEN_IMPLEMENTATION_PATHS["data_config"],
            research_config_path=(
                ROOT / RUNNER.FROZEN_IMPLEMENTATION_PATHS["research_config"]
            ),
            protocol_path=ROOT / "protocols" / "6.2-wide-universe.json",
            protocol_amendment_path=(
                ROOT / "protocols" / "6.2-wide-universe-amendment-1.json"
            ),
            corrective_amendment_path=ROOT / RUNNER.CORRECTIVE_AMENDMENT,
            release_closure_path=ROOT / RUNNER.PRESELECTION_CLOSURE_PATH,
            work_root=work_root,
            suspension_path=tmp_path / "suspensions.parquet",
            suspension_metadata_path=tmp_path / "suspensions.meta.json",
            stock_st_checkpoint_path=tmp_path / "stock-st.json",
        )

    paths = RUNNER._stage_paths(work_root, "train")
    assert downstream == {"decisions": False, "pricing": False}
    assert not paths["pricing"].exists()
    assert not paths["evaluation"].exists()


def test_selection_never_evaluates_a_stage_that_failed_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    freeze_path = tmp_path / "winner-freeze.json"
    monkeypatch.setattr(RUNNER, "WINNER_FREEZE", str(freeze_path))
    monkeypatch.setattr(
        RUNNER,
        "_git_text",
        lambda command, *_args: "" if command == "status" else "a" * 40,
    )
    monkeypatch.setattr(
        RUNNER, "_require_formal_6_3_paths", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        RUNNER,
        "_verify_release_closure",
        lambda *_args: {"payload_sha256": "3" * 64},
    )
    monkeypatch.setattr(RUNNER, "_current_stage_lineage", _fixture_stage_lineage)
    monkeypatch.setattr(RUNNER, "verify_active_runtime", lambda _root: {})
    monkeypatch.setattr(
        RUNNER,
        "build_stage",
        lambda **_kwargs: (_ for _ in ()).throw(
            ValueError("structural score data admission failed")
        ),
    )
    evaluated = {"value": False}

    def forbidden_evaluation(**_kwargs: object) -> None:
        evaluated["value"] = True
        raise AssertionError("return evaluation opened after failed admission")

    monkeypatch.setattr(RUNNER, "evaluate_stage", forbidden_evaluation)
    args = SimpleNamespace(
        freeze_output=freeze_path,
        release_closure=tmp_path / "release.json",
        protocol=tmp_path / "protocol.json",
        protocol_amendment=tmp_path / "amendment.json",
        corrective_amendment=tmp_path / "corrective-amendment.json",
        config=ROOT / RUNNER.FROZEN_IMPLEMENTATION_PATHS["data_config"],
        research_config=ROOT / RUNNER.FROZEN_IMPLEMENTATION_PATHS["research_config"],
        work_root=tmp_path / "wide-universe",
        train_suspensions=tmp_path / "train-suspensions.parquet",
        train_suspension_metadata=tmp_path / "train-suspensions.meta.json",
        train_stock_st_checkpoint=tmp_path / "train-stock-st.json",
        suspensions=tmp_path / "validation-suspensions.parquet",
        suspension_metadata=tmp_path / "validation-suspensions.meta.json",
        stock_st_checkpoint=tmp_path / "validation-stock-st.json",
    )

    with pytest.raises(ValueError, match="structural score data admission failed"):
        RUNNER.run_selection(args)

    assert evaluated["value"] is False
    assert not freeze_path.exists()


def test_fewer_than_twenty_five_finite_scores_fails_admission() -> None:
    diagnostics = _admission_diagnostics(
        member_count=500,
        finite_score_count=24,
        pe_ttm_null_count=476,
    )

    with pytest.raises(ValueError, match="structural score data admission failed"):
        RUNNER._audit_admission_diagnostics(
            diagnostics,
            _complete_rankings(),
            expected_universes=RUNNER.UNIVERSE_IDS,
        )


def test_every_universe_member_must_have_a_scoreability_classification() -> None:
    diagnostics = _admission_diagnostics(classified_unscoreable_count=99)

    with pytest.raises(ValueError, match="classify every universe member"):
        RUNNER._audit_admission_diagnostics(
            diagnostics,
            _complete_rankings(),
            expected_universes=RUNNER.UNIVERSE_IDS,
        )


def test_incomplete_top25_fails_admission() -> None:
    with pytest.raises(ValueError, match="structural score data admission failed"):
        RUNNER._audit_admission_diagnostics(
            _admission_diagnostics(top25_row_count=24),
            _complete_rankings(row_count=24),
            expected_universes=RUNNER.UNIVERSE_IDS,
        )


def test_admission_diagnostics_artifact_is_manifest_bound(tmp_path: Path) -> None:
    paths, _manifest = _write_valid_stage_fixture(tmp_path)
    assert "admission_diagnostics" in RUNNER.STAGE_ARTIFACT_NAMES

    _, loaded = RUNNER._load_stage(
        tmp_path, "train", expected_lineage=_fixture_stage_lineage()
    )
    assert loaded["artifacts"]["admission_diagnostics"]["sha256"] == (
        RUNNER.sha256_file(paths["admission_diagnostics"])
    )

    tampered = pd.read_parquet(paths["admission_diagnostics"])
    tampered.loc[0, "pe_ttm_null_count"] += 1
    tampered.to_parquet(paths["admission_diagnostics"], index=False)
    with pytest.raises(ValueError, match="artifact identity failed"):
        RUNNER._load_stage(
            tmp_path, "train", expected_lineage=_fixture_stage_lineage()
        )


def test_formal_loader_rejects_the_legacy_6_2_stage_root() -> None:
    with pytest.raises(ValueError, match="canonical 6.3 work root"):
        RUNNER._load_stage(ROOT / RUNNER.LEGACY_6_2_WORK_ROOT, "train")


@pytest.mark.parametrize(
    "field",
    (
        "protocol_payload_sha256",
        "protocol_amendment_payload_sha256",
        "corrective_amendment_payload_sha256",
        "preselection_closure_payload_sha256",
        "git_commit",
    ),
)
def test_stage_manifest_requires_complete_6_3_lineage(
    tmp_path: Path, field: str
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    manifest[field] = "f" * (40 if field == "git_commit" else 64)
    _write_manifest(paths, manifest)

    with pytest.raises(ValueError, match=field):
        RUNNER._load_stage(
            tmp_path, "train", expected_lineage=_fixture_stage_lineage()
        )


def test_stage_source_ledger_must_bind_the_corrective_implementation(
    tmp_path: Path,
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    source_payload = RUNNER._read_json(paths["source_files"])
    source_payload["files"] = [
        row
        for row in source_payload["files"]
        if row["path"] != RUNNER.CORRECTIVE_AMENDMENT_PATH
    ]
    source_payload["file_count"] = len(source_payload["files"])
    source_payload["payload_sha256"] = RUNNER.canonical_sha256(
        source_payload["files"]
    )
    paths["source_files"].write_text(
        json.dumps(source_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest["source_file_count"] = source_payload["file_count"]
    manifest["source_file_payload_sha256"] = source_payload["payload_sha256"]
    manifest["artifacts"]["source_files"] = RUNNER._artifact(
        paths["source_files"]
    )
    _write_manifest(paths, manifest)

    with pytest.raises(ValueError, match="lacks 6.3 binding"):
        RUNNER._load_stage(
            tmp_path, "train", expected_lineage=_fixture_stage_lineage()
        )


def test_missing_admission_artifact_stops_before_return_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    paths["admission_diagnostics"].unlink()
    opened = _forbid_return_work(monkeypatch, paths)

    with pytest.raises(
        ValueError, match="artifact identity failed: admission_diagnostics"
    ):
        _evaluate_train_fixture(tmp_path, str(manifest["payload_sha256"]))

    assert opened == {"decisions": False, "pricing": False, "portfolio": False}
    assert not (paths["root"] / "exact-runs").exists()
    assert not paths["evaluation"].exists()


def test_stage_manifest_requires_the_exact_artifact_allowlist(
    tmp_path: Path,
) -> None:
    _paths, manifest = _write_valid_stage_fixture(tmp_path)
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, dict)
    artifacts.pop("admission_diagnostics")
    _write_manifest(_paths, manifest)

    with pytest.raises(ValueError, match="artifact allowlist mismatch"):
        RUNNER._load_stage(
            tmp_path, "train", expected_lineage=_fixture_stage_lineage()
        )


def test_evaluation_rejects_a_replaced_manifest_against_the_build_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    completed_build_sha256 = str(manifest["payload_sha256"])
    manifest["synchronous_replacement_marker"] = "different-stage-instance"
    _write_manifest(paths, manifest)
    assert manifest["payload_sha256"] != completed_build_sha256
    opened = _forbid_return_work(monkeypatch, paths)

    with pytest.raises(ValueError, match="payload differs from the completed build"):
        _evaluate_train_fixture(tmp_path, completed_build_sha256)

    assert opened == {"decisions": False, "pricing": False, "portfolio": False}
    assert not (paths["root"] / "exact-runs").exists()


def test_self_hashed_manifest_cannot_relax_the_admission_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    contract = dict(manifest["score_data_admission"])
    contract["finite_score_count_min_per_signal"] = 24
    manifest["score_data_admission"] = contract
    _write_manifest(paths, manifest)
    opened = _forbid_return_work(monkeypatch, paths)

    with pytest.raises(ValueError, match="score-data admission contract mismatch"):
        _evaluate_train_fixture(tmp_path, str(manifest["payload_sha256"]))

    assert opened == {"decisions": False, "pricing": False, "portfolio": False}
    assert not (paths["root"] / "exact-runs").exists()


def test_rehashed_hard_anomaly_cannot_bypass_admission_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    tampered = pd.read_parquet(paths["admission_diagnostics"])
    tampered.loc[0, "invalid_non_null_fundamental_count"] = 1
    tampered.to_parquet(paths["admission_diagnostics"], index=False)
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, dict)
    artifacts["admission_diagnostics"] = RUNNER._artifact(
        paths["admission_diagnostics"]
    )
    _write_manifest(paths, manifest)
    opened = _forbid_return_work(monkeypatch, paths)

    with pytest.raises(ValueError, match="structural score data admission failed"):
        _evaluate_train_fixture(tmp_path, str(manifest["payload_sha256"]))

    assert opened == {"decisions": False, "pricing": False, "portfolio": False}
    assert not (paths["root"] / "exact-runs").exists()
    assert not paths["evaluation"].exists()


def test_evaluation_candidates_must_exactly_match_the_admitted_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    opened = _forbid_return_work(monkeypatch, paths)

    with pytest.raises(ValueError, match="candidates differ"):
        RUNNER.evaluate_stage(
            stage="train",
            candidates=tuple(reversed(RUNNER.UNIVERSE_IDS)),
            work_root=tmp_path,
            research_config_path=(
                ROOT / RUNNER.FROZEN_IMPLEMENTATION_PATHS["research_config"]
            ),
            expected_manifest_payload_sha256=str(manifest["payload_sha256"]),
            expected_lineage=_fixture_stage_lineage(),
        )

    assert opened == {"decisions": False, "pricing": False, "portfolio": False}
    assert not (paths["root"] / "exact-runs").exists()


def test_replay_requires_manifest_row_counts_to_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    manifest["admission_diagnostic_row_count"] = (
        int(manifest["admission_diagnostic_row_count"]) + 1
    )
    _write_manifest(paths, manifest)
    opened = _forbid_return_work(monkeypatch, paths)

    with pytest.raises(
        ValueError, match="admission_diagnostic_row_count differs from replay"
    ):
        _evaluate_train_fixture(tmp_path, str(manifest["payload_sha256"]))

    assert opened == {"decisions": False, "pricing": False, "portfolio": False}
    assert not (paths["root"] / "exact-runs").exists()


def test_replay_requires_manifest_coverage_to_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, manifest = _write_valid_stage_fixture(tmp_path)
    coverage = manifest["finite_score_coverage"]
    assert isinstance(coverage, dict)
    control = coverage[RUNNER.CONTROL_ID]
    assert isinstance(control, dict)
    control["median"] = float(control["median"]) - 0.01
    _write_manifest(paths, manifest)
    opened = _forbid_return_work(monkeypatch, paths)

    with pytest.raises(ValueError, match="coverage diagnostics differ from replay"):
        _evaluate_train_fixture(tmp_path, str(manifest["payload_sha256"]))

    assert opened == {"decisions": False, "pricing": False, "portfolio": False}
    assert not (paths["root"] / "exact-runs").exists()
