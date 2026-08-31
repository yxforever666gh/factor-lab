"""Single command-line entry point for the lightweight Factor Lab mainline."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
from typing import Any, Mapping, Sequence

from factor_lab.data import (
    RuntimeLayout,
    audit_top500_store,
    build_data,
    enrich_top500_store,
    load_data_config,
    plan_feature_store_migration,
    sync_data,
    sync_daily_stock_st,
    sync_enrichment,
    sync_exact_reference,
    sync_suspensions,
)
from factor_lab.data.suspensions import SuspensionProviderWaitingError
from factor_lab.release_integrity import (
    AUDIT_EVIDENCE_PATH,
    CORRECTIVE_AMENDMENT_FILE_SHA256,
    CORRECTIVE_AMENDMENT_PATH,
    FROZEN_IMPLEMENTATION_PATHS,
    PRESELECTION_CLOSURE_PATH,
    RELEASE_RESULT_PATH,
    RUNTIME_FILE_SHA256,
    RUNTIME_PATH,
    WINNER_FREEZE_PATH,
    file_sha256,
    verify_corrective_amendment_contract,
    verify_frozen_runtime_contract,
    verify_preselection_closure,
    verify_release_result,
    verify_winner_freeze,
)
from factor_lab.research.runner import latest_run, run_research
from factor_lab.strategy import generate_sleeve_target_schedule


V7_PROTOCOL_PATH = "protocols/7.0-multi-asset.json"
V7_ASSET_SELECTION_PATH = "protocols/7.0-asset-selection.json"
V7_CLOSURE_PATH = "protocols/7.0-release.json"
V7_PRECLOSURE_TRAIN_PATH = "protocols/evidence/7.0/preclosure-train.json"
V7_WINNER_FREEZE_PATH = "protocols/evidence/7.0/winner-freeze.json"
V7_AUDIT_PATH = "protocols/evidence/7.0/historical-audit.json"
V7_RESULT_PATH = "protocols/evidence/7.0/result.json"
V7_RUNTIME_PATH = "runtime/data/multi-asset-7.0"
V7_FAILURE_PATH = "protocols/evidence/7.0/execution-failure.json"
V71_CLOSURE_PATH = "protocols/7.1-release.json"
V71_WINNER_FREEZE_PATH = "protocols/evidence/7.1/winner-freeze.json"
V71_AUDIT_PATH = "protocols/evidence/7.1/historical-audit.json"
V71_RESULT_PATH = "protocols/evidence/7.1/result.json"
V71_RUNTIME_PATH = "runtime/data/multi-asset-7.1"
V7_TAG_OBJECT = "25bbc306e8842feab923380416f8329e0dd81100"
V7_TAG_COMMIT = "412026ca0370d53ca704adfd1122a811e768842e"
V8_PROTOCOL_PATH = "protocols/8.0-static-capital-budget.json"
V8_ASSET_SELECTION_PATH = V7_ASSET_SELECTION_PATH
V8_CLOSURE_PATH = "protocols/8.0-release.json"
V8_EVIDENCE_ROOT = "protocols/evidence/8.0"
V8_TRAIN_ADMISSION_PATH = f"{V8_EVIDENCE_ROOT}/train-admission.json"
V8_WINNER_FREEZE_PATH = "protocols/evidence/8.0/winner-freeze.json"
V8_AUDIT_PATH = "protocols/evidence/8.0/historical-audit.json"
V8_RESULT_PATH = "protocols/evidence/8.0/result.json"
V8_FAILURE_PATH = "protocols/evidence/8.0/execution-failure.json"
V8_RUNTIME_PATH = "runtime/data/multi-asset-8.0"
V8_TAG_OBJECT = "3fcbd73f7497b074e484ce7793e2d3603bf5a177"
V8_TAG_COMMIT = "78aba86bf4e741699afca1acd1470493785fd952"
V8_FAILURE_PAYLOAD_SHA256 = (
    "751b85c6c2e52b450e9c3549f7f4504af50b634599be4c32e240ee503de9823a"
)
V8_FAILURE_FILE_SHA256 = (
    "6af779495081f6ee391c6388a1e4342b878168b529f8074cf03d9ec2cc50eeaa"
)
V71_TAG_OBJECT = "15ea8e8de95638fdc0786ff0f35177b0ecba878d"
V71_TAG_COMMIT = "e7f09e17646cc44d78a49f6ddc41acc471f205d4"
V81_PROTOCOL_PATH = "protocols/8.1-policy-operational-metric-reclassification.json"
V81_CLOSURE_PATH = "protocols/8.1-release.json"
V81_EVIDENCE_ROOT = "protocols/evidence/8.1"
V81_RECLASSIFICATION_PATH = f"{V81_EVIDENCE_ROOT}/train-reclassification.json"
V81_WINNER_FREEZE_PATH = f"{V81_EVIDENCE_ROOT}/winner-freeze.json"
V81_AUDIT_PATH = f"{V81_EVIDENCE_ROOT}/historical-audit.json"
V81_RESULT_PATH = f"{V81_EVIDENCE_ROOT}/result.json"
V81_RUNTIME_PATH = "runtime/data/multi-asset-8.1"
V81_ROUTE = "policy_operational_metric_reclassification"
V81_PROTOCOL_ID = "factor-lab/8.1/policy-operational-metric-reclassification-v1"
V81_PROTOCOL_PAYLOAD_SHA256 = (
    "2fc5ea8316173f7fd19fbf5c34248e5a70b2a901c99345dcf8d933826fa15ee5"
)
V81_PROTOCOL_FILE_SHA256 = (
    "b0a213b62cf6f2723425e77d01565fd8c29721960d50d4a25d19306f3817c583"
)
V81_TAG_OBJECT = "8f575ed3833c8cc01f89e7a951d4234bd7ee6622"
V81_TAG_COMMIT = "a4c0d36f727e99f6b2353facf24fd3cdedba958e"
V81_CLOSURE_PAYLOAD_SHA256 = "f4a47421d08ca77eca6b27fd6417909a04c3eaf789c11d9ca069366412440ef5"
V81_CLOSURE_FILE_SHA256 = "ef1596fa5cfbfdfd0c27d74c2747dcc852b7f209a4e27de2b7c01c6d8dbcc557"
V81_RECLASSIFICATION_PAYLOAD_SHA256 = "4f498ffc12deac61144c77c56ba89cb9abccc034d2d73df4f1df8a6c50184c79"
V81_RECLASSIFICATION_FILE_SHA256 = "bfd2c0c801259394861eba000a8e34bc9617cba3adcf6629d7e8b501ccf3c51b"
V81_FREEZE_PAYLOAD_SHA256 = "d10f51b522a16838a4744fa16d770a720d34c2d340c2bf0bd5a05bedc61ceb76"
V81_FREEZE_FILE_SHA256 = "b865e80cb899f7e5274d72b46ab1e0d88dad64b0ab2eb4e46750c5cec2167387"
V81_RESULT_PAYLOAD_SHA256 = "d4496b9a64def6a443827737987d44ec77532cc9d11137a247302376a00ad6a4"
V81_RESULT_FILE_SHA256 = "bcbcb09974e6314190de7a835560c4abbc1cde79734ed4fcef759061653cd95d"
V9_PROTOCOL_PATH = "protocols/9.0-causal-volatility-balanced-budget.json"
V9_SCOUT_PATH = "protocols/9.0-preprotocol-scout.json"
V9_CLOSURE_PATH = "protocols/9.0-release.json"
V9_EVIDENCE_ROOT = "protocols/evidence/9.0"
V9_WINNER_FREEZE_PATH = f"{V9_EVIDENCE_ROOT}/winner-freeze.json"
V9_AUDIT_PATH = f"{V9_EVIDENCE_ROOT}/historical-audit.json"
V9_RESULT_PATH = f"{V9_EVIDENCE_ROOT}/result.json"
V9_RUNTIME_PATH = "runtime/data/multi-asset-9.0"
V9_ROUTE = "causal_monthly_volatility_balanced_budget"
V9_PROTOCOL_ID = "factor-lab/9.0/causal-monthly-volatility-balanced-budget-v1"
V9_PROTOCOL_PAYLOAD_SHA256 = "f6c7cce39e8b9a1ae5df10965a2dd607916095b2caf24fcf0a29b625c5bafc3e"
V9_PROTOCOL_FILE_SHA256 = "19ecf56b5bd9c8b42b9f4df50761f719e2ca544eaea959a88c62d0ea4178d620"
V9_SCOUT_PAYLOAD_SHA256 = "71926f08ce5ca2ab1b6470f7d3ee385371c4bfaf3243c5f942a891f63a8075a0"
V9_SCOUT_FILE_SHA256 = "44b90b964ecca9a30029b1dfad45ae313ae4a5c12a91d82ba885ceecb826b857"
V9_TAG_OBJECT = "c5e00f055183cceab44e3f8d182727e198af5714"
V9_TAG_COMMIT = "ed7627c974d9d04cd653be61b2966397e075719f"
V9_CLOSURE_PAYLOAD_SHA256 = "722d93904d3bc67792f32fb7a39ab8461336fa1956513c9ea2586d9ce31e68b3"
V9_CLOSURE_FILE_SHA256 = "049c5de6e7b8c1113445c9790e4e572ffb5694b56ebc7e21f70f2c28ed4850e5"
V9_FREEZE_PAYLOAD_SHA256 = "430b45eec730084a3d82e7d392bf609e533d5c7a98b5623f9d13a471171495a7"
V9_FREEZE_FILE_SHA256 = "98bdff4454b1a9430ca5f343bc4ce08a63924701b3a17e780f57c2454b3b413b"
V9_AUDIT_PAYLOAD_SHA256 = "7a034510cc38aaca5ea2b2113265c2ff2b984c302f366cd68f34f8c73af98681"
V9_AUDIT_FILE_SHA256 = "737ccdd9146334732a6e6a60e52423e45f0e7b9eb76837200b232e2a34601018"
V9_RESULT_PAYLOAD_SHA256 = "3b6fbcab3dafb1086be3062109d02c1c05f408d30913dc15146ed2b7eb3aa7b2"
V9_RESULT_FILE_SHA256 = "816bdb1837b75a8110bb863fa5584dc54166107a26671a402d17f5dc23f4f076"
V10_PROTOCOL_PATH = "protocols/10.0-results-first-quarterly-borda.json"
V10_EVIDENCE_PATH = "protocols/evidence/10.0/results-first-diagnostic.json"
V10_RUNNER_PATH = "scripts/run-10.0-results-first.py"
V10_SOURCE_MANIFEST_PATH = "runtime/data/multi-asset-9.0/sources/stage=audit/manifest.json"
V10_ROUTE = "quarterly_12_1_dual_momentum_rank_budget"
V10_PROTOCOL_ID = "factor-lab/10.0/results-first-quarterly-borda-v1"
V10_PROTOCOL_PAYLOAD_SHA256 = "dc79550ee9fefe4fdb01f54fe0c299a40c2d118a687f6e5571156dff5701cb7b"
V10_PROTOCOL_FILE_SHA256 = "6a949ce4374f407a6084053a08b76dedbb3f1478fbe56edf233bb23befe730dd"
V10_SOURCE_MANIFEST_PAYLOAD_SHA256 = "050ad4ddcb86dc4fbc71befad54c400b48a44f72ab6fecc33936b6da0c8f9aff"
V10_SOURCE_MANIFEST_FILE_SHA256 = "cdbf8ba498142adff04216b476522f47ee18df6f0fa02f3395d0e141191adbfa"


def _root() -> Path:
    """Locate the checkout even when Factor Lab is installed as a wheel.

    Editable installs happen to place ``__file__`` under ``<root>/src``.  A
    normal wheel instead places it under ``<root>/runtime/environments/...``;
    assuming a fixed parent depth would silently create a second runtime tree
    inside site-packages.  Walk both the installed-file and current-directory
    ancestry and require the repository's exact marker set.
    """

    starts = (Path(__file__).resolve().parent, Path.cwd().resolve())
    seen: set[Path] = set()
    for start in starts:
        for candidate in (start, *start.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            if (
                (candidate / ".git").exists()
                and (candidate / "pyproject.toml").is_file()
                and (candidate / "configs" / "data.json").is_file()
            ):
                return candidate
    raise SystemExit(
        "cannot locate the Factor Lab checkout; run inside it or pass --root"
    )


def _json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"JSON artifact must contain an object: {path}")
    return value


def _canonical_payload_sha256(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("payload_sha256", None)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _mode(arguments: argparse.Namespace) -> str:
    return "full" if bool(getattr(arguments, "full", False)) else "canary"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="factor-lab",
        description="Causal point-in-time research and exact cost-aware execution.",
    )
    parser.add_argument("--root", type=Path, default=None, help=argparse.SUPPRESS)
    commands = parser.add_subparsers(dest="command", required=True)

    data = commands.add_parser("data", help="Inspect, sync, or build local Parquet data.")
    data_commands = data.add_subparsers(dest="data_command", required=True)
    status = data_commands.add_parser("status", help="Show canonical data readiness.")
    status.add_argument("--deep", action="store_true", help="Read data columns and check keys/coverage.")
    status.add_argument("--hash", action="store_true", help="Hash the canonical Parquet files.")

    sync = data_commands.add_parser("sync", help="Resume full-market Tushare daily partitions.")
    sync.add_argument("--from", dest="start_date", required=True)
    sync.add_argument("--to", dest="end_date", required=True)
    sync.add_argument(
        "--calendar-to",
        dest="calendar_end_date",
        help="Persist the official calendar through this date without downloading future partitions.",
    )
    sync.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    sync.add_argument("--dataset", action="append", dest="datasets")
    sync.add_argument("--max-partitions", type=int)

    stock_st = data_commands.add_parser(
        "stock-st",
        help="Resume official-session daily Tushare stock_st partitions.",
    )
    stock_st.add_argument("--from", dest="start_date", required=True)
    stock_st.add_argument("--to", dest="end_date", required=True)
    stock_st.add_argument(
        "--resume", action=argparse.BooleanOptionalAction, default=True
    )
    stock_st.add_argument("--max-partitions", type=int)
    stock_st.add_argument(
        "--checkpoint",
        type=Path,
        help="Use a separate stock_st checkpoint (for post-freeze audit isolation).",
    )

    reference = data_commands.add_parser(
        "reference",
        help="Capture one stable exact-as-of raw bak_basic reference partition.",
    )
    reference.add_argument("--trade-date", required=True)

    suspensions = data_commands.add_parser(
        "suspensions", help="Synchronize Tushare daily suspension/resumption events."
    )
    suspensions.add_argument("--from", dest="start_date", required=True)
    suspensions.add_argument("--to", dest="end_date", required=True)
    suspensions.add_argument(
        "--resume", action=argparse.BooleanOptionalAction, default=True
    )

    enrich = data_commands.add_parser(
        "enrich",
        help="Resume PIT financial/reference downloads and enrich canonical Parquet.",
    )
    enrich.add_argument("--from", dest="start_date", required=True)
    enrich.add_argument("--to", dest="end_date", required=True)
    enrich.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    enrich.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        choices=("fina_indicator_vip", "bak_basic", "stock_st"),
    )
    enrich.add_argument("--max-partitions", type=int)
    enrich.add_argument("--batch-size", type=int, default=100_000)
    enrich_phase = enrich.add_mutually_exclusive_group()
    enrich_phase.add_argument("--sync-only", action="store_true")
    enrich_phase.add_argument("--apply-only", action="store_true")

    build = data_commands.add_parser("build", help="Adopt and audit the frozen Top-500 store.")
    build_mode = build.add_mutually_exclusive_group(required=True)
    build_mode.add_argument("--canary", action="store_true")
    build_mode.add_argument("--full", action="store_true")
    build.add_argument(
        "--apply-migration",
        action="store_true",
        help="Copy and hash-verify the existing frozen store into runtime/data/top500.",
    )
    build.add_argument("--hash", action="store_true")

    research = commands.add_parser("research", help="Run or inspect historical factor research.")
    research_commands = research.add_subparsers(dest="research_command", required=True)
    run = research_commands.add_parser("run", help="Run a factor research suite.")
    run.add_argument(
        "--suite",
        choices=(
            "walk-forward",
            "results-first",
            "recovery",
            "next",
            "legacy-regression",
        ),
        default="walk-forward",
    )
    run_mode = run.add_mutually_exclusive_group(required=True)
    run_mode.add_argument("--canary", action="store_true")
    run_mode.add_argument("--full", action="store_true")
    run.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    run.add_argument("--no-robustness", action="store_true", help=argparse.SUPPRESS)
    research_commands.add_parser("status", help="Show the latest completed run.")

    strategy = commands.add_parser(
        "strategy", help="Inspect versioned strategy evidence or generate legacy targets."
    )
    strategy_commands = strategy.add_subparsers(
        dest="strategy_command", required=True
    )
    strategy_status = strategy_commands.add_parser(
        "status", help="Verify tracked strategy implementation and evidence."
    )
    strategy_status.add_argument(
        "--verify-data",
        action="store_true",
        help="Also hash the canonical Parquet inputs (slower).",
    )
    strategy_status.add_argument(
        "--release",
        choices=("6.0", "6.3", "7.0", "7.1", "8.0", "8.1", "9.0", "10.0"),
        help="Verify one release closure; defaults to the latest tracked closure.",
    )
    strategy_targets = strategy_commands.add_parser(
        "targets",
        help="Rebuild the deterministic target state through one official session.",
    )
    strategy_targets.add_argument(
        "--signal-date",
        default="latest",
        help="Official session date or 'latest' (latest date with a signal).",
    )

    report = commands.add_parser("report", help="Print a completed Markdown report.")
    report.add_argument("--run", default="latest", help="Run id or 'latest'.")

    return parser


def _data_layout(root: Path) -> tuple[dict[str, Any], RuntimeLayout, Path]:
    config_path = root / "configs" / "data.json"
    config = load_data_config(config_path)
    return config, RuntimeLayout.from_config(config, config_path=config_path, repo_root=root), config_path


def _data_command(arguments: argparse.Namespace) -> int:
    root = arguments.root.resolve()
    config, layout, config_path = _data_layout(root)
    if arguments.data_command == "status":
        audit = audit_top500_store(
            layout,
            config,
            deep=bool(arguments.deep),
            hash_files=bool(arguments.hash),
        )
        migration = plan_feature_store_migration(
            config_path=config_path,
            config=config,
            layout=layout,
            hash_files=bool(arguments.hash),
        )
        _json({"runtime_root": str(layout.runtime_root), "audit": audit, "migration": migration})
        return 0
    if arguments.data_command == "sync":
        result = sync_data(
            arguments.start_date,
            arguments.end_date,
            calendar_end_date=arguments.calendar_end_date,
            config_path=config_path,
            layout=layout,
            datasets=arguments.datasets,
            resume=bool(arguments.resume),
            max_partitions=arguments.max_partitions,
        )
        _json(result)
        status = str(result.get("status") or "")
        if status in {"complete", "partial"}:
            return 0
        if status == "waiting":
            return 2
        if status == "blocked":
            return 3
        return 1
    if arguments.data_command == "stock-st":
        try:
            result = sync_daily_stock_st(
                arguments.start_date,
                arguments.end_date,
                config_path=config_path,
                layout=layout,
                resume=bool(arguments.resume),
                max_partitions=arguments.max_partitions,
                checkpoint_path=arguments.checkpoint,
            )
        except (OSError, TypeError, ValueError) as exc:
            result = {
                "schema_version": 1,
                "status": "blocked",
                "reason": "daily_stock_st_evidence_invalid",
                "error": str(exc),
            }
        _json(result)
        return {
            "complete": 0,
            "partial": 0,
            "blocked": 3,
        }.get(str(result.get("status")), 1)
    if arguments.data_command == "reference":
        try:
            result = sync_exact_reference(
                arguments.trade_date,
                config_path=config_path,
                layout=layout,
            )
        except ValueError as exc:
            result = {
                "schema_version": 1,
                "status": "blocked",
                "reason": "reference_evidence_invalid",
                "error": str(exc),
            }
        _json(result)
        return {
            "complete": 0,
            "waiting": 2,
            "blocked": 3,
        }.get(str(result.get("status")), 1)
    if arguments.data_command == "suspensions":
        try:
            result = sync_suspensions(
                arguments.start_date,
                arguments.end_date,
                config_path=config_path,
                layout=layout,
                resume=bool(arguments.resume),
            )
        except SuspensionProviderWaitingError as exc:
            result = {
                "schema_version": 1,
                "status": "waiting",
                "reason": "provider_temporarily_unavailable",
                "error": str(exc),
            }
        except (OSError, TypeError, ValueError) as exc:
            result = {
                "schema_version": 1,
                "status": "blocked",
                "reason": "suspension_evidence_invalid",
                "error": str(exc),
            }
        _json(result)
        return {
            "complete": 0,
            "waiting": 2,
            "blocked": 3,
        }.get(str(result.get("status")), 1)
    if arguments.data_command == "enrich":
        sync_result = None
        if not bool(arguments.apply_only):
            sync_result = sync_enrichment(
                arguments.start_date,
                arguments.end_date,
                config_path=config_path,
                layout=layout,
                datasets=arguments.datasets,
                resume=bool(arguments.resume),
                max_partitions=arguments.max_partitions,
            )
        if bool(arguments.sync_only) or (
            sync_result is not None and sync_result.get("status") != "complete"
        ):
            result = {"status": sync_result.get("status"), "sync": sync_result}
            _json(result)
            return 0 if result["status"] in {"complete", "partial"} else 1
        enrichment = enrich_top500_store(
            config_path=config_path,
            layout=layout,
            batch_size=int(arguments.batch_size),
        )
        result = {"status": enrichment.get("status"), "sync": sync_result, "enrichment": enrichment}
        _json(result)
        return 0 if result["status"] == "complete" else 1
    if arguments.data_command == "build":
        result = build_data(
            _mode(arguments),
            config_path=config_path,
            layout=layout,
            apply_migration=bool(arguments.apply_migration),
            hash_files=bool(arguments.hash),
        )
        _json(result)
        return 0 if result.get("status") == "ready" else 1
    raise AssertionError(arguments.data_command)


def _compact_research(summary: Mapping[str, Any]) -> dict[str, Any]:
    walk_forward = dict(summary.get("walk_forward") or {})
    compact = {
        "status": summary.get("status"),
        "run_id": summary.get("run_id"),
        "suite": summary.get("suite"),
        "mode": summary.get("mode"),
        "validated_count": summary.get("validated_count"),
        "validated_factors": summary.get("validated_factors"),
        "stage_b_selected": summary.get("stage_b_selected"),
        "search_stopped": summary.get("search_stopped"),
        "best_historical_strategy": (summary.get("results_first") or {}).get(
            "best_historical_strategy"
        ),
        "results_first_top": ((summary.get("results_first") or {}).get("rankings") or [])[:5],
        "walk_forward": {
            "evidence_class": walk_forward.get("evidence_class"),
            "canary_smoke_only": walk_forward.get("canary_smoke_only"),
            "causal_history_valid": walk_forward.get("causal_history_valid"),
            "historical_diagnostic_passed": walk_forward.get(
                "historical_diagnostic_passed"
            ),
            "future_selection_violation_count": walk_forward.get(
                "future_selection_violation_count"
            ),
            "common_evaluation_start": walk_forward.get("common_evaluation_start"),
            "dynamic_phase_rank": walk_forward.get("dynamic_phase_rank"),
            "dynamic_status": walk_forward.get("dynamic_status"),
            "best_phase_strategy": walk_forward.get("best_phase_strategy"),
            "fixed_comparator": (walk_forward.get("fixed_comparator") or {}).get(
                "factor_name"
            ),
            "dynamic_annual_q20_delta_vs_fixed_comparator": (
                ((walk_forward.get("fixed_comparator") or {}).get(
                    "dynamic_phase_deltas"
                ) or {}).get("net_annual_return")
                or {}
            ).get("q20"),
        }
        if walk_forward.get("enabled")
        else None,
        "data": summary.get("data"),
    }
    if summary.get("suite") != "results-first":
        compact.pop("best_historical_strategy", None)
        compact.pop("results_first_top", None)
    return compact


def _research_command(arguments: argparse.Namespace) -> int:
    root = arguments.root.resolve()
    if arguments.research_command == "run":
        summary = run_research(
            project_root=root,
            suite=arguments.suite,
            mode=_mode(arguments),
            resume=bool(arguments.resume),
            run_robustness=not bool(arguments.no_robustness),
        )
        _json(_compact_research(summary))
        return 0
    if arguments.research_command == "status":
        latest = latest_run(root)
        if latest is None:
            _json({"status": "no_runs"})
            return 0
        summary_path = Path(str(latest["summary_path"]))
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        _json({**latest, **_compact_research(summary)})
        return 0
    raise AssertionError(arguments.research_command)


def _report_command(arguments: argparse.Namespace) -> int:
    root = arguments.root.resolve()
    if arguments.run == "latest":
        latest = latest_run(root)
        if latest is None:
            raise SystemExit("no completed research runs")
        output_dir = Path(str(latest["output_dir"]))
    else:
        output_dir = root / "runtime" / "runs" / str(arguments.run)
    report_path = output_dir / "report.md"
    if not report_path.is_file():
        raise SystemExit(f"missing report: {report_path}")
    print(report_path.read_text(encoding="utf-8"))
    return 0


def _strategy_status_6_0(root: Path, *, verify_data: bool) -> tuple[dict[str, Any], int]:
    protocol_path = root / "protocols" / "6.0-low-churn.json"
    protocol = _read_json(protocol_path)
    checks: list[dict[str, Any]] = []

    def check_file(
        relative_path: str,
        expected_sha256: str,
        *,
        category: str,
        enabled: bool = True,
    ) -> None:
        path = root / relative_path
        item: dict[str, Any] = {
            "category": category,
            "path": relative_path.replace("\\", "/"),
            "expected_sha256": expected_sha256,
        }
        if not enabled:
            item["status"] = "not_verified"
        elif not path.is_file():
            item["status"] = "missing"
        else:
            actual = _file_sha256(path)
            item.update(
                {
                    "status": "match" if actual == expected_sha256 else "mismatch",
                    "actual_sha256": actual,
                }
            )
        checks.append(item)

    def check_json_payload(
        relative_path: str,
        expected_sha256: str,
        *,
        category: str,
    ) -> None:
        path = root / relative_path
        item: dict[str, Any] = {
            "category": category,
            "path": relative_path.replace("\\", "/"),
            "expected_sha256": expected_sha256,
        }
        if not path.is_file():
            item["status"] = "missing"
        else:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(value, dict):
                    raise ValueError("root is not an object")
                actual = _canonical_payload_sha256(value)
                item.update(
                    {
                        "status": (
                            "match" if actual == expected_sha256 else "mismatch"
                        ),
                        "actual_sha256": actual,
                    }
                )
            except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
                item.update({"status": "invalid_json", "error": str(exc)})
        checks.append(item)

    check_json_payload(
        "protocols/6.0-low-churn.json",
        str(protocol.get("payload_sha256") or ""),
        category="protocol_payload:low_churn",
    )

    for name, binding in sorted(dict(protocol.get("implementation") or {}).items()):
        if isinstance(binding, Mapping):
            check_file(
                str(binding.get("path") or ""),
                str(binding.get("sha256") or ""),
                category=f"implementation:{name}",
            )
    for name, binding in sorted(dict(protocol.get("evidence") or {}).items()):
        if isinstance(binding, Mapping):
            check_file(
                str(binding.get("path") or ""),
                str(binding.get("file_sha256") or ""),
                category=f"evidence:{name}",
            )
            payload_sha256 = str(binding.get("payload_sha256") or "")
            if payload_sha256:
                check_json_payload(
                    str(binding.get("path") or ""),
                    payload_sha256,
                    category=f"evidence_payload:{name}",
                )
    data_paths = {
        "features": "runtime/data/top500/features.parquet",
        "execution": "runtime/data/top500/execution.parquet",
        "suspension_metadata": "runtime/data/top500/suspensions.meta.json",
        "suspensions": "runtime/data/top500/suspensions.parquet",
    }
    for name, expected in sorted(
        dict(protocol.get("canonical_data_sha256") or {}).items()
    ):
        check_file(
            data_paths.get(str(name), f"runtime/data/top500/{name}.parquet"),
            str(expected),
            category=f"canonical_data:{name}",
            enabled=verify_data,
        )

    result_path = root / "protocols" / "evidence" / "6.0" / "result.json"
    result = _read_json(result_path) if result_path.is_file() else {}
    event_path = (
        root / "protocols" / "evidence" / "6.0" / "pit-event-negative.json"
    )
    event = _read_json(event_path) if event_path.is_file() else {}
    orthogonal_path = (
        root
        / "protocols"
        / "evidence"
        / "6.0"
        / "orthogonal-canonical-negative.json"
    )
    orthogonal = _read_json(orthogonal_path) if orthogonal_path.is_file() else {}
    analyst_protocol_path = root / "protocols" / "6.0-analyst-revisions.json"
    analyst_protocol = (
        _read_json(analyst_protocol_path) if analyst_protocol_path.is_file() else {}
    )
    analyst_evidence_path = (
        root / "protocols" / "evidence" / "6.0" / "analyst-scout.json"
    )
    analyst_evidence = (
        _read_json(analyst_evidence_path) if analyst_evidence_path.is_file() else {}
    )
    failures = [
        item
        for item in checks
        if item.get("status") not in {"match", "not_verified"}
    ]
    summary = dict(result.get("summary") or {})
    candidate = dict(summary.get("candidate") or {})
    candidate_phases = dict(candidate.get("phases") or {})
    output = {
        "status": "ready" if not failures else "integrity_mismatch",
        "version": protocol.get("version"),
        "route": protocol.get("route"),
        "protocol_id": protocol.get("protocol_id"),
        "historical_evidence_class": (
            protocol.get("claim_contract") or {}
        ).get("historical_evidence_class"),
        "profit_claim_allowed": (
            protocol.get("claim_contract") or {}
        ).get("profit_claim_allowed"),
        "candidate": {
            "id": candidate.get("candidate_id"),
            "full": candidate_phases.get("full"),
            "audit": candidate_phases.get("audit"),
        },
        "pit_event_search": {
            "selected_candidate_id": (event.get("decision") or {}).get(
                "selected_candidate_id"
            ),
            "candidate_count": (event.get("decision") or {}).get(
                "candidate_count"
            ),
            "candidate_pass_count": (event.get("decision") or {}).get(
                "candidate_pass_count"
            ),
        },
        "orthogonal_canonical_search": {
            "selected_candidate_id": (orthogonal.get("decision") or {}).get(
                "selected_candidate_id"
            ),
            "candidate_count": (orthogonal.get("decision") or {}).get(
                "candidate_count"
            ),
            "train_q20_positive_count": (orthogonal.get("decision") or {}).get(
                "train_q20_positive_count"
            ),
            "validation_q20_positive_count": (
                orthogonal.get("decision") or {}
            ).get("validation_q20_positive_count"),
        },
        "next_data_lane": {
            "protocol_id": analyst_protocol.get("protocol_id"),
            "status": analyst_protocol.get("status"),
            "source_endpoint": (analyst_protocol.get("source") or {}).get(
                "endpoint"
            ),
            "returns_or_labels_opened": (
                analyst_protocol.get("research_conclusion") or {}
            ).get("returns_or_labels_opened"),
            "alpha_claim": (analyst_protocol.get("research_conclusion") or {}).get(
                "alpha_claim"
            ),
            "selected_route": analyst_evidence.get("selected_route"),
            "current_permission": (
                (analyst_protocol.get("source") or {}).get("permission") or {}
            ).get("observed_state"),
        },
        "canonical_data_hashes_verified": bool(verify_data),
        "checks": checks,
    }
    return output, 0 if not failures else 3


def _strategy_status_6_3(root: Path, *, verify_data: bool) -> tuple[dict[str, Any], int]:
    closure_path = root / PRESELECTION_CLOSURE_PATH
    closure: dict[str, Any] = {}
    checks: list[dict[str, Any]] = []
    integrity_error: str | None = None
    release_result: dict[str, Any] | None = None
    winner_freeze: dict[str, Any] | None = None
    try:
        closure = _read_json(closure_path)
        verified_closure = verify_preselection_closure(
            root,
            closure_path=closure_path,
            protocol_path=root / "protocols" / "6.2-wide-universe.json",
            amendment_path=(
                root / "protocols" / "6.2-wide-universe-amendment-1.json"
            ),
            corrective_amendment_path=(
                root / "protocols" / "6.3-corrective-amendment-1.json"
            ),
        )
        freeze_path = root / WINNER_FREEZE_PATH
        if freeze_path.is_file():
            winner_freeze = verify_winner_freeze(
                root,
                preselection_closure=verified_closure,
                freeze_path=freeze_path,
            )
        result_path = root / RELEASE_RESULT_PATH
        if result_path.is_file():
            release_result = verify_release_result(
                root,
                preselection_closure=verified_closure,
                result_path=result_path,
            )
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as exc:
        integrity_error = str(exc)

    def as_mapping(value: Any) -> Mapping[str, Any]:
        return value if isinstance(value, Mapping) else {}

    def check_file(
        relative_path: str,
        expected_sha256: str,
        *,
        category: str,
        enabled: bool = True,
    ) -> None:
        path = root / relative_path
        item: dict[str, Any] = {
            "category": category,
            "path": relative_path.replace("\\", "/"),
            "expected_sha256": expected_sha256,
        }
        if not enabled:
            item["status"] = "not_verified"
        elif not path.is_file():
            item["status"] = "missing"
        else:
            actual = _file_sha256(path)
            item.update(
                {
                    "status": "match" if actual == expected_sha256 else "mismatch",
                    "actual_sha256": actual,
                }
            )
        checks.append(item)

    def check_json_payload(
        relative_path: str,
        expected_sha256: str,
        *,
        category: str,
    ) -> None:
        path = root / relative_path
        item: dict[str, Any] = {
            "category": category,
            "path": relative_path.replace("\\", "/"),
            "expected_sha256": expected_sha256,
        }
        if not path.is_file():
            item["status"] = "missing"
        else:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(value, dict):
                    raise ValueError("root is not an object")
                actual = _canonical_payload_sha256(value)
                item.update(
                    {
                        "status": "match" if actual == expected_sha256 else "mismatch",
                        "actual_sha256": actual,
                    }
                )
            except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
                item.update({"status": "invalid_json", "error": str(exc)})
        checks.append(item)

    contract_valid = bool(
        integrity_error is None
        and set(as_mapping(closure.get("implementation")))
        == set(FROZEN_IMPLEMENTATION_PATHS)
    )
    checks.append(
        {
            "category": "release_contract",
            "path": PRESELECTION_CLOSURE_PATH,
            "status": "match" if contract_valid else "mismatch",
            **({"error": integrity_error} if integrity_error else {}),
        }
    )
    if (root / RELEASE_RESULT_PATH).is_file():
        result_binding_valid = release_result is not None and integrity_error is None
        checks.append(
            {
                "category": "terminal_result_contract",
                "path": RELEASE_RESULT_PATH,
                "status": "match" if result_binding_valid else "mismatch",
                **({"error": integrity_error} if integrity_error else {}),
            }
        )
    if (root / WINNER_FREEZE_PATH).is_file():
        freeze_binding_valid = winner_freeze is not None and integrity_error is None
        checks.append(
            {
                "category": "winner_freeze_contract",
                "path": WINNER_FREEZE_PATH,
                "status": "match" if freeze_binding_valid else "mismatch",
                **({"error": integrity_error} if integrity_error else {}),
            }
        )
    check_json_payload(
        PRESELECTION_CLOSURE_PATH,
        str(closure.get("payload_sha256") or ""),
        category="release_payload",
    )
    protocol = dict(as_mapping(closure.get("protocol")))
    protocol_path = str(protocol.get("path") or "")
    check_file(
        protocol_path,
        str(protocol.get("file_sha256") or ""),
        category="protocol_file:wide_universe",
    )
    check_json_payload(
        protocol_path,
        str(protocol.get("payload_sha256") or ""),
        category="protocol_payload:wide_universe",
    )
    amendment = dict(as_mapping(closure.get("protocol_amendment")))
    amendment_path = str(amendment.get("path") or "")
    check_file(
        amendment_path,
        str(amendment.get("file_sha256") or ""),
        category="protocol_file:wide_universe_amendment",
    )
    check_json_payload(
        amendment_path,
        str(amendment.get("payload_sha256") or ""),
        category="protocol_payload:wide_universe_amendment",
    )
    corrective_amendment = dict(
        as_mapping(closure.get("corrective_amendment"))
    )
    corrective_amendment_path = str(corrective_amendment.get("path") or "")
    check_file(
        corrective_amendment_path,
        str(corrective_amendment.get("file_sha256") or ""),
        category="protocol_file:corrective_amendment",
    )
    check_json_payload(
        corrective_amendment_path,
        str(corrective_amendment.get("payload_sha256") or ""),
        category="protocol_payload:corrective_amendment",
    )
    for name, binding in sorted(as_mapping(closure.get("implementation")).items()):
        if isinstance(binding, Mapping):
            check_file(
                str(binding.get("path") or ""),
                str(binding.get("sha256") or ""),
                category=f"implementation:{name}",
            )
    for name, binding in sorted(as_mapping(closure.get("evidence")).items()):
        if not isinstance(binding, Mapping):
            continue
        relative_path = str(binding.get("path") or "")
        check_file(
            relative_path,
            str(binding.get("file_sha256") or ""),
            category=f"evidence:{name}",
        )
        payload_sha256 = str(binding.get("payload_sha256") or "")
        if payload_sha256:
            check_json_payload(
                relative_path,
                payload_sha256,
                category=f"evidence_payload:{name}",
            )
    for name, binding in sorted(as_mapping(closure.get("canonical_data")).items()):
        if isinstance(binding, Mapping):
            check_file(
                str(binding.get("path") or ""),
                str(binding.get("sha256") or ""),
                category=f"canonical_data:{name}",
                enabled=verify_data,
            )
    failures = [
        item
        for item in checks
        if item.get("status") not in {"match", "not_verified"}
    ]
    effective = release_result or closure
    if release_result is None and winner_freeze is not None:
        winner = winner_freeze.get("selected_candidate_id")
        effective = {
            **closure,
            "status": (
                "selection_frozen_pending_historical_audit"
                if winner is not None
                else "selection_frozen_no_candidate_pending_finalize"
            ),
            "selected_candidate_id": winner,
            "audit_status": "not_opened",
        }
    output = {
        "status": (
            str(effective.get("status") or "unknown")
            if not failures
            else "integrity_mismatch"
        ),
        "version": "6.3",
        "route": closure.get("route"),
        "protocol_id": protocol.get("protocol_id"),
        "historical_evidence_class": (
            effective.get("claim_contract") or {}
        ).get("historical_evidence_class"),
        "profit_claim_allowed": (
            effective.get("claim_contract") or {}
        ).get("profit_claim_allowed"),
        "selected_candidate_id": effective.get("selected_candidate_id"),
        "audit_status": effective.get("audit_status"),
        "terminal_result_payload_sha256": (
            release_result.get("payload_sha256") if release_result else None
        ),
        "canonical_data_hashes_verified": bool(verify_data),
        "checks": checks,
    }
    return output, 0 if not failures else 3


def _working_tree_is_clean(root: Path) -> bool:
    return subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip() == ""


def _strategy_status_6_3_pending(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    """Report the clean pre-closure state without falling back to stale 6.0."""

    checks: list[dict[str, Any]] = []
    failures: list[str] = []
    try:
        corrective_path = root / CORRECTIVE_AMENDMENT_PATH
        corrective = _read_json(corrective_path)
        if (
            corrective.get("payload_sha256")
            != _canonical_payload_sha256(corrective)
            or file_sha256(corrective_path) != CORRECTIVE_AMENDMENT_FILE_SHA256
        ):
            raise ValueError("6.3 corrective amendment payload hash is invalid")
        verify_corrective_amendment_contract(root, corrective)
        runtime_path = root / RUNTIME_PATH
        runtime = _read_json(runtime_path)
        if (
            runtime.get("payload_sha256") != _canonical_payload_sha256(runtime)
            or file_sha256(runtime_path) != RUNTIME_FILE_SHA256
        ):
            raise ValueError("6.3 runtime payload hash is invalid")
        verify_frozen_runtime_contract(runtime)
        checks.extend(
            [
                {
                    "category": "protocol_payload:corrective_amendment",
                    "path": CORRECTIVE_AMENDMENT_PATH,
                    "status": "match",
                    "actual_sha256": corrective["payload_sha256"],
                },
                {
                    "category": "runtime_payload",
                    "path": RUNTIME_PATH,
                    "status": "match",
                    "actual_sha256": runtime["payload_sha256"],
                },
            ]
        )
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as exc:
        failures.append(str(exc))
        checks.append(
            {
                "category": "preclosure_contract",
                "path": CORRECTIVE_AMENDMENT_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
        )

    forbidden_before_closure = (
        PRESELECTION_CLOSURE_PATH,
        WINNER_FREEZE_PATH,
        AUDIT_EVIDENCE_PATH,
        RELEASE_RESULT_PATH,
    )
    for relative_path in forbidden_before_closure:
        exists = (root / relative_path).exists()
        checks.append(
            {
                "category": "preclosure_absence",
                "path": relative_path,
                "status": "unexpected" if exists else "match",
            }
        )
        if exists:
            failures.append(f"pre-closure artifact already exists: {relative_path}")

    clean = _working_tree_is_clean(root)
    checks.append(
        {
            "category": "preclosure_working_tree",
            "path": ".",
            "status": "match" if clean else "pending_clean_commit",
        }
    )
    if failures:
        status = "integrity_mismatch"
        exit_code = 3
    elif not clean:
        status = "implementation_pending_clean_commit"
        exit_code = 2
    else:
        status = "implementation_ready_for_preselection_closure"
        exit_code = 0

    return (
        {
            "status": status,
            "version": "6.3",
            "route": "widened_opportunity_set",
            "protocol_id": "factor-lab/6.2/widened-opportunity-set-v2",
            "selected_candidate_id": None,
            "audit_status": "not_opened",
            "profit_claim_allowed": False,
            "canonical_data_hashes_verified": bool(verify_data),
            "checks": checks,
        },
        exit_code,
    )


def _v7_json_check(
    root: Path,
    relative_path: str,
    *,
    expected_payload: str | None = None,
    expected_file: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    path = root / relative_path
    check: dict[str, Any] = {"path": relative_path}
    if not path.is_file() or path.is_symlink():
        check["status"] = "missing"
        return None, check
    try:
        value = _read_json(path)
        payload = _canonical_payload_sha256(value)
        actual_file = _file_sha256(path)
        valid = value.get("payload_sha256") == payload
        if expected_payload is not None:
            valid = valid and payload == expected_payload
        if expected_file is not None:
            valid = valid and actual_file == expected_file
        check.update(
            {
                "status": "match" if valid else "mismatch",
                "actual_payload_sha256": payload,
                "actual_file_sha256": actual_file,
            }
        )
        return value, check
    except (OSError, SystemExit, TypeError, ValueError, json.JSONDecodeError) as exc:
        check.update({"status": "invalid_json", "error": str(exc)})
        return None, check


def _load_v8_runner(root: Path) -> Any:
    resolved = root.resolve()
    script = resolved / "scripts" / "run-multi-asset-evidence.py"
    if script.is_symlink() or not script.is_file():
        raise ValueError("8.0 formal runner is missing or indirect")
    spec = importlib.util.spec_from_file_location("factor_lab_v8_status_verifier", script)
    if spec is None or spec.loader is None:
        raise ValueError("could not load the 8.0 formal verifier")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if (
        module.ROOT.resolve() != resolved
        or module.RELEASE != "8.0"
        or module.CLOSURE_PATH.as_posix() != V8_CLOSURE_PATH
        or module.WORK_ROOT.resolve() != (resolved / V8_RUNTIME_PATH).resolve()
        or module.SOURCE_ROOT.resolve()
        != (resolved / V8_RUNTIME_PATH / "sources").resolve()
        or module.EVALUATION_ROOT.resolve()
        != (resolved / V8_RUNTIME_PATH / "evaluations").resolve()
        or module.BINDING_ROOT.resolve()
        != (resolved / V8_RUNTIME_PATH / "stage-bindings").resolve()
        or module.PRIOR_WORK_ROOT.resolve()
        != (resolved / V71_RUNTIME_PATH).resolve()
        or module.EVIDENCE_ROOT.as_posix() != "protocols/evidence/8.0"
        or module.TRAIN_ADMISSION_PATH.as_posix() != V8_TRAIN_ADMISSION_PATH
        or module.WINNER_FREEZE_PATH.as_posix() != V8_WINNER_FREEZE_PATH
        or module.AUDIT_PATH.as_posix() != V8_AUDIT_PATH
        or module.RESULT_PATH.as_posix() != V8_RESULT_PATH
        or module.PROTOCOL_PATH.as_posix() != V8_PROTOCOL_PATH
        or module.INHERITED_PROTOCOL_PATH.as_posix() != V7_PROTOCOL_PATH
        or module.INHERITED_PROTOCOL_FILE_SHA256
        != "2d2e96a1605b5e088a7cf5952dd816d8aecb10e39b9ba529fe81b00592bfa14f"
        or module.INHERITED_PROTOCOL_PAYLOAD
        != "6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0"
    ):
        raise ValueError("8.0 runner contains an incorrect release namespace")
    return module


def _v8_require_head_ci(root: Path) -> str:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    _load_v8_runner(root)._require_head_pushed_and_ci_success(head)
    return head


def _load_v9_runner(root: Path) -> Any:
    resolved = root.resolve()
    script = resolved / "scripts" / "run-multi-asset-evidence.py"
    if script.is_symlink() or not script.is_file():
        raise ValueError("9.0 formal runner is missing or indirect")
    spec = importlib.util.spec_from_file_location("factor_lab_v9_status_verifier", script)
    if spec is None or spec.loader is None:
        raise ValueError("could not load the 9.0 formal verifier")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if (
        module.ROOT.resolve() != resolved
        or module.RELEASE != "9.0"
        or module.ROUTE != V9_ROUTE
        or module.PRIMARY_ID != V9_ROUTE
        or module.VOLATILITY_FLOOR != 1e-12
        or module.PROTOCOL_ID != V9_PROTOCOL_ID
        or module.PROTOCOL_PATH.as_posix() != V9_PROTOCOL_PATH
        or module.PROTOCOL_PAYLOAD != V9_PROTOCOL_PAYLOAD_SHA256
        or module.PROTOCOL_FILE_SHA256 != V9_PROTOCOL_FILE_SHA256
        or module.SCOUT_PATH.as_posix() != V9_SCOUT_PATH
        or module.SCOUT_PAYLOAD != V9_SCOUT_PAYLOAD_SHA256
        or module.SCOUT_FILE_SHA256 != V9_SCOUT_FILE_SHA256
        or module.CLOSURE_PATH.as_posix() != V9_CLOSURE_PATH
        or module.EVIDENCE_ROOT.as_posix() != V9_EVIDENCE_ROOT
        or module.WINNER_FREEZE_PATH.as_posix() != V9_WINNER_FREEZE_PATH
        or module.AUDIT_PATH.as_posix() != V9_AUDIT_PATH
        or module.RESULT_PATH.as_posix() != V9_RESULT_PATH
        or module.WORK_ROOT.resolve() != (resolved / V9_RUNTIME_PATH).resolve()
        or module.PRIOR_TAG_OBJECT != V81_TAG_OBJECT
        or module.PRIOR_COMMIT != V81_TAG_COMMIT
    ):
        raise ValueError("9.0 runner contains an incorrect release namespace")
    return module


def _v9_require_head_ci(root: Path) -> str:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    _load_v9_runner(root)._require_head_pushed_and_ci_success(head)
    return head


def _load_v10_runner(root: Path) -> Any:
    resolved = root.resolve()
    script = resolved / V10_RUNNER_PATH
    if script.is_symlink() or not script.is_file():
        raise ValueError("10.0 results-first runner is missing or indirect")
    spec = importlib.util.spec_from_file_location("factor_lab_v10_status_verifier", script)
    if spec is None or spec.loader is None:
        raise ValueError("could not load the 10.0 results-first verifier")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if (
        module.ROOT.resolve() != resolved
        or module.RELEASE != "10.0"
        or module.ROUTE != V10_ROUTE
        or module.QUARTERLY_BORDA_ID != V10_ROUTE
        or module.PROTOCOL_ID != V10_PROTOCOL_ID
        or module.PROTOCOL_PATH.as_posix() != V10_PROTOCOL_PATH
        or module.PROTOCOL_PAYLOAD != V10_PROTOCOL_PAYLOAD_SHA256
        or module.PROTOCOL_FILE_SHA256 != V10_PROTOCOL_FILE_SHA256
        or module.EVIDENCE_PATH.as_posix() != V10_EVIDENCE_PATH
        or module.SOURCE_MANIFEST_PAYLOAD != V10_SOURCE_MANIFEST_PAYLOAD_SHA256
        or module.SOURCE_MANIFEST_FILE_SHA256 != V10_SOURCE_MANIFEST_FILE_SHA256
    ):
        raise ValueError("10.0 runner contains an incorrect release namespace")
    return module


def _verify_published_7_0_failure(root: Path) -> dict[str, Any]:
    def git(*args: str, check: bool = True) -> bytes:
        completed = subprocess.run(
            ["git", *args],
            cwd=root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if check and completed.returncode != 0:
            raise ValueError(
                "could not verify the published 7.0 archive: "
                + completed.stderr.decode("utf-8", errors="replace").strip()
            )
        return completed.stdout

    if (
        git("cat-file", "-t", "refs/tags/7.0").decode("ascii").strip() != "tag"
        or git("rev-parse", "refs/tags/7.0").decode("ascii").strip()
        != V7_TAG_OBJECT
        or git("rev-parse", "refs/tags/7.0^{}").decode("ascii").strip()
        != V7_TAG_COMMIT
    ):
        raise ValueError("published 7.0 annotated tag identity differs")
    for relative in (
        V7_PROTOCOL_PATH,
        V7_ASSET_SELECTION_PATH,
        V7_CLOSURE_PATH,
        V7_PRECLOSURE_TRAIN_PATH,
        V7_FAILURE_PATH,
    ):
        current = (root / relative).read_bytes()
        if git("show", f"{V7_TAG_COMMIT}:{relative}") != current:
            raise ValueError(f"current 7.0 archive differs from its tag: {relative}")
    for relative in (V7_WINNER_FREEZE_PATH, V7_AUDIT_PATH, V7_RESULT_PATH):
        if subprocess.run(
            ["git", "cat-file", "-e", f"{V7_TAG_COMMIT}:{relative}"],
            cwd=root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).returncode == 0:
            raise ValueError(f"published 7.0 unexpectedly contains normal evidence: {relative}")
    failure = _read_json(root / V7_FAILURE_PATH)
    if (
        failure.get("payload_sha256")
        != "04099ab6c2bd03099c9d045120578344bfe9ba3c963dfb82a0cba9f8a49f5df9"
        or failure.get("payload_sha256") != _canonical_payload_sha256(failure)
        or failure.get("status") != "selection_inconclusive_software_failure"
        or failure.get("classification") != "target_order_replay_false_negative"
    ):
        raise ValueError("published 7.0 execution-failure receipt differs")
    return failure


def _verify_published_7_1_result(root: Path) -> dict[str, Any]:
    def git(*args: str) -> bytes:
        completed = subprocess.run(
            ["git", *args],
            cwd=root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode != 0:
            raise ValueError(
                "could not verify the local 7.1 archive: "
                + completed.stderr.decode("utf-8", errors="replace").strip()
            )
        return completed.stdout

    if (
        git("cat-file", "-t", "refs/tags/7.1").decode("ascii").strip() != "tag"
        or git("rev-parse", "refs/tags/7.1").decode("ascii").strip()
        != V71_TAG_OBJECT
        or git("rev-parse", "refs/tags/7.1^{}").decode("ascii").strip()
        != V71_TAG_COMMIT
    ):
        raise ValueError("local 7.1 annotated tag identity differs")
    paths = (
        V71_CLOSURE_PATH,
        V71_WINNER_FREEZE_PATH,
        V71_RESULT_PATH,
    )
    for relative in paths:
        if git("show", f"{V71_TAG_COMMIT}:{relative}") != (root / relative).read_bytes():
            raise ValueError(f"current 7.1 archive differs from its tag: {relative}")
    closure = _read_json(root / V71_CLOSURE_PATH)
    freeze = _read_json(root / V71_WINNER_FREEZE_PATH)
    result = _read_json(root / V71_RESULT_PATH)
    if (
        closure.get("payload_sha256")
        != "8cd80c7c770477cf29c2fa04348e9ed16f637f7d5ee61f31232d6f1f81ff2e55"
        or freeze.get("payload_sha256")
        != "451b7de8bbcba9372731b7dd7236e16a46467bdf5499eeff5e17e8e946ffabfd"
        or freeze.get("selected_candidate_id") is not None
        or result.get("payload_sha256")
        != "869b6f1fe028378e1071a416c7f8d045650a41c17c01bd9a1d48f62b35c3a4b9"
        or result.get("status") != "selection_falsified_no_candidate"
        or result.get("selected_candidate_id") is not None
        or any(
            value.get("payload_sha256") != _canonical_payload_sha256(value)
            for value in (closure, freeze, result)
        )
    ):
        raise ValueError("local 7.1 terminal archive differs")
    return result


def _v8_archive_git(
    root: Path, *args: str, check: bool = True
) -> tuple[int, bytes, bytes]:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and completed.returncode != 0:
        raise ValueError(
            "could not verify the 8.0 execution-failure archive: "
            + completed.stderr.decode("utf-8", errors="replace").strip()
        )
    return completed.returncode, completed.stdout, completed.stderr


def _verify_8_0_failure_archive(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], bool, bool]:
    failure_path = root / V8_FAILURE_PATH
    if failure_path.is_symlink() or not failure_path.is_file():
        raise ValueError("8.0 execution-failure receipt is missing or indirect")
    failure = _read_json(failure_path)
    if (
        set(failure)
        != {
            "schema_version",
            "kind",
            "release",
            "status",
            "classification",
            "calibration_execution",
            "frozen_inputs",
            "train_stage",
            "failure_boundary",
            "archive_contract",
            "payload_sha256",
        }
        or failure.get("schema_version") != 1
        or failure.get("kind") != "factor_lab_8_0_execution_failure"
        or failure.get("release") != "8.0"
        or failure.get("status") != "selection_inconclusive_execution_failure"
        or failure.get("classification")
        != "post_evaluation_github_ci_transport_failure"
        or failure.get("payload_sha256") != V8_FAILURE_PAYLOAD_SHA256
        or failure.get("payload_sha256") != _canonical_payload_sha256(failure)
        or _file_sha256(failure_path) != V8_FAILURE_FILE_SHA256
    ):
        raise ValueError("8.0 execution-failure receipt identity differs")

    execution = failure.get("calibration_execution") or {}
    if execution != {
        "command": "python scripts/run-multi-asset-evidence.py --mode calibration",
        "execution_commit": "644840a4967d69f6acc8903549705370bffdcba1",
        "phase": "train",
        "candidate_id": "static_risk_budget",
        "comparator_id": "cash_only_511880",
        "failure_function": (
            "run-multi-asset-evidence._require_head_pushed_and_ci_success"
        ),
        "exception_type": "RuntimeError",
        "exception_message": (
            "formal HEAD is not the current pushed origin/main commit"
        ),
        "external_transport_diagnostic": (
            "git ls-remote subsequently returned Empty reply from server"
        ),
        "canonical_failure_code": (
            "github_ci_empty_reply_after_train_evaluation"
        ),
    }:
        raise ValueError("8.0 calibration failure identity differs")

    protocol = _read_json(root / V8_PROTOCOL_PATH)
    selection = _read_json(root / V8_ASSET_SELECTION_PATH)
    disclosure = _read_json(root / V7_PRECLOSURE_TRAIN_PATH)
    closure = _read_json(root / V8_CLOSURE_PATH)
    expected_inputs = {
        "protocol": {
            "path": V8_PROTOCOL_PATH,
            "file_sha256": (
                "ac4a6f94cfbbe709c26120bad7499196fa36fc497f366cf445896cd486519abc"
            ),
            "payload_sha256": (
                "801374f58aa5edd66365e0937ed119082559f2950cc1106134a3cdb58e0099e7"
            ),
        },
        "asset_selection": {
            "path": V8_ASSET_SELECTION_PATH,
            "file_sha256": (
                "6d2d819db2579db76f8e7830a5de090d8d471c7fdc657abd8aba626cd1b065ec"
            ),
            "payload_sha256": (
                "b00536d618c7fe46e3cbe8d258d2b2032ef4e0c16d40fb9c74ff016c34525e0b"
            ),
        },
        "preclosure_train_disclosure": {
            "path": V7_PRECLOSURE_TRAIN_PATH,
            "file_sha256": (
                "01c3d97f7a3cce81bd8abe4e430c5b35b35deddefc80950403d3b40d109f7c09"
            ),
            "payload_sha256": (
                "6bd2909ddc97ec84d3535d15e8f13330a5752831aead82d8fb50afdd16ac6775"
            ),
        },
        "prevalidation_closure": {
            "path": V8_CLOSURE_PATH,
            "file_sha256": (
                "8e4fe890efb746c15ae5f0375d8a1dfd85a061172426165af1441d5011bfa97d"
            ),
            "payload_sha256": (
                "7bdd27bc6365c936c7e17736920d5fbf2556608e8b59b0869b3e70b9e61e5de7"
            ),
            "implementation_commit": (
                "beb4c56cb875e386b5742a7ca10fa634703dbc81"
            ),
        },
    }
    if failure.get("frozen_inputs") != expected_inputs:
        raise ValueError("8.0 execution failure frozen inputs differ")
    for value, binding in (
        (protocol, expected_inputs["protocol"]),
        (selection, expected_inputs["asset_selection"]),
        (disclosure, expected_inputs["preclosure_train_disclosure"]),
        (closure, expected_inputs["prevalidation_closure"]),
    ):
        path = root / str(binding["path"])
        if (
            path.is_symlink()
            or not path.is_file()
            or value.get("payload_sha256") != binding["payload_sha256"]
            or value.get("payload_sha256") != _canonical_payload_sha256(value)
            or _file_sha256(path) != binding["file_sha256"]
        ):
            raise ValueError(f"8.0 frozen input differs: {binding['path']}")

    boundary = failure.get("failure_boundary") or {}
    if (
        boundary.get("train_evaluation_persisted") is not True
        or boundary.get("train_phase_deep_verified") is not True
        or boundary.get("disclosed_static_replay_verified") is not True
        or boundary.get("second_github_ci_transport_check_passed") is not False
        or boundary.get("train_admission_created") is not False
        or boundary.get("validation_market_outcomes_opened") is not False
        or boundary.get("audit_market_outcomes_opened") is not False
        or boundary.get("terminal_result_created") is not False
        or boundary.get("profit_claim_allowed") is not False
    ):
        raise ValueError("8.0 execution-failure evidence boundary differs")
    observed = (failure.get("train_stage") or {}).get("observed_gate") or {}
    if (
        observed.get("gate_passed") is not False
        or observed.get("failed_checks")
        != ["requested_notional_fill_ratio_at_least"]
        or observed.get("observed_minimum_requested_notional_fill_ratio")
        != 0.8965780451229126
        or observed.get("primary_requested_notional_fill_ratio")
        != 0.9954253240120514
        or observed.get("stress_requested_notional_fill_ratio")
        != 0.9951486761513633
        or observed.get("would_have_admission_status")
        != "train_admission_failed"
    ):
        raise ValueError("8.0 observed failed gate differs")
    archive_contract = failure.get("archive_contract") or {}
    if (
        archive_contract.get("same_release_retry_allowed") is not False
        or archive_contract.get("normal_validation_audit_or_finalize_allowed")
        is not False
        or archive_contract.get("next_release") != "8.1"
        or archive_contract.get("direction_change") is not False
        or archive_contract.get("correction_chosen_after_observing_8_0_train_failure")
        is not True
    ):
        raise ValueError("8.0 archive/corrective boundary differs")

    normal_paths = (
        V8_TRAIN_ADMISSION_PATH,
        V8_WINNER_FREEZE_PATH,
        V8_AUDIT_PATH,
        V8_RESULT_PATH,
    )
    if any((root / relative).exists() or (root / relative).is_symlink() for relative in normal_paths):
        raise ValueError("8.0 failure receipt is mutually exclusive with normal evidence")
    evidence_root = root / V8_EVIDENCE_ROOT
    if (
        evidence_root.is_symlink()
        or not evidence_root.is_dir()
        or {path.name for path in evidence_root.iterdir()}
        != {Path(V8_FAILURE_PATH).name}
    ):
        raise ValueError("8.0 execution-failure evidence layout differs")

    execution_commit = str(execution["execution_commit"])
    implementation_commit = str(
        expected_inputs["prevalidation_closure"]["implementation_commit"]
    )
    _code, resolved, _stderr = _v8_archive_git(
        root, "rev-parse", "--verify", f"{execution_commit}^{{commit}}"
    )
    if resolved.decode("ascii").strip() != execution_commit:
        raise ValueError("8.0 calibration execution commit differs")
    code, _stdout, _stderr = _v8_archive_git(
        root,
        "merge-base",
        "--is-ancestor",
        execution_commit,
        "HEAD",
        check=False,
    )
    if code != 0:
        raise ValueError("8.0 calibration execution is not an ancestor of HEAD")
    for binding in expected_inputs.values():
        relative = str(binding["path"])
        _code, historical, _stderr = _v8_archive_git(
            root, "show", f"{execution_commit}:{relative}"
        )
        if historical != (root / relative).read_bytes():
            raise ValueError(f"8.0 execution commit lacks frozen input: {relative}")
    for relative in (*normal_paths, V8_FAILURE_PATH):
        code, _stdout, _stderr = _v8_archive_git(
            root,
            "cat-file",
            "-e",
            f"{execution_commit}:{relative}",
            check=False,
        )
        if code == 0:
            raise ValueError(f"8.0 evidence predates its execution: {relative}")
    implementation = closure.get("implementation") or {}
    if closure.get("implementation_commit") != implementation_commit or not isinstance(
        implementation, Mapping
    ):
        raise ValueError("8.0 closure implementation identity differs")
    for relative, binding in implementation.items():
        if not isinstance(binding, Mapping) or binding.get("path") != relative:
            raise ValueError(f"invalid 8.0 implementation binding: {relative}")
        _code, historical, _stderr = _v8_archive_git(
            root, "show", f"{implementation_commit}:{relative}"
        )
        if hashlib.sha256(historical).hexdigest() != binding.get("sha256"):
            raise ValueError(f"8.0 historical implementation differs: {relative}")

    tag_verified = False
    code, _stdout, _stderr = _v8_archive_git(
        root, "show-ref", "--verify", "--quiet", "refs/tags/8.0", check=False
    )
    if code == 0:
        _code, tag_type, _stderr = _v8_archive_git(
            root, "cat-file", "-t", "refs/tags/8.0"
        )
        _code, tag_object_raw, _stderr = _v8_archive_git(
            root, "rev-parse", "refs/tags/8.0"
        )
        _code, tag_commit_raw, _stderr = _v8_archive_git(
            root, "rev-parse", "refs/tags/8.0^{}"
        )
        tag_object = tag_object_raw.decode("ascii").strip()
        tag_commit = tag_commit_raw.decode("ascii").strip()
        if (
            tag_type.decode("ascii").strip() != "tag"
            or tag_object != V8_TAG_OBJECT
            or tag_commit != V8_TAG_COMMIT
        ):
            raise ValueError("local 8.0 annotated tag identity differs")
        remote_code, remote_raw, _stderr = _v8_archive_git(
            root,
            "ls-remote",
            "--exit-code",
            "origin",
            "refs/tags/8.0",
            "refs/tags/8.0^{}",
            check=False,
        )
        remote_refs = {
            ref: object_id
            for object_id, ref in (
                line.split() for line in remote_raw.decode("ascii").splitlines()
            )
        }
        if remote_code == 0 and (
            remote_refs.get("refs/tags/8.0") != V8_TAG_OBJECT
            or remote_refs.get("refs/tags/8.0^{}") != V8_TAG_COMMIT
        ):
            raise ValueError("local and GitHub 8.0 tag identities differ")
        code, _stdout, _stderr = _v8_archive_git(
            root,
            "merge-base",
            "--is-ancestor",
            execution_commit,
            tag_commit,
            check=False,
        )
        if code != 0:
            raise ValueError("8.0 tag does not descend from calibration execution")
        for relative in (
            V8_PROTOCOL_PATH,
            V8_ASSET_SELECTION_PATH,
            V7_PRECLOSURE_TRAIN_PATH,
            V8_CLOSURE_PATH,
            V8_FAILURE_PATH,
        ):
            _code, tagged, _stderr = _v8_archive_git(
                root, "show", f"{tag_commit}:{relative}"
            )
            if tagged != (root / relative).read_bytes():
                raise ValueError(f"current 8.0 archive differs from tag: {relative}")
        for relative in normal_paths:
            code, _stdout, _stderr = _v8_archive_git(
                root,
                "cat-file",
                "-e",
                f"{tag_commit}:{relative}",
                check=False,
            )
            if code == 0:
                raise ValueError(
                    f"published 8.0 tag contains normal evidence: {relative}"
                )
        tag_verified = True

    data_verified = False
    runtime_root = root / V8_RUNTIME_PATH
    if not tag_verified and not runtime_root.is_dir():
        raise ValueError(
            "8.0 train runtime must be retained until the GitHub tag is verified"
        )
    if (verify_data or not tag_verified) and runtime_root.exists():
        source_root = runtime_root / "sources"
        evaluation_root = runtime_root / "evaluations"
        binding_root = runtime_root / "stage-bindings"
        if (
            runtime_root.is_symlink()
            or {path.name for path in runtime_root.iterdir()}
            != {"sources", "evaluations", "stage-bindings"}
            or source_root.is_symlink()
            or {path.name for path in source_root.iterdir()} != {"stage=train"}
            or evaluation_root.is_symlink()
            or {path.name for path in evaluation_root.iterdir()} != {"stage=train"}
            or binding_root.is_symlink()
            or {path.name for path in binding_root.iterdir()} != {"train.json"}
        ):
            raise ValueError("8.0 archived runtime layout differs")
        train = failure["train_stage"]
        manifest_path = root / train["manifest"]["path"]
        binding_path = root / train["binding"]["path"]
        evaluation_path = root / train["evaluation"]["path"]
        manifest = _read_json(manifest_path)
        stage_binding = _read_json(binding_path)
        evaluation = _read_json(evaluation_path)
        for path, value, expected in (
            (manifest_path, manifest, train["manifest"]),
            (binding_path, stage_binding, train["binding"]),
            (evaluation_path, evaluation, train["evaluation"]),
        ):
            if (
                _file_sha256(path) != expected["file_sha256"]
                or value.get("payload_sha256") != expected["payload_sha256"]
                or value.get("payload_sha256") != _canonical_payload_sha256(value)
            ):
                raise ValueError(f"8.0 archived runtime identity differs: {path}")
        if (
            len(manifest.get("assets") or {}) != train["manifest"]["asset_count"]
            or sum(
                int(asset.get("row_count") or 0)
                for asset in (manifest.get("assets") or {}).values()
            )
            != train["manifest"]["asset_row_count"]
            or (manifest.get("calendar") or {}).get("row_count")
            != train["manifest"]["calendar_row_count"]
            or stage_binding.get("execution_commit") != execution_commit
            or stage_binding.get("run_nonce") != train["binding"]["run_nonce"]
            or evaluation.get("execution_commit") != execution_commit
            or evaluation.get("run_nonce") != train["evaluation"]["run_nonce"]
            or _canonical_payload_sha256(evaluation.get("metrics") or {})
            != train["evaluation"]["metrics_sha256"]
            or _canonical_payload_sha256(evaluation.get("gate") or {})
            != train["evaluation"]["gate_sha256"]
        ):
            raise ValueError("8.0 archived train contract differs")
        import pandas as pd

        source_stage = manifest_path.parent
        expected_source_files = {"manifest.json", "calendar.parquet"}
        expected_source_files.update(
            str(asset.get("path") or "")
            for asset in (manifest.get("assets") or {}).values()
        )
        if {path.name for path in source_stage.iterdir()} != expected_source_files:
            raise ValueError("8.0 archived source file set differs")
        calendar_binding = manifest.get("calendar") or {}
        calendar_path = source_stage / str(calendar_binding.get("path") or "")
        if (
            calendar_path.is_symlink()
            or not calendar_path.is_file()
            or calendar_path.stat().st_size != calendar_binding.get("size_bytes")
            or _file_sha256(calendar_path) != calendar_binding.get("file_sha256")
            or len(pd.read_parquet(calendar_path)) != calendar_binding.get("row_count")
        ):
            raise ValueError("8.0 archived calendar differs")
        for asset in (manifest.get("assets") or {}).values():
            asset_path = source_stage / str(asset.get("path") or "")
            if (
                asset_path.is_symlink()
                or not asset_path.is_file()
                or asset_path.stat().st_size != asset.get("size_bytes")
                or _file_sha256(asset_path) != asset.get("file_sha256")
                or len(pd.read_parquet(asset_path)) != asset.get("row_count")
            ):
                raise ValueError(f"8.0 archived source asset differs: {asset_path}")
        artifacts = evaluation.get("artifacts") or {}
        if set(artifacts) != set(train["evaluation"]["artifact_roles"]):
            raise ValueError("8.0 archived evaluation role set differs")
        phase = {
            "source_manifest_payload_sha256": evaluation[
                "source_manifest_payload_sha256"
            ],
            "stage_binding_payload_sha256": evaluation[
                "stage_binding_payload_sha256"
            ],
            "evaluation_payload_sha256": evaluation["payload_sha256"],
            "evaluation_file_sha256": _file_sha256(evaluation_path),
            "metrics": evaluation["metrics"],
            "gate": evaluation["gate"],
        }
        if _canonical_payload_sha256(phase) != train["evaluation"]["phase_sha256"]:
            raise ValueError("8.0 archived phase hash differs")
        evaluation_stage_root = evaluation_path.parent
        expected_evaluation_files = {"evaluation.json"}
        expected_evaluation_files.update(
            str(artifact.get("path") or "")
            for role_artifacts in artifacts.values()
            for artifact in role_artifacts.values()
        )
        if {path.name for path in evaluation_stage_root.iterdir()} != expected_evaluation_files:
            raise ValueError("8.0 archived evaluation file set differs")
        parquet_count = 0
        row_count = 0
        for role_artifacts in artifacts.values():
            for artifact in role_artifacts.values():
                artifact_path = evaluation_stage_root / str(
                    artifact.get("path") or ""
                )
                if (
                    artifact_path.is_symlink()
                    or not artifact_path.is_file()
                    or artifact_path.stat().st_size != artifact.get("size_bytes")
                    or _file_sha256(artifact_path) != artifact.get("file_sha256")
                ):
                    raise ValueError(f"8.0 archived artifact differs: {artifact_path}")
                rows = len(pd.read_parquet(artifact_path))
                if rows != artifact.get("rows"):
                    raise ValueError(f"8.0 archived artifact rows differ: {artifact_path}")
                parquet_count += 1
                row_count += rows
        if (
            parquet_count != train["evaluation"]["artifact_parquet_count"]
            or row_count != train["evaluation"]["artifact_row_count"]
        ):
            raise ValueError("8.0 archived artifact totals differ")
        data_verified = True
    elif runtime_root.is_symlink():
        raise ValueError("8.0 archived runtime must not be indirect")
    return failure, data_verified, tag_verified


def _strategy_status_8_0_archived_failure(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    checks: list[dict[str, Any]] = []
    try:
        clean = _working_tree_is_clean(root)
        if not clean:
            raise RuntimeError("8.0 archive status requires a clean worktree")
        failure, data_verified, tag_verified = _verify_8_0_failure_archive(
            root, verify_data=verify_data
        )
        checks.append(
            {
                "category": "execution_failure_archive",
                "path": V8_FAILURE_PATH,
                "status": "match",
                "payload_sha256": failure["payload_sha256"],
            }
        )
        if tag_verified:
            checks.append(
                {
                    "category": "local_archived_annotated_tag",
                    "path": "refs/tags/8.0",
                    "status": "match",
                }
            )
        else:
            head = _v8_require_head_ci(root)
            checks.append(
                {
                    "category": "archive_head_push_ci",
                    "path": ".git",
                    "status": "match",
                    "head": head,
                }
            )
        checks.append(
            {
                "category": "canonical_stage_artifacts",
                "path": V8_RUNTIME_PATH,
                "status": (
                    "match"
                    if data_verified
                    else "not_retained"
                    if not (root / V8_RUNTIME_PATH).exists()
                    else "not_verified"
                ),
            }
        )
        return {
            "status": failure["status"],
            "version": "8.0",
            "route": "strategic_static_capital_budget_beta",
            "protocol_id": (
                "factor-lab/8.0/strategic-static-capital-budget-beta-v1"
            ),
            "historical_evidence_class": (
                "exposed_train_execution_failure_diagnostic"
            ),
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "observed_train_gate_passed": False,
            "audit_status": "not_opened",
            "terminal_result_payload_sha256": None,
            "execution_failure_payload_sha256": failure["payload_sha256"],
            "canonical_data_hashes_verified": data_verified,
            "checks": checks,
        }, 0
    except (
        AttributeError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.SubprocessError,
    ) as exc:
        checks.append(
            {
                "category": "execution_failure_archive",
                "path": V8_FAILURE_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
        )
        return {
            "status": "integrity_mismatch",
            "version": "8.0",
            "route": "strategic_static_capital_budget_beta",
            "protocol_id": None,
            "historical_evidence_class": None,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "observed_train_gate_passed": None,
            "audit_status": "unknown",
            "terminal_result_payload_sha256": None,
            "execution_failure_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3


def _strategy_status_8_0_pending(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    checks: list[dict[str, Any]] = []
    protocol, protocol_check = _v7_json_check(
        root,
        V8_PROTOCOL_PATH,
        expected_payload="801374f58aa5edd66365e0937ed119082559f2950cc1106134a3cdb58e0099e7",
        expected_file="ac4a6f94cfbbe709c26120bad7499196fa36fc497f366cf445896cd486519abc",
    )
    protocol_check["category"] = "protocol_payload"
    checks.append(protocol_check)
    selection, selection_check = _v7_json_check(root, V8_ASSET_SELECTION_PATH)
    selection_check["category"] = "asset_selection_payload"
    checks.append(selection_check)
    _inherited, inherited_check = _v7_json_check(
        root,
        V7_PROTOCOL_PATH,
        expected_payload="6f2fcd2a67d52bfae19bedcaecf495faa986195f6840da48a3a67a666589aaf0",
        expected_file="2d2e96a1605b5e088a7cf5952dd816d8aecb10e39b9ba529fe81b00592bfa14f",
    )
    inherited_check["category"] = "inherited_data_execution_protocol"
    checks.append(inherited_check)
    for relative, payload, file_hash, category in (
        (
            V71_CLOSURE_PATH,
            "8cd80c7c770477cf29c2fa04348e9ed16f637f7d5ee61f31232d6f1f81ff2e55",
            "794b11d55cfbdf1f33e5e15c917691b76f244a9fd5f8f400a5f862d7830f11cd",
            "published_7_1_closure",
        ),
        (
            V71_WINNER_FREEZE_PATH,
            "451b7de8bbcba9372731b7dd7236e16a46467bdf5499eeff5e17e8e946ffabfd",
            "2b239ac699d80db0965d87f1fb96a366b7a2f820c173fa08988fb4801323fa77",
            "published_7_1_freeze",
        ),
        (
            V71_RESULT_PATH,
            "869b6f1fe028378e1071a416c7f8d045650a41c17c01bd9a1d48f62b35c3a4b9",
            "ff0278104d1e7fd5f940671322e1987ea416bb4eeb7b3a343ec814393053449a",
            "published_7_1_result",
        ),
    ):
        _value, check = _v7_json_check(
            root, relative, expected_payload=payload, expected_file=file_hash
        )
        check["category"] = category
        checks.append(check)
    failures = [item for item in checks if item.get("status") != "match"]
    if protocol is not None and selection is not None:
        valid = (
            protocol.get("release") == "8.0"
            and protocol.get("direction_change") is True
            and protocol.get("route") == "strategic_static_capital_budget_beta"
            and protocol.get("strategy_registry", [{}])[0].get("strategy_id")
            == "static_risk_budget"
            and protocol.get("cash_comparator", {}).get("comparator_id")
            == "cash_only_511880"
            and protocol.get("prior_train_exposure", {}).get(
                "static_control_metrics_sha256"
            )
            == "fb1b146e34d62486dfd2c7ff39102ca7418419260f7eda99b11b6c2768c12492"
            and selection.get("selected_codes")
            == [
                "510300.SH",
                "159920.SZ",
                "513100.SH",
                "518880.SH",
                "511010.SH",
                "511880.SH",
            ]
        )
        item = {
            "category": "strategic_policy_contract",
            "path": V8_PROTOCOL_PATH,
            "status": "match" if valid else "mismatch",
        }
        checks.append(item)
        if not valid:
            failures.append(item)
    try:
        _verify_published_7_1_result(root)
        checks.append(
            {
                "category": "local_7_1_tag_binding",
                "path": "refs/tags/7.1",
                "status": "match",
            }
        )
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as exc:
        item = {
            "category": "local_7_1_tag_binding",
            "path": "refs/tags/7.1",
            "status": "mismatch",
            "error": str(exc),
        }
        checks.append(item)
        failures.append(item)
    for relative in (
        V8_CLOSURE_PATH,
        V8_EVIDENCE_ROOT,
        V8_TRAIN_ADMISSION_PATH,
        V8_WINNER_FREEZE_PATH,
        V8_AUDIT_PATH,
        V8_RESULT_PATH,
        V8_RUNTIME_PATH,
    ):
        path = root / relative
        exists = path.exists() or path.is_symlink()
        item = {
            "category": "preclosure_absence",
            "path": relative,
            "status": "unexpected" if exists else "match",
        }
        checks.append(item)
        if exists:
            failures.append(item)
    clean = _working_tree_is_clean(root)
    checks.append(
        {
            "category": "preclosure_working_tree",
            "path": ".",
            "status": "match" if clean else "pending_clean_commit",
        }
    )
    if clean:
        try:
            head = _v8_require_head_ci(root)
            checks.append(
                {
                    "category": "preclosure_head_push_ci",
                    "path": ".git",
                    "status": "match",
                    "head": head,
                }
            )
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            item = {
                "category": "preclosure_head_push_ci",
                "path": ".git",
                "status": "mismatch",
                "error": str(exc),
            }
            checks.append(item)
            failures.append(item)
    status, exit_code = (
        ("integrity_mismatch", 3)
        if failures
        else ("implementation_ready_for_prevalidation_closure", 0)
        if clean
        else ("implementation_pending_clean_commit", 2)
    )
    claim = protocol.get("claim_contract") if protocol is not None else {}
    return {
        "status": status,
        "version": "8.0",
        "route": "strategic_static_capital_budget_beta",
        "protocol_id": protocol.get("protocol_id") if protocol is not None else None,
        "historical_evidence_class": (
            claim.get("historical_pass_interpretation")
            if isinstance(claim, Mapping)
            else None
        ),
        "profit_claim_allowed": False,
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "terminal_result_payload_sha256": None,
        "canonical_data_hashes_verified": False,
        "checks": checks,
    }, exit_code


def _strategy_status_8_0(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    if (root / V8_FAILURE_PATH).is_file():
        return _strategy_status_8_0_archived_failure(
            root, verify_data=verify_data
        )
    if not (root / V8_CLOSURE_PATH).is_file():
        return _strategy_status_8_0_pending(root, verify_data=verify_data)
    checks: list[dict[str, Any]] = []
    try:
        verifier = _load_v8_runner(root)
        state = verifier.verify_release_state(
            verify_data=verify_data,
            verify_runtime=False,
        )
        closure = state["closure"]
        protocol = state["protocol"]
        freeze = state["freeze"]
        audit = state["audit"]
        result = state["result"]
        phase_evidence_present = bool(
            freeze is not None or state.get("train_admission") is not None
        )
        data_verified = bool(verify_data and phase_evidence_present)
        checks.append(
            {
                "category": "release_evidence_chain",
                "path": V8_CLOSURE_PATH,
                "status": "match",
            }
        )
        checks.append(
            {
                "category": "canonical_stage_artifacts",
                "path": V8_RUNTIME_PATH,
                "status": (
                    "match"
                    if data_verified
                    else "not_applicable"
                    if not phase_evidence_present
                    else "not_verified"
                ),
            }
        )
        clean = _working_tree_is_clean(root)
        checks.append(
            {
                "category": "working_tree",
                "path": ".",
                "status": "match" if clean else "mismatch",
            }
        )
        if not clean:
            raise RuntimeError("8.0 formal status requires a clean worktree")
        head = _v8_require_head_ci(root)
        checks.append(
            {
                "category": "head_push_ci",
                "path": ".git",
                "status": "match",
                "head": head,
            }
        )
        selected = (
            result.get("selected_candidate_id")
            if result is not None
            else freeze.get("selected_candidate_id")
            if freeze is not None
            else None
        )
        audit_status = (
            result.get("audit_status")
            if result is not None
            else audit.get("status")
            if audit is not None
            else "not_opened"
        )
        claim = protocol.get("claim_contract") or {}
        return {
            "status": state["status"],
            "version": "8.0",
            "route": closure.get("route"),
            "protocol_id": protocol.get("protocol_id"),
            "historical_evidence_class": claim.get("historical_pass_interpretation"),
            "profit_claim_allowed": claim.get("profit_claim_allowed", False),
            "selected_candidate_id": selected,
            "audit_status": audit_status,
            "terminal_result_payload_sha256": (
                result.get("payload_sha256") if result is not None else None
            ),
            "canonical_data_hashes_verified": data_verified,
            "checks": checks,
        }, 0
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as exc:
        checks.append(
            {
                "category": "release_evidence_chain",
                "path": V8_CLOSURE_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
        )
        return {
            "status": "integrity_mismatch",
            "version": "8.0",
            "route": "strategic_static_capital_budget_beta",
            "protocol_id": None,
            "historical_evidence_class": None,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "audit_status": "unknown",
            "terminal_result_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3


def _verify_published_8_1_archive(root: Path) -> dict[str, Any]:
    def git(*args: str, check: bool = True) -> bytes:
        completed = subprocess.run(
            ["git", *args],
            cwd=root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if check and completed.returncode != 0:
            raise ValueError(
                "could not verify the published 8.1 archive: "
                + completed.stderr.decode("utf-8", errors="replace").strip()
            )
        return completed.stdout

    if (
        git("cat-file", "-t", "refs/tags/8.1").decode("ascii").strip() != "tag"
        or git("rev-parse", "refs/tags/8.1").decode("ascii").strip()
        != V81_TAG_OBJECT
        or git("rev-parse", "refs/tags/8.1^{}").decode("ascii").strip()
        != V81_TAG_COMMIT
    ):
        raise ValueError("local 8.1 annotated tag identity differs")
    remote_result = subprocess.run(
        [
            "git",
            "ls-remote",
            "--exit-code",
            "origin",
            "refs/tags/8.1",
            "refs/tags/8.1^{}",
        ],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if remote_result.returncode == 0:
        remote = {
            ref: object_id
            for object_id, ref in (
                line.split()
                for line in remote_result.stdout.decode("ascii").splitlines()
            )
        }
        if remote != {
            "refs/tags/8.1": V81_TAG_OBJECT,
            "refs/tags/8.1^{}": V81_TAG_COMMIT,
        }:
            raise ValueError("local and GitHub 8.1 tag identities differ")
    specs = (
        (V81_PROTOCOL_PATH, V81_PROTOCOL_FILE_SHA256, V81_PROTOCOL_PAYLOAD_SHA256),
        (V81_CLOSURE_PATH, V81_CLOSURE_FILE_SHA256, V81_CLOSURE_PAYLOAD_SHA256),
        (
            V81_RECLASSIFICATION_PATH,
            V81_RECLASSIFICATION_FILE_SHA256,
            V81_RECLASSIFICATION_PAYLOAD_SHA256,
        ),
        (V81_WINNER_FREEZE_PATH, V81_FREEZE_FILE_SHA256, V81_FREEZE_PAYLOAD_SHA256),
        (V81_RESULT_PATH, V81_RESULT_FILE_SHA256, V81_RESULT_PAYLOAD_SHA256),
    )
    values: dict[str, dict[str, Any]] = {}
    for relative, expected_file, expected_payload in specs:
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"published 8.1 archive file is missing or indirect: {relative}")
        working = path.read_bytes()
        tagged = git("show", f"{V81_TAG_COMMIT}:{relative}")
        value = json.loads(working.decode("utf-8"))
        if (
            working != tagged
            or _file_sha256(path) != expected_file
            or value.get("payload_sha256") != expected_payload
            or _canonical_payload_sha256(value) != expected_payload
        ):
            raise ValueError(f"published 8.1 archive differs: {relative}")
        values[relative] = value
    protocol = values[V81_PROTOCOL_PATH]
    closure = values[V81_CLOSURE_PATH]
    reclassification = values[V81_RECLASSIFICATION_PATH]
    freeze = values[V81_WINNER_FREEZE_PATH]
    result = values[V81_RESULT_PATH]
    audit_in_tag = subprocess.run(
        ["git", "cat-file", "-e", f"{V81_TAG_COMMIT}:{V81_AUDIT_PATH}"],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode == 0
    if (
        protocol.get("release") != "8.1"
        or protocol.get("protocol_id") != V81_PROTOCOL_ID
        or closure.get("release") != "8.1"
        or reclassification.get("status") != "train_reclassification_passed"
        or freeze.get("status") != "selected_null_frozen_validation_failed"
        or freeze.get("selected_candidate_id") is not None
        or freeze.get("validation_market_outcomes_opened") is not True
        or freeze.get("audit_market_outcomes_opened") is not False
        or result.get("status") != "selection_falsified_no_candidate"
        or result.get("selected_candidate_id") is not None
        or result.get("audit_status") != "not_opened"
        or (root / V81_AUDIT_PATH).exists()
        or audit_in_tag
    ):
        raise ValueError("published 8.1 terminal null archive differs")
    return {
        "protocol": protocol,
        "closure": closure,
        "train_reclassification": reclassification,
        "freeze": freeze,
        "result": result,
    }


def _strategy_status_8_1_archived(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    del verify_data
    checks: list[dict[str, Any]] = []
    try:
        if not _working_tree_is_clean(root):
            raise RuntimeError("8.1 archive status requires a clean worktree")
        archive = _verify_published_8_1_archive(root)
        checks.extend(
            [
                {
                    "category": "local_archived_annotated_tag",
                    "path": "refs/tags/8.1",
                    "status": "match",
                    "tag_object": V81_TAG_OBJECT,
                    "peeled_commit": V81_TAG_COMMIT,
                },
                {
                    "category": "terminal_result",
                    "path": V81_RESULT_PATH,
                    "status": "match",
                    "payload_sha256": archive["result"]["payload_sha256"],
                },
            ]
        )
        return {
            "status": "selection_falsified_no_candidate",
            "version": "8.1",
            "route": V81_ROUTE,
            "protocol_id": V81_PROTOCOL_ID,
            "historical_evidence_class": "post_hoc_train_reclassification_then_public_validation",
            "post_hoc_reclassification": True,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "train_reclassification_status": "train_reclassification_passed",
            "winner_freeze_status": "selected_null_frozen_validation_failed",
            "audit_status": "not_opened",
            "terminal_result_status": "selection_falsified_no_candidate",
            "terminal_result_payload_sha256": archive["result"]["payload_sha256"],
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 0
    except (
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.SubprocessError,
    ) as exc:
        checks.append(
            {
                "category": "published_8_1_archive",
                "path": "refs/tags/8.1",
                "status": "mismatch",
                "error": str(exc),
            }
        )
        return {
            "status": "integrity_mismatch",
            "version": "8.1",
            "route": V81_ROUTE,
            "protocol_id": None,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "audit_status": "unknown",
            "terminal_result_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3


def _strategy_status_9_0_pending(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    del verify_data  # Prior 8.1 readiness is always deep before the 9.0 closure.
    checks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    runner: Any | None = None
    try:
        runner = _load_v9_runner(root)
        checks.append(
            {
                "category": "formal_runner_namespace",
                "path": "scripts/run-multi-asset-evidence.py",
                "status": "match",
            }
        )
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        item = {
            "category": "formal_runner_namespace",
            "path": "scripts/run-multi-asset-evidence.py",
            "status": "mismatch",
            "error": str(exc),
        }
        checks.append(item)
        failures.append(item)
    protocol, protocol_check = _v7_json_check(
        root,
        V9_PROTOCOL_PATH,
        expected_payload=V9_PROTOCOL_PAYLOAD_SHA256,
        expected_file=V9_PROTOCOL_FILE_SHA256,
    )
    protocol_check["category"] = "protocol_payload"
    checks.append(protocol_check)
    if protocol_check.get("status") != "match":
        failures.append(protocol_check)
    scout, scout_check = _v7_json_check(
        root,
        V9_SCOUT_PATH,
        expected_payload=V9_SCOUT_PAYLOAD_SHA256,
        expected_file=V9_SCOUT_FILE_SHA256,
    )
    scout_check["category"] = "preprotocol_scout_payload"
    checks.append(scout_check)
    if scout_check.get("status") != "match":
        failures.append(scout_check)
    if protocol is not None and scout is not None:
        valid = (
            protocol.get("release") == "9.0"
            and protocol.get("direction_change") is True
            and protocol.get("route") == V9_ROUTE
            and protocol.get("strategy_id") == V9_ROUTE
            and protocol.get("protocol_id") == V9_PROTOCOL_ID
            and protocol.get("development_disclosure", {}).get(
                "development_is_independent_oos"
            )
            is False
            and protocol.get("physical_phases", {}).get("audit", {}).get(
                "market_outcome_opened"
            )
            is False
            and protocol.get("preprotocol_scout", {}).get("payload_sha256")
            == scout.get("payload_sha256")
            and scout.get("status")
            == "selected_volatility_balanced_after_fully_exposed_development"
            and (protocol.get("claim_contract") or {}).get("profit_claim_allowed")
            is False
        )
        item = {
            "category": "causal_volatility_balanced_contract",
            "path": V9_PROTOCOL_PATH,
            "status": "match" if valid else "mismatch",
        }
        checks.append(item)
        if not valid:
            failures.append(item)
    try:
        archive = _verify_published_8_1_archive(root)
        if runner is None:
            raise ValueError("9.0 runner unavailable for retained 8.1 readiness")
        readiness = runner._verify_prior_8_1_archive(
            verify_data=True, verify_runtime=True
        )
        if (
            readiness.get("status") != "selection_falsified_no_candidate"
            or readiness.get("deep_data_verified") is not True
            or readiness.get("deep_runtime_verified") is not True
            or readiness.get("artifact_parquet_count") != 20
            or readiness.get("artifact_row_count") != 62654
        ):
            raise ValueError("retained 8.1 archive readiness differs")
        checks.extend(
            [
                {
                    "category": "published_8_1_archive",
                    "path": "refs/tags/8.1",
                    "status": "match",
                    "terminal_payload_sha256": archive["result"]["payload_sha256"],
                },
                {
                    "category": "retained_8_1_development_readiness",
                    "path": V81_RUNTIME_PATH,
                    "status": "match",
                    "archive_identity_sha256": readiness[
                        "archive_identity_sha256"
                    ],
                    "artifact_parquet_count": 20,
                    "artifact_row_count": 62654,
                },
            ]
        )
    except (
        AttributeError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.SubprocessError,
    ) as exc:
        item = {
            "category": "retained_8_1_development_readiness",
            "path": V81_RUNTIME_PATH,
            "status": "mismatch",
            "error": str(exc),
        }
        checks.append(item)
        failures.append(item)
    for relative in (V9_CLOSURE_PATH, V9_EVIDENCE_ROOT, V9_RUNTIME_PATH):
        path = root / relative
        exists = path.exists() or path.is_symlink()
        item = {
            "category": "preclosure_absence",
            "path": relative,
            "status": "unexpected" if exists else "match",
        }
        checks.append(item)
        if exists:
            failures.append(item)
    clean = _working_tree_is_clean(root)
    checks.append(
        {
            "category": "preclosure_working_tree",
            "path": ".",
            "status": "match" if clean else "pending_clean_commit",
        }
    )
    if clean and not failures:
        try:
            head = _v9_require_head_ci(root)
            checks.append(
                {
                    "category": "preclosure_head_push_ci",
                    "path": ".git",
                    "status": "match",
                    "head": head,
                }
            )
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            item = {
                "category": "preclosure_head_push_ci",
                "path": ".git",
                "status": "mismatch",
                "error": str(exc),
            }
            checks.append(item)
            failures.append(item)
    status, exit_code = (
        ("integrity_mismatch", 3)
        if failures
        else ("implementation_ready_for_preselection_closure", 0)
        if clean
        else ("implementation_pending_clean_commit", 2)
    )
    return {
        "status": status,
        "version": "9.0",
        "route": V9_ROUTE,
        "protocol_id": protocol.get("protocol_id") if protocol else None,
        "development_evidence_class": "fully_exposed_post_selection_non_oos",
        "profit_claim_allowed": False,
        "selected_candidate_id": None,
        "winner_freeze_status": "not_created",
        "audit_status": "not_opened",
        "terminal_result_status": "not_created",
        "terminal_result_payload_sha256": None,
        "canonical_data_hashes_verified": False,
        "checks": checks,
    }, exit_code


def _strategy_status_9_0(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    if not (root / V9_CLOSURE_PATH).is_file():
        return _strategy_status_9_0_pending(root, verify_data=verify_data)
    checks: list[dict[str, Any]] = []
    try:
        verifier = _load_v9_runner(root)
        state = verifier.verify_release_state(
            verify_data=verify_data, verify_runtime=False
        )
        closure = state["closure"]
        protocol = state["protocol"]
        freeze = state["freeze"]
        audit = state["audit"]
        result = state["result"]
        data_verified = bool(verify_data and freeze is not None)
        checks.extend(
            [
                {
                    "category": "release_evidence_chain",
                    "path": V9_CLOSURE_PATH,
                    "status": "match",
                },
                {
                    "category": "canonical_stage_artifacts",
                    "path": V9_RUNTIME_PATH,
                    "status": (
                        "match"
                        if data_verified
                        else "not_applicable"
                        if freeze is None
                        else "not_verified"
                    ),
                },
            ]
        )
        if not _working_tree_is_clean(root):
            raise RuntimeError("9.0 formal status requires a clean worktree")
        head = _v9_require_head_ci(root)
        checks.append(
            {
                "category": "head_push_ci",
                "path": ".git",
                "status": "match",
                "head": head,
            }
        )
        selected = (
            result.get("selected_candidate_id")
            if result is not None
            else freeze.get("selected_candidate_id")
            if freeze is not None
            else None
        )
        return {
            "status": state["status"],
            "version": "9.0",
            "route": closure.get("route", V9_ROUTE),
            "protocol_id": protocol.get("protocol_id"),
            "development_evidence_class": "fully_exposed_post_selection_non_oos",
            "profit_claim_allowed": False,
            "selected_candidate_id": selected,
            "winner_freeze_status": (
                freeze.get("status") if freeze is not None else "not_created"
            ),
            "winner_freeze_payload_sha256": (
                freeze.get("payload_sha256") if freeze is not None else None
            ),
            "audit_status": (
                result.get("audit_status")
                if result is not None
                else audit.get("status")
                if audit is not None
                else "not_opened"
            ),
            "historical_audit_payload_sha256": (
                audit.get("payload_sha256") if audit is not None else None
            ),
            "terminal_result_status": (
                result.get("status") if result is not None else "not_created"
            ),
            "terminal_result_payload_sha256": (
                result.get("payload_sha256") if result is not None else None
            ),
            "canonical_data_hashes_verified": data_verified,
            "checks": checks,
        }, 0
    except (
        AttributeError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.SubprocessError,
    ) as exc:
        checks.append(
            {
                "category": "release_evidence_chain",
                "path": V9_CLOSURE_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
        )
        return {
            "status": "integrity_mismatch",
            "version": "9.0",
            "route": V9_ROUTE,
            "protocol_id": None,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "winner_freeze_status": "unknown",
            "audit_status": "unknown",
            "terminal_result_status": "unknown",
            "terminal_result_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3


def _strategy_status_9_0_archived(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    del verify_data
    checks: list[dict[str, Any]] = []
    try:
        if not _working_tree_is_clean(root):
            raise RuntimeError("9.0 archive status requires a clean worktree")

        def git(*args: str) -> bytes:
            completed = subprocess.run(
                ["git", *args],
                cwd=root,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if completed.returncode != 0:
                raise ValueError(
                    "could not verify the 9.0 archive: "
                    + completed.stderr.decode("utf-8", errors="replace").strip()
                )
            return completed.stdout

        if (
            git("cat-file", "-t", "refs/tags/9.0").decode("ascii").strip()
            != "tag"
            or git("rev-parse", "refs/tags/9.0").decode("ascii").strip()
            != V9_TAG_OBJECT
            or git("rev-parse", "refs/tags/9.0^{}").decode("ascii").strip()
            != V9_TAG_COMMIT
        ):
            raise ValueError("local 9.0 annotated tag identity differs")
        specs = (
            (V9_PROTOCOL_PATH, V9_PROTOCOL_FILE_SHA256, V9_PROTOCOL_PAYLOAD_SHA256),
            (V9_SCOUT_PATH, V9_SCOUT_FILE_SHA256, V9_SCOUT_PAYLOAD_SHA256),
            (V9_CLOSURE_PATH, V9_CLOSURE_FILE_SHA256, V9_CLOSURE_PAYLOAD_SHA256),
            (V9_WINNER_FREEZE_PATH, V9_FREEZE_FILE_SHA256, V9_FREEZE_PAYLOAD_SHA256),
            (V9_AUDIT_PATH, V9_AUDIT_FILE_SHA256, V9_AUDIT_PAYLOAD_SHA256),
            (V9_RESULT_PATH, V9_RESULT_FILE_SHA256, V9_RESULT_PAYLOAD_SHA256),
        )
        values: dict[str, dict[str, Any]] = {}
        for relative, expected_file, expected_payload in specs:
            path = root / relative
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"published 9.0 archive is missing or indirect: {relative}")
            working = path.read_bytes()
            value = json.loads(working.decode("utf-8"))
            if (
                working != git("show", f"{V9_TAG_COMMIT}:{relative}")
                or _file_sha256(path) != expected_file
                or value.get("payload_sha256") != expected_payload
                or _canonical_payload_sha256(value) != expected_payload
            ):
                raise ValueError(f"published 9.0 archive differs: {relative}")
            values[relative] = value
        freeze = values[V9_WINNER_FREEZE_PATH]
        audit = values[V9_AUDIT_PATH]
        result = values[V9_RESULT_PATH]
        if (
            freeze.get("selected_candidate_id") != V9_ROUTE
            or audit.get("status") != "historical_audit_passed"
            or result.get("status")
            != "historical_adaptive_beta_diagnostic_passed_fresh_evidence_required"
            or result.get("selected_candidate_id") != V9_ROUTE
            or result.get("audit_status") != "historical_audit_passed"
        ):
            raise ValueError("published 9.0 terminal boundary differs")
        checks.extend(
            [
                {
                    "category": "release_evidence_chain",
                    "path": V9_RESULT_PATH,
                    "status": "match",
                },
                {
                    "category": "canonical_stage_artifacts",
                    "path": V9_RUNTIME_PATH,
                    "status": "not_verified",
                },
                {
                    "category": "local_archived_annotated_tag",
                    "path": "refs/tags/9.0",
                    "status": "match",
                    "tag_object": V9_TAG_OBJECT,
                    "peeled_commit": V9_TAG_COMMIT,
                },
            ]
        )
        return {
            "status": result["status"],
            "version": "9.0",
            "route": V9_ROUTE,
            "protocol_id": V9_PROTOCOL_ID,
            "development_evidence_class": "fully_exposed_post_selection_non_oos",
            "profit_claim_allowed": False,
            "selected_candidate_id": V9_ROUTE,
            "winner_freeze_status": freeze["status"],
            "winner_freeze_payload_sha256": freeze["payload_sha256"],
            "audit_status": audit["status"],
            "historical_audit_payload_sha256": audit["payload_sha256"],
            "terminal_result_status": result["status"],
            "terminal_result_payload_sha256": result["payload_sha256"],
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 0
    except (
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        subprocess.SubprocessError,
    ) as exc:
        checks.append(
            {
                "category": "release_evidence_chain",
                "path": V9_RESULT_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
        )
        return {
            "status": "integrity_mismatch",
            "version": "9.0",
            "route": V9_ROUTE,
            "protocol_id": None,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "audit_status": "unknown",
            "terminal_result_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3


def _strategy_status_10_0(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    checks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    runner: Any | None = None
    try:
        runner = _load_v10_runner(root)
        checks.append(
            {
                "category": "results_first_runner_namespace",
                "path": V10_RUNNER_PATH,
                "status": "match",
            }
        )
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        item = {
            "category": "results_first_runner_namespace",
            "path": V10_RUNNER_PATH,
            "status": "mismatch",
            "error": str(exc),
        }
        checks.append(item)
        failures.append(item)

    protocol, protocol_check = _v7_json_check(
        root,
        V10_PROTOCOL_PATH,
        expected_payload=V10_PROTOCOL_PAYLOAD_SHA256,
        expected_file=V10_PROTOCOL_FILE_SHA256,
    )
    protocol_check["category"] = "results_first_protocol"
    checks.append(protocol_check)
    if protocol_check.get("status") != "match":
        failures.append(protocol_check)
    elif not (
        protocol.get("release") == "10.0"
        and protocol.get("route") == V10_ROUTE
        and protocol.get("protocol_id") == V10_PROTOCOL_ID
        and protocol.get("frozen_strategy", {}).get("strategy_id") == V10_ROUTE
        and protocol.get("claim_contract", {}).get("profit_claim_allowed") is False
    ):
        item = {
            "category": "results_first_protocol_contract",
            "path": V10_PROTOCOL_PATH,
            "status": "mismatch",
        }
        checks.append(item)
        failures.append(item)

    evidence_path = root / V10_EVIDENCE_PATH
    if not evidence_path.is_file():
        checks.append(
            {
                "category": "results_first_evidence",
                "path": V10_EVIDENCE_PATH,
                "status": "not_created",
            }
        )
        return {
            "status": "implementation_pending_results_first_replay",
            "version": "10.0",
            "route": V10_ROUTE,
            "protocol_id": V10_PROTOCOL_ID if protocol is not None else None,
            "evidence_class": "fully_exposed_results_first_causal_historical_diagnostic",
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "evidence_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3 if failures else 2

    evidence, evidence_check = _v7_json_check(root, V10_EVIDENCE_PATH)
    evidence_check["category"] = "results_first_evidence"
    checks.append(evidence_check)
    if evidence_check.get("status") != "match":
        failures.append(evidence_check)
    data_verified = False
    if evidence is not None and protocol is not None and runner is not None:
        try:
            runner.verify_evidence(evidence, verify_data=verify_data)
            data_verified = bool(verify_data)
            checks.append(
                {
                    "category": "results_first_evidence_contract",
                    "path": V10_EVIDENCE_PATH,
                    "status": "match",
                }
            )
        except (
            AttributeError,
            KeyError,
            OSError,
            OverflowError,
            RuntimeError,
            TypeError,
            ValueError,
            subprocess.SubprocessError,
        ) as exc:
            item = {
                "category": "results_first_evidence_contract",
                "path": V10_EVIDENCE_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
            checks.append(item)
            failures.append(item)

    clean = _working_tree_is_clean(root)
    if not clean:
        item = {
            "category": "results_first_worktree",
            "path": ".",
            "status": "mismatch",
            "error": "10.0 evidence status requires a clean worktree",
        }
        checks.append(item)
        failures.append(item)
    else:
        committed = subprocess.run(
            ["git", "show", f"HEAD:{V10_EVIDENCE_PATH}"],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        exact = committed.returncode == 0 and committed.stdout == evidence_path.read_bytes()
        item = {
            "category": "results_first_committed_evidence",
            "path": V10_EVIDENCE_PATH,
            "status": "match" if exact else "mismatch",
        }
        checks.append(item)
        if not exact:
            failures.append(item)

    if data_verified:
        checks.append(
            {
                "category": "retained_9_0_source_and_metric_replay",
                "path": V10_SOURCE_MANIFEST_PATH,
                "status": "match",
            }
        )

    if failures or evidence is None:
        return {
            "status": "integrity_mismatch",
            "version": "10.0",
            "route": V10_ROUTE,
            "protocol_id": V10_PROTOCOL_ID,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "evidence_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3
    return {
        "status": evidence["status"],
        "version": "10.0",
        "route": V10_ROUTE,
        "protocol_id": V10_PROTOCOL_ID,
        "evidence_class": evidence["evidence_class"],
        "profit_claim_allowed": False,
        "selected_candidate_id": evidence["selection"]["selected_candidate_id"],
        "evidence_payload_sha256": evidence["payload_sha256"],
        "full_cagr": evidence["periods"]["full"]["metrics"]["candidate"]["cagr"],
        "full_static_cagr": evidence["periods"]["full"]["metrics"]["static"]["cagr"],
        "full_max_drawdown": evidence["periods"]["full"]["metrics"]["candidate"]["max_drawdown"],
        "canonical_data_hashes_verified": data_verified,
        "checks": checks,
    }, 0


def _strategy_status_7_0_archived(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    checks: list[dict[str, Any]] = []
    try:
        failure = _verify_published_7_0_failure(root)
        checks.extend(
            [
                {
                    "category": "local_archived_annotated_tag",
                    "path": "refs/tags/7.0",
                    "status": "match",
                    "tag_object": V7_TAG_OBJECT,
                    "peeled_commit": V7_TAG_COMMIT,
                },
                {
                    "category": "selection_execution_failure",
                    "path": V7_FAILURE_PATH,
                    "status": "match",
                    "payload_sha256": failure["payload_sha256"],
                },
                {
                    "category": "canonical_stage_artifacts",
                    "path": V7_RUNTIME_PATH,
                    "status": "not_retained" if not (root / V7_RUNTIME_PATH).exists() else "not_verified",
                },
            ]
        )
        return {
            "status": "selection_inconclusive_software_failure",
            "version": "7.0",
            "route": "fixed_multi_asset_causal_trend_budget",
            "protocol_id": "factor-lab/7.0/fixed-multi-asset-trend-budget-v1",
            "historical_evidence_class": "preclosure_exposed_historical_diagnostic",
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "audit_status": "not_opened",
            "terminal_result_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 0
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as exc:
        checks.append(
            {
                "category": "published_7_0_failure_archive",
                "path": V7_FAILURE_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
        )
        return {
            "status": "integrity_mismatch",
            "version": "7.0",
            "route": "fixed_multi_asset_causal_trend_budget",
            "protocol_id": None,
            "historical_evidence_class": None,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "audit_status": "unknown",
            "terminal_result_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3


def _strategy_status_7_1_archived(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    checks: list[dict[str, Any]] = []
    try:
        result = _verify_published_7_1_result(root)
        checks.extend(
            [
                {
                    "category": "local_archived_annotated_tag",
                    "path": "refs/tags/7.1",
                    "status": "match",
                    "tag_object": V71_TAG_OBJECT,
                    "peeled_commit": V71_TAG_COMMIT,
                },
                {
                    "category": "terminal_result",
                    "path": V71_RESULT_PATH,
                    "status": "match",
                    "payload_sha256": result["payload_sha256"],
                },
                {
                    "category": "canonical_stage_artifacts",
                    "path": V71_RUNTIME_PATH,
                    "status": "not_retained" if not (root / V71_RUNTIME_PATH).exists() else "not_verified",
                },
            ]
        )
        return {
            "status": "selection_falsified_no_candidate",
            "version": "7.1",
            "route": "fixed_multi_asset_causal_trend_budget",
            "protocol_id": "factor-lab/7.0/fixed-multi-asset-trend-budget-v1",
            "historical_evidence_class": "preclosure_exposed_historical_diagnostic",
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "audit_status": "not_opened",
            "terminal_result_payload_sha256": result["payload_sha256"],
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 0
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as exc:
        checks.append(
            {
                "category": "local_7_1_terminal_archive",
                "path": V71_RESULT_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
        )
        return {
            "status": "integrity_mismatch",
            "version": "7.1",
            "route": "fixed_multi_asset_causal_trend_budget",
            "protocol_id": None,
            "historical_evidence_class": None,
            "profit_claim_allowed": False,
            "selected_candidate_id": None,
            "audit_status": "unknown",
            "terminal_result_payload_sha256": None,
            "canonical_data_hashes_verified": False,
            "checks": checks,
        }, 3


def _strategy_status(
    root: Path, *, verify_data: bool, release: str | None = None
) -> tuple[dict[str, Any], int]:
    selected = release or "10.0"
    if selected == "6.0":
        return _strategy_status_6_0(root, verify_data=verify_data)
    if selected == "6.3":
        return _strategy_status_6_3(root, verify_data=verify_data)
    if selected == "7.0":
        return _strategy_status_7_0_archived(root, verify_data=verify_data)
    if selected == "7.1":
        return _strategy_status_7_1_archived(root, verify_data=verify_data)
    if selected == "8.0":
        return _strategy_status_8_0(root, verify_data=verify_data)
    if selected == "8.1":
        return _strategy_status_8_1_archived(root, verify_data=verify_data)
    if selected == "9.0":
        return _strategy_status_9_0_archived(root, verify_data=verify_data)
    if selected == "10.0":
        return _strategy_status_10_0(root, verify_data=verify_data)
    raise ValueError(f"unsupported strategy release: {selected}")


def _strategy_targets(root: Path, signal_date: str) -> dict[str, Any]:
    import pandas as pd

    data_root = root / "runtime" / "data" / "top500"
    features_path = data_root / "features.parquet"
    execution_path = data_root / "execution.parquet"
    for path in (features_path, execution_path):
        if not path.is_file():
            raise SystemExit(f"missing canonical strategy input: {path}")

    columns = [
        "date",
        "ticker",
        "eligible",
        "universe_member",
        "earnings_yield",
        "pb",
        "book_yield",
        "volatility_20",
    ]
    frame = pd.read_parquet(features_path, columns=columns)
    if frame.empty:
        raise SystemExit("canonical features contain no signal rows")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if frame["date"].isna().any():
        raise SystemExit("canonical features contain invalid dates")
    feature_max_date = pd.Timestamp(frame["date"].max()).normalize()

    calendar_frame = pd.read_parquet(execution_path, columns=["date"])
    calendar = sorted(
        pd.to_datetime(calendar_frame["date"], errors="coerce")
        .dropna()
        .dt.normalize()
        .unique()
    )
    if not calendar:
        raise SystemExit("canonical execution contains no official sessions")
    requested = (
        feature_max_date
        if signal_date == "latest"
        else pd.Timestamp(signal_date).normalize()
    )
    if requested not in calendar:
        raise SystemExit(f"signal date is not an official execution session: {requested.date()}")
    frame = frame.loc[frame["date"].le(requested)].copy()
    calendar = [session for session in calendar if session <= requested]
    schedule = generate_sleeve_target_schedule(frame, calendar)
    if not schedule or schedule[-1]["signal_date"] != requested.date().isoformat():
        raise SystemExit(f"could not construct target state for {requested.date()}")
    return {
        "schema_version": 1,
        "route": "fixed_core_low_churn",
        "protocol_id": "factor-lab/6.0/low-churn-fixed-core",
        "requested_signal_date": requested.date().isoformat(),
        "latest_feature_date": feature_max_date.date().isoformat(),
        "feature_is_stale_for_requested_date": requested > feature_max_date,
        "decision": schedule[-1],
    }


def _strategy_command(arguments: argparse.Namespace) -> int:
    root = arguments.root.resolve()
    if arguments.strategy_command == "status":
        result, exit_code = _strategy_status(
            root,
            verify_data=bool(arguments.verify_data),
            release=arguments.release,
        )
        _json(result)
        return exit_code
    if arguments.strategy_command == "targets":
        _json(_strategy_targets(root, str(arguments.signal_date)))
        return 0
    raise AssertionError(arguments.strategy_command)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.root is None:
        arguments.root = _root()
    if arguments.command == "data":
        return _data_command(arguments)
    if arguments.command == "research":
        return _research_command(arguments)
    if arguments.command == "strategy":
        return _strategy_command(arguments)
    if arguments.command == "report":
        return _report_command(arguments)
    raise AssertionError(arguments.command)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
