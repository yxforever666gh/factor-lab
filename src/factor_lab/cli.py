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
V71_PROTOCOL_PATH = V7_PROTOCOL_PATH
V71_ASSET_SELECTION_PATH = V7_ASSET_SELECTION_PATH
V71_AMENDMENT_PATH = "protocols/7.1-corrective-amendment-1.json"
V71_CLOSURE_PATH = "protocols/7.1-release.json"
V71_PRECLOSURE_TRAIN_PATH = V7_PRECLOSURE_TRAIN_PATH
V71_WINNER_FREEZE_PATH = "protocols/evidence/7.1/winner-freeze.json"
V71_AUDIT_PATH = "protocols/evidence/7.1/historical-audit.json"
V71_RESULT_PATH = "protocols/evidence/7.1/result.json"
V71_RUNTIME_PATH = "runtime/data/multi-asset-7.1"
V7_TAG_OBJECT = "25bbc306e8842feab923380416f8329e0dd81100"
V7_TAG_COMMIT = "412026ca0370d53ca704adfd1122a811e768842e"


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
        description="Local Parquet factor research and long-only backtesting.",
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
        choices=("6.0", "6.3", "7.0", "7.1"),
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


def _load_v71_runner(root: Path) -> Any:
    resolved = root.resolve()
    script = resolved / "scripts" / "run-multi-asset-evidence.py"
    if script.is_symlink() or not script.is_file():
        raise ValueError("7.1 formal runner is missing or indirect")
    spec = importlib.util.spec_from_file_location("factor_lab_v71_status_verifier", script)
    if spec is None or spec.loader is None:
        raise ValueError("could not load the 7.1 formal verifier")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if (
        module.ROOT.resolve() != resolved
        or module.RELEASE != "7.1"
        or module.CLOSURE_PATH.as_posix() != V71_CLOSURE_PATH
        or module.WORK_ROOT.resolve() != (resolved / V71_RUNTIME_PATH).resolve()
        or module.SOURCE_ROOT.resolve()
        != (resolved / V71_RUNTIME_PATH / "sources").resolve()
        or module.EVALUATION_ROOT.resolve()
        != (resolved / V71_RUNTIME_PATH / "evaluations").resolve()
        or module.BINDING_ROOT.resolve()
        != (resolved / V71_RUNTIME_PATH / "stage-bindings").resolve()
        or module.PRIOR_WORK_ROOT.resolve()
        != (resolved / V7_RUNTIME_PATH).resolve()
        or module.EVIDENCE_ROOT.as_posix() != "protocols/evidence/7.1"
        or module.AMENDMENT_PATH.as_posix() != V71_AMENDMENT_PATH
    ):
        raise ValueError("7.1 runner contains an incorrect release namespace")
    return module


def _v71_require_head_ci(root: Path) -> str:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    _load_v71_runner(root)._require_head_pushed_and_ci_success(head)
    return head


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


def _strategy_status_7_1_pending(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    checks: list[dict[str, Any]] = []
    protocol, protocol_check = _v7_json_check(root, V71_PROTOCOL_PATH)
    protocol_check["category"] = "protocol_payload"
    checks.append(protocol_check)
    selection, selection_check = _v7_json_check(root, V71_ASSET_SELECTION_PATH)
    selection_check["category"] = "asset_selection_payload"
    checks.append(selection_check)
    amendment, amendment_check = _v7_json_check(
        root,
        V71_AMENDMENT_PATH,
        expected_payload="7335cdbb61cd0d7b9c3e6f6896ec576c7e403b87d83cfa3d6679965691984c86",
    )
    amendment_check["category"] = "corrective_amendment"
    checks.append(amendment_check)
    failure, failure_check = _v7_json_check(
        root,
        V7_FAILURE_PATH,
        expected_payload="04099ab6c2bd03099c9d045120578344bfe9ba3c963dfb82a0cba9f8a49f5df9",
        expected_file="674e62603f7ab9a026e9ef69dc52810889f584302e94be13a685dc708b76da53",
    )
    failure_check["category"] = "published_7_0_execution_failure"
    checks.append(failure_check)
    disclosure_contract = (
        protocol.get("preclosure_train_disclosure")
        if protocol is not None
        else {}
    )
    disclosure, disclosure_check = _v7_json_check(
        root,
        V71_PRECLOSURE_TRAIN_PATH,
        expected_payload=str(disclosure_contract.get("payload_sha256") or ""),
        expected_file=str(disclosure_contract.get("file_sha256") or ""),
    )
    disclosure_check["category"] = "preclosure_train_disclosure"
    checks.append(disclosure_check)
    failures = [
        item for item in checks if item.get("status") != "match"
    ]
    if amendment is not None:
        try:
            _load_v71_runner(root)._verify_corrective_amendment(amendment)
            checks.append(
                {
                    "category": "corrective_whitelist",
                    "path": V71_AMENDMENT_PATH,
                    "status": "match",
                }
            )
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            item = {
                "category": "corrective_whitelist",
                "path": V71_AMENDMENT_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
            checks.append(item)
            failures.append(item)
    if failure is not None:
        boundary = failure.get("evidence_boundary") or {}
        valid_failure = (
            failure.get("status") == "selection_inconclusive_software_failure"
            and failure.get("classification") == "target_order_replay_false_negative"
            and all(
                boundary.get(key) is False
                for key in (
                    "validation_market_outcomes_opened",
                    "winner_freeze_created",
                    "audit_market_outcomes_opened",
                    "terminal_result_created",
                )
            )
        )
        item = {
            "category": "published_7_0_failure_boundary",
            "path": V7_FAILURE_PATH,
            "status": "match" if valid_failure else "mismatch",
        }
        checks.append(item)
        if not valid_failure:
            failures.append(item)
    try:
        _verify_published_7_0_failure(root)
        checks.append(
            {
                "category": "local_7_0_tag_binding",
                "path": "refs/tags/7.0",
                "status": "match",
            }
        )
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as exc:
        item = {
            "category": "local_7_0_tag_binding",
            "path": "refs/tags/7.0",
            "status": "mismatch",
            "error": str(exc),
        }
        checks.append(item)
        failures.append(item)
    if disclosure is not None:
        try:
            _load_v71_runner(root)._verify_disclosed_outcome_boundary(disclosure)
            checks.append(
                {
                    "category": "preclosure_outcome_boundary",
                    "path": V71_PRECLOSURE_TRAIN_PATH,
                    "status": "match",
                }
            )
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            item = {
                "category": "preclosure_outcome_boundary",
                "path": V71_PRECLOSURE_TRAIN_PATH,
                "status": "mismatch",
                "error": str(exc),
            }
            checks.append(item)
            failures.append(item)
    if protocol is not None and selection is not None and disclosure is not None:
        assets = protocol.get("assets") or {}
        if (
            assets.get("selection_evidence_file_sha256")
            != _file_sha256(root / V71_ASSET_SELECTION_PATH)
            or assets.get("selection_evidence_payload_sha256")
            != selection.get("payload_sha256")
            or selection.get("selected_codes")
            != [
                "510300.SH",
                "159920.SZ",
                "513100.SH",
                "518880.SH",
                "511010.SH",
                "511880.SH",
            ]
        ):
            item = {
                "category": "asset_selection_binding",
                "path": V71_ASSET_SELECTION_PATH,
                "status": "mismatch",
            }
            checks.append(item)
            failures.append(item)
        else:
            checks.append(
                {
                    "category": "asset_selection_binding",
                    "path": V71_ASSET_SELECTION_PATH,
                    "status": "match",
                }
            )
    for relative in (
        V71_CLOSURE_PATH,
        V71_WINNER_FREEZE_PATH,
        V71_AUDIT_PATH,
        V71_RESULT_PATH,
        V71_RUNTIME_PATH,
    ):
        exists = (root / relative).exists()
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
            head = _v71_require_head_ci(root)
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
    if failures:
        status, exit_code = "integrity_mismatch", 3
    elif clean:
        status, exit_code = "implementation_ready_for_preselection_closure", 0
    else:
        status, exit_code = "implementation_pending_clean_commit", 2
    return {
        "status": status,
        "version": "7.1",
        "route": "fixed_multi_asset_causal_trend_budget",
        "protocol_id": (
            protocol.get("protocol_id") if protocol is not None else None
        ),
        "historical_evidence_class": (
            (protocol.get("claim_contract") or {}).get(
                "historical_evidence_class"
            )
            if protocol is not None
            else None
        ),
        "profit_claim_allowed": False,
        "selected_candidate_id": None,
        "audit_status": "not_opened",
        "terminal_result_payload_sha256": None,
        "canonical_data_hashes_verified": False,
        "checks": checks,
    }, exit_code


def _strategy_status_7_1(
    root: Path, *, verify_data: bool
) -> tuple[dict[str, Any], int]:
    if not (root / V71_CLOSURE_PATH).is_file():
        return _strategy_status_7_1_pending(root, verify_data=verify_data)
    checks: list[dict[str, Any]] = []
    try:
        verifier = _load_v71_runner(root)
        state = verifier.verify_release_state(
            verify_data=verify_data,
            verify_runtime=False,
        )
        closure = state["closure"]
        protocol = state["protocol"]
        freeze = state["freeze"]
        audit = state["audit"]
        result = state["result"]
        data_verified = bool(verify_data and freeze is not None)
        checks.append(
            {
                "category": "release_evidence_chain",
                "path": V71_CLOSURE_PATH,
                "status": "match",
            }
        )
        checks.append(
            {
                "category": "canonical_stage_artifacts",
                "path": V71_RUNTIME_PATH,
                "status": (
                    "match"
                    if data_verified
                    else "not_applicable"
                    if freeze is None
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
            raise RuntimeError("7.1 formal status requires a clean worktree")
        head = _v71_require_head_ci(root)
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
            "version": "7.1",
            "route": closure.get("route"),
            "protocol_id": protocol.get("protocol_id"),
            "historical_evidence_class": claim.get("historical_evidence_class"),
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
                "path": V71_CLOSURE_PATH,
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


def _strategy_status(
    root: Path, *, verify_data: bool, release: str | None = None
) -> tuple[dict[str, Any], int]:
    selected = release or "7.1"
    if selected == "6.0":
        return _strategy_status_6_0(root, verify_data=verify_data)
    if selected == "6.3":
        return _strategy_status_6_3(root, verify_data=verify_data)
    if selected == "7.0":
        return _strategy_status_7_0_archived(root, verify_data=verify_data)
    if selected == "7.1":
        return _strategy_status_7_1(root, verify_data=verify_data)
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
