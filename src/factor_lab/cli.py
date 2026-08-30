"""Single command-line entry point for the lightweight Factor Lab mainline."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from factor_lab.data import (
    RuntimeLayout,
    audit_top500_store,
    build_data,
    enrich_top500_store,
    load_data_config,
    plan_feature_store_migration,
    sync_data,
    sync_enrichment,
    sync_exact_reference,
    sync_suspensions,
)
from factor_lab.data.suspensions import SuspensionProviderWaitingError
from factor_lab.research.runner import latest_run, run_research
from factor_lab.strategy import generate_sleeve_target_schedule


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
        "strategy", help="Inspect or generate the 6.0 low-churn fixed-core route."
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


def _strategy_status(root: Path, *, verify_data: bool) -> tuple[dict[str, Any], int]:
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
            root, verify_data=bool(arguments.verify_data)
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
