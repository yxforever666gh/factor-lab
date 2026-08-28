"""Single command-line entry point for the lightweight Factor Lab mainline."""

from __future__ import annotations

import argparse
from datetime import date
import json
import subprocess
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
    sync_suspensions,
)
from factor_lab.research.runner import latest_run, run_research
from factor_lab.implementation_closure import verify_implementation_closure
from factor_lab.prospective_attestation import API_VERSION, DEFAULT_REPOSITORY
from factor_lab.prospective_evaluation import (
    EVALUATION_CONTRACT_SHA256,
    EVALUATOR_ID,
)
from factor_lab.prospective_ledger import (
    LedgerLayout,
    abandon_implementation_upgrade,
    activate_protocol,
    append_correction,
    append_implementation_upgrade,
    append_outcome,
    audit_ledger,
    build_decision_plan,
    build_execution_evidence,
    build_membership_evidence,
    build_outcome_payload,
    build_signal_input_evidence,
    canonical_json_bytes,
    checkpoint_evaluation,
    create_only_file,
    ledger_status,
    seal_decision,
    sha256_file,
    store_decision_plan,
    strict_load_canonical,
)
from factor_lab.prospective_runtime import attest_snapshot, verify_authoritative_run


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
                and (candidate / "protocols" / "5.0.json").is_file()
            ):
                return candidate
    raise SystemExit(
        "cannot locate the Factor Lab checkout; run inside it or pass --root"
    )


def _json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


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
            "adaptive",
            "walk-forward",
            "results-first",
            "recovery",
            "next",
            "legacy-regression",
        ),
        default="adaptive",
    )
    run_mode = run.add_mutually_exclusive_group(required=True)
    run_mode.add_argument("--canary", action="store_true")
    run_mode.add_argument("--full", action="store_true")
    run.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    run.add_argument("--no-robustness", action="store_true", help=argparse.SUPPRESS)
    research_commands.add_parser("status", help="Show the latest completed run.")

    report = commands.add_parser("report", help="Print a completed Markdown report.")
    report.add_argument("--run", default="latest", help="Run id or 'latest'.")

    prospective = commands.add_parser(
        "prospective", help="Manage the create-only 5.0 prospective evidence ledger."
    )
    prospective_commands = prospective.add_subparsers(
        dest="prospective_command", required=True
    )
    prospective.add_argument("--ledger-root", type=Path, help=argparse.SUPPRESS)
    activation = prospective_commands.add_parser(
        "activate", help="Bind an empty ledger to the immutable 5.0 release."
    )
    activation.add_argument(
        "--run",
        required=True,
        help="Exact completed full adaptive run id to bind as authoritative.",
    )
    activation.add_argument("--protocol", type=Path)
    activation.add_argument("--release-tag", default="5.0")
    upgrade = prospective_commands.add_parser(
        "upgrade",
        help="Bind a published target-generator implementation to the activated protocol.",
    )
    upgrade.add_argument(
        "--manifest",
        type=Path,
        default=Path("protocols/5.2-target-generator.json"),
    )
    upgrade.add_argument("--release-tag", default="5.4")
    abandon_upgrade = prospective_commands.add_parser(
        "abandon-upgrade",
        help="Explicitly abandon an unattested implementation upgrade.",
    )
    abandon_upgrade.add_argument("--upgrade", required=True)
    abandon_upgrade.add_argument("--reason", required=True)
    signal_input = prospective_commands.add_parser(
        "input",
        help="Build a content-addressed point-in-time signal snapshot.",
    )
    signal_input.add_argument("--signal-date", required=True)
    signal_input.add_argument("--available-at-utc")
    signal_input.add_argument("--membership-snapshot", type=Path)
    membership = prospective_commands.add_parser(
        "membership",
        help="Build a release-bound forward monthly Top-500 membership snapshot.",
    )
    membership.add_argument("--month", required=True)
    membership.add_argument("--available-at-utc")
    plan = prospective_commands.add_parser(
        "plan", help="Generate a canonical decision plan from a verified signal snapshot."
    )
    plan.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Content-addressed signal snapshot directory (or its manifest.json).",
    )
    plan.add_argument("--output", type=Path)
    seal = prospective_commands.add_parser(
        "seal", help="Append a pre-deadline decision from a canonical plan."
    )
    seal.add_argument("--plan", type=Path, required=True)
    execution = prospective_commands.add_parser(
        "execution",
        help="Build immutable source-backed execution evidence for a sealed decision.",
    )
    execution.add_argument("--decision", required=True)
    execution.add_argument("--available-at-utc")
    outcome = prospective_commands.add_parser(
        "outcome", help="Recompute and append a source-backed prospective outcome."
    )
    outcome.add_argument("--decision", required=True)
    outcome.add_argument(
        "--execution",
        required=True,
        help="Execution snapshot SHA-256 or its canonical bundle directory.",
    )
    correction = prospective_commands.add_parser(
        "correct", help="Append a correction without rewriting an outcome."
    )
    correction.add_argument("--input", type=Path, required=True)
    attest = prospective_commands.add_parser(
        "attest", help="Attest a sealed snapshot and append its verified receipt."
    )
    attest.add_argument(
        "--snapshot",
        default="latest",
        help="Snapshot path or 'latest' (default).",
    )
    attest.add_argument("--release-tag", default="5.0")
    attest.add_argument("--workflow-run-id", type=int)
    attest.add_argument(
        "--purpose",
        choices=(
            "activation_canary",
            "implementation_upgrade_canary",
            "decision_anchor",
        ),
        required=True,
    )
    attest.add_argument("--decision-record-sha256")
    attest.add_argument("--admission-deadline-utc")
    attest.add_argument("--repository", default=DEFAULT_REPOSITORY)
    prospective_commands.add_parser("audit", help="Verify every record and snapshot.")
    prospective_commands.add_parser(
        "evaluate", help="Evaluate confirmed outcomes against the preregistered 5.2 gates."
    )
    prospective_commands.add_parser("status", help="Show the current ledger phase.")
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
        return 0 if result.get("status") in {"complete", "partial"} else 1
    if arguments.data_command == "suspensions":
        result = sync_suspensions(
            arguments.start_date,
            arguments.end_date,
            config_path=config_path,
            layout=layout,
            resume=bool(arguments.resume),
        )
        _json(result)
        return 0 if result.get("status") == "complete" else 1
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
    adaptive = dict(summary.get("adaptive") or {})
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
        "adaptive": {
            "evidence_class": adaptive.get("evidence_class"),
            "canary_smoke_only": adaptive.get("canary_smoke_only"),
            "protocol_sha256": adaptive.get("protocol_sha256"),
            "common_evaluation_start": adaptive.get(
                "common_evaluation_start"
            ),
            "shadow_accounts_valid": adaptive.get("shadow_accounts_valid"),
            "scoring_accounts_valid": adaptive.get("scoring_accounts_valid"),
            "future_feedback_violation_count": adaptive.get(
                "future_feedback_violation_count"
            ),
            "future_overlay_violation_count": adaptive.get(
                "future_overlay_violation_count"
            ),
            "integrity_valid": adaptive.get("integrity_valid"),
            "frozen_route": adaptive.get("frozen_route"),
            "gate_results": adaptive.get("gate_results"),
        }
        if adaptive.get("enabled")
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


def _json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _prospective_root(arguments: argparse.Namespace) -> Path:
    configured = getattr(arguments, "ledger_root", None)
    return (
        Path(configured).resolve()
        if configured is not None
        else (arguments.root.resolve() / "runtime" / "prospective" / "5.0")
    )


def _published_tag_oids(root: Path, tag: str) -> tuple[str, str]:
    def git(*values: str) -> str:
        completed = subprocess.run(
            ["git", *values],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    reference = f"refs/tags/{tag}"
    object_oid = git("rev-parse", reference)
    if git("cat-file", "-t", object_oid) != "tag":
        raise ValueError(f"release tag must be annotated: {tag}")
    commit_oid = git("rev-parse", f"{reference}^{{}}")
    try:
        remote_lines = git(
            "ls-remote",
            "origin",
            reference,
            f"{reference}^{{}}",
        ).splitlines()
        remote = {
            name: oid
            for line in remote_lines
            for oid, name in [line.split("\t", maxsplit=1)]
        }
        remote_object_oid = remote.get(reference)
        remote_commit_oid = remote.get(f"{reference}^{{}}")
    except subprocess.CalledProcessError:
        # Git smart-HTTP can be unavailable even while the GitHub REST API is
        # healthy.  The fallback preserves the same annotated-object and
        # peeled-commit checks; it is not a contents-API or branch-name proxy.
        repo_payload = json.loads(
            subprocess.run(
                ["gh", "repo", "view", "--json", "nameWithOwner"],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        repository = repo_payload.get("nameWithOwner")
        if not isinstance(repository, str) or not repository.strip():
            raise ValueError("could not resolve the GitHub repository")

        def github_api(endpoint: str) -> Mapping[str, Any]:
            payload = json.loads(
                subprocess.run(
                    [
                        "gh",
                        "api",
                        endpoint,
                        "-H",
                        "Accept: application/vnd.github+json",
                        "-H",
                        f"X-GitHub-Api-Version: {API_VERSION}",
                    ],
                    cwd=root,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
            )
            if not isinstance(payload, Mapping):
                raise ValueError("GitHub tag response must be an object")
            return payload

        remote_ref = github_api(
            f"repos/{repository.strip()}/git/ref/tags/{tag}"
        )
        remote_ref_object = remote_ref.get("object")
        if remote_ref.get("ref") != reference or not isinstance(
            remote_ref_object, Mapping
        ):
            raise ValueError(f"GitHub tag ref is invalid for {tag}")
        if remote_ref_object.get("type") != "tag":
            raise ValueError(f"GitHub release tag is not annotated: {tag}")
        remote_object_oid = remote_ref_object.get("sha")
        if not isinstance(remote_object_oid, str):
            raise ValueError(f"GitHub tag object is missing for {tag}")

        remote_tag = github_api(
            f"repos/{repository.strip()}/git/tags/{remote_object_oid}"
        )
        remote_target = remote_tag.get("object")
        if remote_tag.get("tag") != tag or not isinstance(remote_target, Mapping):
            raise ValueError(f"GitHub annotated tag is invalid for {tag}")
        if remote_target.get("type") != "commit":
            raise ValueError(f"GitHub release tag does not target a commit: {tag}")
        remote_commit_oid = remote_target.get("sha")

    if remote_object_oid != object_oid or remote_commit_oid != commit_oid:
        raise ValueError(
            f"local and GitHub tag objects do not match for {tag}"
        )
    return object_oid, commit_oid


def _prospective_snapshot_path(ledger_root: Path, requested: str) -> Path:
    if requested != "latest":
        path = Path(requested).resolve()
        if not path.is_file():
            raise ValueError(f"missing prospective snapshot: {path}")
        return path
    audit = audit_ledger(ledger_root)
    if not audit.get("valid"):
        raise ValueError("cannot attest latest snapshot of an invalid ledger")
    sequence = audit.get("head_sequence")
    if type(sequence) is not int:
        raise ValueError("cannot attest an unactivated ledger")
    layout = LedgerLayout.at(ledger_root)
    candidates = sorted(layout.snapshots.glob(f"{sequence:016d}-*.json"))
    if len(candidates) != 1 or not candidates[0].is_file():
        raise ValueError(
            f"expected exactly one latest snapshot for sequence {sequence}"
        )
    return candidates[0]


def _path_in_project(root: Path, requested: Path, *, label: str) -> tuple[Path, str]:
    path = (requested if requested.is_absolute() else root / requested).resolve()
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(f"{label} must be inside the project root") from exc
    if not path.is_file():
        raise ValueError(f"missing {label}: {path}")
    return path, relative


def _require_file_at_commit(
    root: Path,
    path: Path,
    relative_path: str,
    commit_oid: str,
    *,
    label: str,
) -> None:
    """Require the local bytes to be the exact bytes published by a tag."""

    try:
        published = subprocess.run(
            ["git", "show", f"{commit_oid}:{relative_path}"],
            cwd=root,
            check=True,
            capture_output=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise ValueError(
            f"{label} is absent from published commit {commit_oid}"
        ) from exc
    if path.read_bytes() != published:
        raise ValueError(f"local {label} differs from published commit {commit_oid}")


def _activation_payload(ledger_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    audit = audit_ledger(ledger_root)
    if not audit.get("valid"):
        raise ValueError("prospective ledger is invalid")
    records = audit.get("records")
    if not isinstance(records, list) or not records:
        raise ValueError("prospective ledger is not activated")
    first_path = records[0].get("path")
    if not isinstance(first_path, str):
        raise ValueError("prospective activation record path is missing")
    record = strict_load_canonical(Path(first_path).read_bytes())
    if not isinstance(record, Mapping) or record.get("kind") != "protocol_activation":
        raise ValueError("prospective activation record is invalid")
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("prospective activation payload is invalid")
    state = audit.get("state")
    if not isinstance(state, Mapping):
        raise ValueError("prospective ledger state is invalid")
    return dict(payload), dict(state)


def _implementation_upgrade_payload(
    root: Path,
    ledger_root: Path,
    manifest_request: Path,
    release_tag: str,
) -> dict[str, Any]:
    manifest_path, manifest_relative = _path_in_project(
        root, manifest_request, label="implementation manifest"
    )
    manifest = _json_object(manifest_path)
    object_oid, commit_oid = _published_tag_oids(root, release_tag)
    _require_file_at_commit(
        root,
        manifest_path,
        manifest_relative,
        commit_oid,
        label="implementation manifest",
    )
    activation, state = _activation_payload(ledger_root)

    required_manifest = {
        "schema_version": 1,
        "kind": "prospective_target_generator_implementation",
        "implementation_release": release_tag,
        "activation_release": activation["protocol_release"],
        "does_not_replace_activation": True,
        "activation_protocol_id": activation["protocol_id"],
        "activation_protocol_sha256": activation["protocol_sha256"],
        "frozen_route": activation["frozen_route"],
        "decision_plan_schema_version": 2,
    }
    for field_name, expected in required_manifest.items():
        if manifest.get(field_name) != expected:
            raise ValueError(
                f"implementation manifest {field_name} differs from {expected!r}"
            )
    for field_name in ("generator_id", "generator_entrypoint"):
        if not isinstance(manifest.get(field_name), str) or not manifest[field_name]:
            raise ValueError(f"implementation manifest {field_name} is missing")
    verify_implementation_closure(
        root,
        manifest_path=manifest_path,
        manifest_sha256=sha256_file(manifest_path),
        implementation_commit_oid=commit_oid,
        generator_id=str(manifest["generator_id"]),
        generator_entrypoint=str(manifest["generator_entrypoint"]),
    )
    vectors = manifest.get("test_vectors")
    if not isinstance(vectors, Mapping):
        raise ValueError("implementation manifest test_vectors is missing")
    vector_path, vector_relative = _path_in_project(
        root, Path(str(vectors.get("path") or "")), label="generator test vectors"
    )
    vector_sha = str(vectors.get("sha256") or "")
    if sha256_file(vector_path) != vector_sha:
        raise ValueError("generator test-vector SHA-256 differs from the manifest")
    _require_file_at_commit(
        root,
        vector_path,
        vector_relative,
        commit_oid,
        label="generator test vectors",
    )
    evaluation = manifest.get("evaluation_contract")
    if (
        not isinstance(evaluation, Mapping)
        or evaluation.get("evaluator_id") != EVALUATOR_ID
        or evaluation.get("contract_sha256") != EVALUATION_CONTRACT_SHA256
    ):
        raise ValueError("implementation manifest evaluation contract differs")
    return {
        "schema_version": 1,
        "activation_record_sha256": state["activation_record_sha256"],
        "supersedes_implementation_upgrade_record_sha256": state.get(
            "latest_implementation_upgrade_record_sha256"
        ),
        "protocol_id": activation["protocol_id"],
        "protocol_sha256": activation["protocol_sha256"],
        "frozen_route": activation["frozen_route"],
        "implementation_release_tag": release_tag,
        "implementation_release_tag_object_oid": object_oid,
        "implementation_commit_oid": commit_oid,
        "generator_id": manifest["generator_id"],
        "generator_entrypoint": manifest["generator_entrypoint"],
        "generator_manifest_path": manifest_relative,
        "generator_manifest_sha256": sha256_file(manifest_path),
        "generator_test_vector_sha256": vector_sha,
        "evaluator_id": EVALUATOR_ID,
        "evaluation_contract_sha256": EVALUATION_CONTRACT_SHA256,
        "decision_plan_schema_version": 2,
    }


def _signal_snapshot_sha256(requested: Path) -> str:
    path = requested.resolve()
    if path.is_file() and path.name in {
        "manifest.json",
        "rows.json",
        "build-receipt.json",
    }:
        path = path.parent
    if not path.is_dir():
        raise ValueError(f"missing prospective signal snapshot directory: {path}")
    value = path.name
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("--input must be a canonical content-addressed snapshot path")
    return value


def _execution_snapshot_sha256(requested: str) -> str:
    """Resolve a canonical execution bundle argument without trusting its JSON."""

    value = str(requested)
    candidate = Path(value).expanduser()
    if candidate.exists():
        candidate = candidate.resolve()
        if candidate.is_file() and candidate.name in {"snapshot.json", "sources.json"}:
            candidate = candidate.parent
        if not candidate.is_dir():
            raise ValueError(f"invalid execution snapshot path: {candidate}")
        value = candidate.name
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("--execution must be a lowercase SHA-256 or canonical bundle path")
    return value


def _prospective_command(arguments: argparse.Namespace) -> int:
    root = arguments.root.resolve()
    ledger_root = _prospective_root(arguments)
    command = arguments.prospective_command
    if command == "activate":
        protocol_path = Path(
            arguments.protocol or root / "protocols" / "5.0.json"
        ).resolve()
        object_oid, commit_oid = _published_tag_oids(
            root, str(arguments.release_tag)
        )
        authoritative_run = verify_authoritative_run(
            root,
            str(arguments.run),
            protocol_path=protocol_path,
            release_commit_oid=commit_oid,
        )
        _json(
            activate_protocol(
                ledger_root,
                protocol_path=protocol_path,
                release_tag=str(arguments.release_tag),
                release_tag_object_oid=object_oid,
                release_commit_oid=commit_oid,
                authoritative_run=authoritative_run,
            )
        )
        return 0
    if command == "upgrade":
        upgrade = _implementation_upgrade_payload(
            root,
            ledger_root,
            Path(arguments.manifest),
            str(arguments.release_tag),
        )
        _json(append_implementation_upgrade(ledger_root, upgrade))
        return 0
    if command == "abandon-upgrade":
        _json(
            abandon_implementation_upgrade(
                ledger_root,
                implementation_upgrade_record_sha256=str(arguments.upgrade),
                reason=str(arguments.reason),
            )
        )
        return 0
    if command == "input":
        try:
            signal_date = date.fromisoformat(str(arguments.signal_date))
        except ValueError as exc:
            raise ValueError("--signal-date must be a canonical ISO date") from exc
        if signal_date.isoformat() != str(arguments.signal_date):
            raise ValueError("--signal-date must be a canonical ISO date")
        # 2026-08-21 and earlier are the frozen historical bridge.  Admitting
        # one of those dates through the operational CLI would manufacture a
        # backfilled 'prospective' observation after the implementation tag.
        if signal_date <= date(2026, 8, 21):
            raise ValueError(
                "prospective input signal date must be after the frozen 2026-08-21 cutoff"
            )
        membership = (
            Path(arguments.membership_snapshot).resolve()
            if arguments.membership_snapshot is not None
            else None
        )
        snapshot = build_signal_input_evidence(
            ledger_root,
            signal_date=signal_date.isoformat(),
            available_at_utc=arguments.available_at_utc,
            membership_snapshot_path=membership,
        )
        _json(snapshot)
        return 0
    if command == "membership":
        _json(
            build_membership_evidence(
                ledger_root,
                membership_month=str(arguments.month),
                available_at_utc=arguments.available_at_utc,
            )
        )
        return 0
    if command == "plan":
        plan = build_decision_plan(
            ledger_root,
            source_data_snapshot_sha256=_signal_snapshot_sha256(arguments.input),
        )
        stored = store_decision_plan(ledger_root, plan)
        if arguments.output is not None:
            output = arguments.output.resolve()
            created = create_only_file(output, canonical_json_bytes(plan))
            stored["requested_output"] = str(output)
            stored["requested_output_created"] = created
        _json({"plan": plan, "stored": stored})
        return 0
    if command == "seal":
        _json(seal_decision(ledger_root, arguments.plan.resolve()))
        return 0
    if command == "execution":
        _json(
            build_execution_evidence(
                ledger_root,
                decision_record_sha256=str(arguments.decision),
                available_at_utc=arguments.available_at_utc,
            )
        )
        return 0
    if command == "outcome":
        payload = build_outcome_payload(
            ledger_root,
            decision_record_sha256=str(arguments.decision),
            execution_snapshot_sha256=_execution_snapshot_sha256(
                str(arguments.execution)
            ),
        )
        _json(append_outcome(ledger_root, payload))
        return 0
    if command == "correct":
        _json(
            append_correction(
                ledger_root, _json_object(arguments.input.resolve())
            )
        )
        return 0
    if command == "attest":
        snapshot_path = _prospective_snapshot_path(
            ledger_root, str(arguments.snapshot)
        )
        snapshot = strict_load_canonical(snapshot_path.read_bytes())
        if not isinstance(snapshot, Mapping):
            raise ValueError(f"expected snapshot JSON object: {snapshot_path}")
        release_commit_oid = snapshot.get("release_commit_oid")
        if not isinstance(release_commit_oid, str) or not release_commit_oid:
            raise ValueError("prospective snapshot has no release commit oid")
        release_tag = snapshot.get("release_tag")
        if not isinstance(release_tag, str) or not release_tag:
            raise ValueError("prospective snapshot has no release tag")
        if str(arguments.release_tag) != release_tag:
            raise ValueError(
                "attestation release tag differs from the activated snapshot"
            )
        ledger_audit = audit_ledger(ledger_root)
        if not ledger_audit.get("valid"):
            raise ValueError("cannot attest an invalid prospective ledger")
        records = ledger_audit.get("records")
        if not isinstance(records, list) or not records:
            raise ValueError("prospective ledger has no activation record")
        activation_path = records[0].get("path")
        if not isinstance(activation_path, str):
            raise ValueError("prospective activation record path is missing")
        activation = strict_load_canonical(Path(activation_path).read_bytes())
        if not isinstance(activation, Mapping) or (
            activation.get("kind") != "protocol_activation"
        ):
            raise ValueError("prospective activation record is invalid")
        activation_payload = activation.get("payload")
        if not isinstance(activation_payload, Mapping):
            raise ValueError("prospective activation payload is invalid")
        published_object_oid, published_commit_oid = _published_tag_oids(
            root, release_tag
        )
        if (
            activation_payload.get("release_tag_object_oid")
            != published_object_oid
            or activation_payload.get("release_commit_oid")
            != published_commit_oid
            or published_commit_oid != release_commit_oid
        ):
            raise ValueError(
                "published release tag differs from the activation binding"
            )
        _json(
            attest_snapshot(
                ledger_root,
                snapshot_path,
                purpose=str(arguments.purpose),
                release_commit_oid=release_commit_oid,
                decision_record_sha256=arguments.decision_record_sha256,
                admission_deadline_utc=arguments.admission_deadline_utc,
                workflow_run_id=arguments.workflow_run_id,
                repository=str(arguments.repository),
                release_tag=release_tag,
            )
        )
        return 0
    if command == "audit":
        audit = audit_ledger(ledger_root)
        _json(audit)
        return 0 if audit.get("valid") else 1
    if command == "evaluate":
        _json(checkpoint_evaluation(ledger_root))
        return 0
    if command == "status":
        _json(ledger_status(ledger_root))
        return 0
    raise AssertionError(command)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.root is None:
        arguments.root = _root()
    if arguments.command == "data":
        return _data_command(arguments)
    if arguments.command == "research":
        return _research_command(arguments)
    if arguments.command == "report":
        return _report_command(arguments)
    if arguments.command == "prospective":
        return _prospective_command(arguments)
    raise AssertionError(arguments.command)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
