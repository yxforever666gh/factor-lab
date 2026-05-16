from __future__ import annotations

import copy
import hashlib
import json
from typing import Any


TRANSIENT_EXPERIMENT_KEYS = {
    "output_dir",
    "generated_at",
    "generated_at_utc",
    "worker_note",
    "notes",
    "description",
    "run_id",
    "rerun_of_run_id",
    "artifact_type",
    "schema_version",
}

TRANSIENT_FACTOR_KEYS = {
    "generated_at",
    "generated_at_utc",
    "worker_note",
    "notes",
    "summary",
}


def _stable_json_hash(payload: Any) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def config_fingerprint(config: dict[str, Any]) -> str:
    return _stable_json_hash(config)


def _strip_keys(obj: Any, keys: set[str]) -> Any:
    if isinstance(obj, dict):
        return {key: _strip_keys(value, keys) for key, value in obj.items() if key not in keys}
    if isinstance(obj, list):
        return [_strip_keys(value, keys) for value in obj]
    return obj


def _normalize_factor_definition(row: Any) -> Any:
    if not isinstance(row, dict):
        return row
    cleaned = _strip_keys(row, TRANSIENT_FACTOR_KEYS)
    # Keep lineage/operator fields because they define semantic equivalence for generated candidates.
    return cleaned


def _normalize_factors(factors: Any) -> Any:
    if not isinstance(factors, list):
        return factors
    normalized = [_normalize_factor_definition(row) for row in factors]
    return sorted(
        normalized,
        key=lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )


def experiment_equivalence_payload(config: dict[str, Any]) -> dict[str, Any]:
    """Return the semantic payload used to decide whether two workflow experiments are equivalent.

    This intentionally removes output paths and transient generation metadata while retaining
    data source, date range, universe, factor definitions, thresholds, and validation settings.
    """
    normalized = copy.deepcopy(config)
    normalized = _strip_keys(normalized, TRANSIENT_EXPERIMENT_KEYS)
    if "factors" in normalized:
        normalized["factors"] = _normalize_factors(normalized.get("factors"))
    return normalized


def experiment_equivalence_fingerprint(config: dict[str, Any]) -> str:
    return _stable_json_hash(experiment_equivalence_payload(config))


def workflow_experiment_fingerprint(config: dict[str, Any]) -> str:
    """Fingerprint a workflow by semantic experiment identity, not output location."""
    return f"workflow::{experiment_equivalence_fingerprint(config)}"
