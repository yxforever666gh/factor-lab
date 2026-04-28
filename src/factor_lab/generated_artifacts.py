from __future__ import annotations

from copy import deepcopy
from typing import Any


GENERATED_CONFIG_SCHEMA_VERSION = "factor_lab.generated_config.v2"
GENERATED_BATCH_SCHEMA_VERSION = "factor_lab.generated_batch.v2"


def build_dependency_graph(factors: list[dict[str, Any]] | None) -> dict[str, Any]:
    factors = [row for row in (factors or []) if isinstance(row, dict)]
    node_names = [str(row.get("name")) for row in factors if row.get("name")]
    generated_factors: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for row in factors:
        name = row.get("name")
        if not name:
            continue
        operator = row.get("generator_operator")
        left_name = row.get("left_factor_name")
        right_name = row.get("right_factor_name")
        if not operator and not left_name and not right_name:
            continue
        inputs = [value for value in [left_name, right_name] if value]
        generated_factors.append(
            {
                "name": name,
                "operator": operator,
                "inputs": inputs,
            }
        )
        if left_name:
            edges.append({"from": left_name, "to": name, "role": "left"})
        if right_name:
            edges.append({"from": right_name, "to": name, "role": "right"})
    return {
        "node_names": node_names,
        "node_count": len(node_names),
        "generated_factor_count": len(generated_factors),
        "generated_factors": generated_factors,
        "edges": edges,
    }


def upgrade_generated_config(config: dict[str, Any], *, source: str | None = None) -> dict[str, Any]:
    upgraded = deepcopy(config)
    legacy_schema = upgraded.get("schema_version")
    upgraded["schema_version"] = GENERATED_CONFIG_SCHEMA_VERSION
    upgraded["artifact_type"] = "generated_workflow_config"
    if legacy_schema and legacy_schema != GENERATED_CONFIG_SCHEMA_VERSION:
        upgraded.setdefault("legacy_schema_version", legacy_schema)
    if source:
        upgraded.setdefault("generated_source", source)
    upgraded["dependency_graph"] = build_dependency_graph(list(upgraded.get("factors") or []))
    return upgraded


def upgrade_generated_batch(batch: dict[str, Any], *, source: str | None = None) -> dict[str, Any]:
    upgraded = deepcopy(batch)
    legacy_schema = upgraded.get("schema_version")
    upgraded["schema_version"] = GENERATED_BATCH_SCHEMA_VERSION
    upgraded["artifact_type"] = "generated_batch"
    if legacy_schema and legacy_schema != GENERATED_BATCH_SCHEMA_VERSION:
        upgraded.setdefault("legacy_schema_version", legacy_schema)
    if source:
        upgraded.setdefault("generated_source", source)
    jobs = [row for row in (upgraded.get("jobs") or []) if isinstance(row, dict)]
    upgraded["job_count"] = len(jobs)
    upgraded["job_names"] = [
        str(row.get("name") or row.get("job_name") or f"job_{idx}")
        for idx, row in enumerate(jobs)
    ]
    return upgraded
