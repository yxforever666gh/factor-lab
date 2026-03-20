from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REL_HIGH_CORR = "high_corr"
REL_CLUSTER = "cluster_peer"
REL_SAME_FAMILY = "same_family"
REL_DUPLICATE = "duplicate_of"
REL_REFINEMENT = "refinement_of"
REL_HYBRID = "hybrid_of"

SEMANTIC_ALIAS = {
    "mom": "momentum",
    "momentum": "momentum",
    "ep": "earnings_yield",
    "earnings": "earnings_yield",
    "earnings_yield": "earnings_yield",
    "bp": "book_yield",
    "book": "book_yield",
    "book_yield": "book_yield",
    "pb": "book_yield",
    "roe": "roe",
    "quality": "roe",
    "size": "size_inv",
    "size_inv": "size_inv",
    "liquidity": "turnover_shock",
    "turnover": "turnover_shock",
    "turnover_shock": "turnover_shock",
    "value": "value_proxy",
}

GENERIC_TOKENS = {
    "plus", "minus", "and", "or", "raw", "neutral", "neutralized", "rank", "score",
    "alpha", "factor", "signal", "window", "recent", "rolling", "expanding", "yield",
}


def canonical_pair(left: str, right: str) -> tuple[str, str]:
    return (left, right) if left <= right else (right, left)


def _normalize_expression(value: str | None) -> str:
    text = (value or "").strip().lower()
    return re.sub(r"\s+", "", text)


def _split_tokens(value: str | None) -> list[str]:
    text = (value or "").lower()
    return [token for token in re.split(r"[^a-z0-9]+", text) if token]


def _semantic_tokens(candidate: dict[str, Any]) -> set[str]:
    raw_tokens = _split_tokens(candidate.get("name")) + _split_tokens(candidate.get("expression"))
    semantic = set()
    for token in raw_tokens:
        if token.isdigit() or token in GENERIC_TOKENS:
            continue
        semantic.add(SEMANTIC_ALIAS.get(token, token))
    return semantic


def _operator_count(expression: str | None) -> int:
    text = expression or ""
    return sum(text.count(op) for op in ["+", "-", "*", "/"])


def _candidate_profile(candidate: dict[str, Any]) -> dict[str, Any]:
    expression = candidate.get("expression") or ""
    semantic_tokens = _semantic_tokens(candidate)
    family = candidate.get("family") or "other"
    atoms = {token for token in semantic_tokens if token not in {"value_proxy", family}}
    if not atoms and family != "other":
        atoms = {family}
    return {
        "id": candidate["id"],
        "name": candidate.get("name"),
        "family": family,
        "expression": expression,
        "normalized_expression": _normalize_expression(expression),
        "semantic_tokens": semantic_tokens,
        "atoms": atoms,
        "operator_count": _operator_count(expression),
        "is_hybrid": len(atoms) >= 2 or _operator_count(expression) > 0,
    }


def build_candidate_relationships(
    *,
    candidates: list[dict[str, Any]],
    candidate_id_by_name: dict[str, str],
    family_by_name: dict[str, str],
    correlation_lookup: dict[str, list[str]],
    clusters: list[list[str]],
    run_id: str,
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    candidate_names = {row.get("factor_name") for row in candidates}
    candidate_records = []
    profile_by_name: dict[str, dict[str, Any]] = {}
    for row in candidates:
        record = {
            "id": candidate_id_by_name.get(row.get("factor_name")),
            "name": row.get("factor_name"),
            "family": family_by_name.get(row.get("factor_name")) or row.get("family") or "other",
            "expression": row.get("expression"),
        }
        if record["id"] and record["name"]:
            candidate_records.append(record)
            profile_by_name[record["name"]] = _candidate_profile(record)

    def put(name_a: str, name_b: str, rel_type: str, strength: float, details: dict[str, Any] | None = None) -> None:
        if name_a == name_b or name_a not in candidate_names or name_b not in candidate_names:
            return
        id_a = candidate_id_by_name.get(name_a)
        id_b = candidate_id_by_name.get(name_b)
        if not id_a or not id_b:
            return
        left_id, right_id = canonical_pair(id_a, id_b)
        left_name, right_name = canonical_pair(name_a, name_b)
        key = (left_id, right_id, rel_type)
        rows[key] = {
            "left_candidate_id": left_id,
            "right_candidate_id": right_id,
            "left_name": left_name,
            "right_name": right_name,
            "relationship_type": rel_type,
            "strength": round(float(strength), 6),
            "run_id": run_id,
            "details": details or {},
        }

    for name, peers in correlation_lookup.items():
        if name not in candidate_names:
            continue
        peer_list = [peer for peer in peers if peer in candidate_names and peer != name]
        for peer in peer_list:
            put(name, peer, REL_HIGH_CORR, 1.0, {"peer_count": len(peer_list)})

    for cluster_index, members in enumerate(clusters, start=1):
        members = [name for name in members if name in candidate_names]
        if len(members) < 2:
            continue
        cluster_strength = min(1.0, 0.35 + 0.15 * len(members))
        for idx, left in enumerate(members):
            for right in members[idx + 1 :]:
                put(
                    left,
                    right,
                    REL_CLUSTER,
                    cluster_strength,
                    {"cluster_index": cluster_index, "cluster_size": len(members), "members": members},
                )

    family_groups: dict[str, list[str]] = defaultdict(list)
    for name in candidate_names:
        family = family_by_name.get(name) or "other"
        family_groups[family].append(name)
    for family, members in family_groups.items():
        if len(members) < 2:
            continue
        strength = min(0.95, 0.2 + 0.1 * len(members))
        for idx, left in enumerate(sorted(members)):
            for right in sorted(members)[idx + 1 :]:
                put(left, right, REL_SAME_FAMILY, strength, {"family": family, "family_size": len(members)})

    profiles = list(profile_by_name.values())
    for idx, left in enumerate(profiles):
        for right in profiles[idx + 1 :]:
            left_atoms = set(left["atoms"])
            right_atoms = set(right["atoms"])
            shared_atoms = sorted(left_atoms & right_atoms)
            union_atoms = sorted(left_atoms | right_atoms)
            if not union_atoms:
                continue

            if left["normalized_expression"] and left["normalized_expression"] == right["normalized_expression"]:
                put(
                    left["name"],
                    right["name"],
                    REL_DUPLICATE,
                    0.98,
                    {"basis": "normalized_expression_match", "shared_atoms": shared_atoms},
                )
                continue

            if left_atoms == right_atoms and left_atoms and left["family"] == right["family"]:
                put(
                    left["name"],
                    right["name"],
                    REL_DUPLICATE,
                    0.9,
                    {"basis": "semantic_atom_match", "shared_atoms": shared_atoms, "families": [left["family"], right["family"]]},
                )
                continue

            if left_atoms and right_atoms and left_atoms.issubset(right_atoms) and len(right_atoms) > len(left_atoms):
                extra_atoms = sorted(right_atoms - left_atoms)
                put(
                    left["name"],
                    right["name"],
                    REL_REFINEMENT,
                    min(0.92, 0.62 + 0.1 * len(shared_atoms)),
                    {
                        "basis": "atom_superset",
                        "parent_candidate": left["name"] if left["operator_count"] <= right["operator_count"] else right["name"],
                        "child_candidate": right["name"] if left["operator_count"] <= right["operator_count"] else left["name"],
                        "shared_atoms": shared_atoms,
                        "extra_atoms": extra_atoms,
                    },
                )
                continue
            if left_atoms and right_atoms and right_atoms.issubset(left_atoms) and len(left_atoms) > len(right_atoms):
                extra_atoms = sorted(left_atoms - right_atoms)
                put(
                    left["name"],
                    right["name"],
                    REL_REFINEMENT,
                    min(0.92, 0.62 + 0.1 * len(shared_atoms)),
                    {
                        "basis": "atom_superset",
                        "parent_candidate": right["name"] if right["operator_count"] <= left["operator_count"] else left["name"],
                        "child_candidate": left["name"] if right["operator_count"] <= left["operator_count"] else right["name"],
                        "shared_atoms": shared_atoms,
                        "extra_atoms": extra_atoms,
                    },
                )
                continue

            if shared_atoms and (left["is_hybrid"] or right["is_hybrid"]):
                if left["operator_count"] > right["operator_count"]:
                    hybrid_candidate = left
                elif right["operator_count"] > left["operator_count"]:
                    hybrid_candidate = right
                else:
                    hybrid_candidate = left if left["is_hybrid"] and len(left_atoms) >= len(right_atoms) else right
                source_candidate = right if hybrid_candidate is left else left
                hybrid_atoms = set(hybrid_candidate["atoms"])
                if len(hybrid_atoms) >= 2 and hybrid_candidate["operator_count"] > 0 and set(source_candidate["atoms"]) & hybrid_atoms:
                    put(
                        left["name"],
                        right["name"],
                        REL_HYBRID,
                        min(0.88, 0.55 + 0.08 * len(shared_atoms)),
                        {
                            "basis": "hybrid_component_overlap",
                            "hybrid_candidate": hybrid_candidate["name"],
                            "component_candidate": source_candidate["name"],
                            "shared_atoms": shared_atoms,
                            "hybrid_atoms": sorted(hybrid_atoms),
                        },
                    )

    return list(rows.values())


def family_rollup(candidates: list[dict[str, Any]], evaluations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_candidate = {row["id"]: row for row in candidates}
    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "family": "other",
        "candidate_count": 0,
        "evaluation_count": 0,
        "window_labels": set(),
        "status_counter": Counter(),
        "avg_scores": [],
        "latest_scores": [],
        "best_scores": [],
        "promising_count": 0,
        "testing_count": 0,
        "rejected_count": 0,
    })

    for candidate in candidates:
        family = candidate.get("family") or "other"
        bucket = grouped[family]
        bucket["family"] = family
        bucket["candidate_count"] += 1
        status = candidate.get("status") or "new"
        bucket["status_counter"][status] += 1
        if status == "promising":
            bucket["promising_count"] += 1
        elif status == "testing":
            bucket["testing_count"] += 1
        elif status in {"rejected", "archived"}:
            bucket["rejected_count"] += 1
        if candidate.get("avg_final_score") is not None:
            bucket["avg_scores"].append(float(candidate["avg_final_score"]))
        if candidate.get("latest_final_score") is not None:
            bucket["latest_scores"].append(float(candidate["latest_final_score"]))
        if candidate.get("best_final_score") is not None:
            bucket["best_scores"].append(float(candidate["best_final_score"]))

    for evaluation in evaluations:
        candidate = by_candidate.get(evaluation.get("candidate_id"))
        if not candidate:
            continue
        family = candidate.get("family") or "other"
        bucket = grouped[family]
        bucket["evaluation_count"] += 1
        bucket["window_labels"].add(evaluation.get("window_label") or "unknown")

    rows = []
    for family, bucket in grouped.items():
        candidate_count = bucket["candidate_count"]
        avg_latest = round(sum(bucket["latest_scores"]) / len(bucket["latest_scores"]), 6) if bucket["latest_scores"] else None
        avg_best = round(sum(bucket["best_scores"]) / len(bucket["best_scores"]), 6) if bucket["best_scores"] else None
        avg_score = round(sum(bucket["avg_scores"]) / len(bucket["avg_scores"]), 6) if bucket["avg_scores"] else None
        status_counter = bucket["status_counter"]
        score = 0.0
        score += min(bucket["promising_count"] * 18, 54)
        score += min(bucket["testing_count"] * 7, 21)
        score += min(len(bucket["window_labels"]) * 3, 15)
        score += max((avg_score or 0.0) * 8, 0)
        score -= min(bucket["rejected_count"] * 4, 20)
        rows.append(
            {
                "family": family,
                "candidate_count": candidate_count,
                "evaluation_count": bucket["evaluation_count"],
                "window_count": len(bucket["window_labels"]),
                "promising_count": bucket["promising_count"],
                "testing_count": bucket["testing_count"],
                "rejected_count": bucket["rejected_count"],
                "top_status": status_counter.most_common(1)[0][0] if status_counter else "new",
                "avg_candidate_score": avg_score,
                "avg_latest_score": avg_latest,
                "avg_best_score": avg_best,
                "family_score": round(score, 6),
            }
        )
    rows.sort(key=lambda row: (-row["family_score"], -row["candidate_count"], row["family"]))
    return rows


def candidate_clusters(candidates: list[dict[str, Any]], relationships: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {row["id"]: row for row in candidates}
    adjacency: dict[str, set[str]] = {row["id"]: set() for row in candidates}
    rels_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    cluster_index_by_candidate: dict[str, int] = {}
    for rel in relationships:
        left = rel["left_candidate_id"]
        right = rel["right_candidate_id"]
        adjacency.setdefault(left, set()).add(right)
        adjacency.setdefault(right, set()).add(left)
        rels_by_pair[canonical_pair(left, right)].append(rel)

    seen: set[str] = set()
    clusters = []
    for candidate_id in adjacency:
        if candidate_id in seen:
            continue
        stack = [candidate_id]
        component = []
        seen.add(candidate_id)
        while stack:
            current = stack.pop()
            component.append(current)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in seen:
                    seen.add(neighbor)
                    stack.append(neighbor)
        members = [by_id[cid] for cid in component if cid in by_id]
        if not members:
            continue
        families = Counter((row.get("family") or "other") for row in members)
        edge_types = Counter()
        strengths = []
        edges = 0
        for idx, left in enumerate(component):
            for right in component[idx + 1 :]:
                pair_rels = rels_by_pair.get(canonical_pair(left, right), [])
                if not pair_rels:
                    continue
                edges += len(pair_rels)
                for rel in pair_rels:
                    edge_types[rel["relationship_type"]] += 1
                    strengths.append(float(rel.get("strength") or 0.0))
        members_sorted = sorted(
            members,
            key=lambda row: (
                -(float(row.get("latest_final_score") or -999.0)),
                -(int(row.get("evaluation_count") or 0)),
                row.get("name") or "",
            ),
        )
        cluster_id = len(clusters) + 1
        for member_id in component:
            cluster_index_by_candidate[member_id] = cluster_id
        clusters.append(
            {
                "cluster_id": cluster_id,
                "cluster_key": "::".join(sorted(component)),
                "cluster_size": len(members_sorted),
                "edge_count": edges,
                "avg_strength": round(sum(strengths) / len(strengths), 6) if strengths else None,
                "dominant_family": families.most_common(1)[0][0] if families else "other",
                "family_mix": dict(families),
                "relationship_mix": dict(edge_types),
                "leader": members_sorted[0].get("name"),
                "members": [
                    {
                        "id": row["id"],
                        "name": row.get("name"),
                        "family": row.get("family") or "other",
                        "status": row.get("status"),
                        "latest_final_score": row.get("latest_final_score"),
                        "avg_final_score": row.get("avg_final_score"),
                        "evaluation_count": row.get("evaluation_count"),
                    }
                    for row in members_sorted
                ],
            }
        )
    clusters.sort(key=lambda row: (-row["cluster_size"], -(row["avg_strength"] or 0.0), row["leader"] or ""))
    for idx, cluster in enumerate(clusters, start=1):
        cluster["cluster_id"] = idx
        for member in cluster["members"]:
            cluster_index_by_candidate[member["id"]] = idx
    return clusters


def build_candidate_graph_context(candidates: list[dict[str, Any]], evaluations: list[dict[str, Any]], relationships: list[dict[str, Any]]) -> dict[str, Any]:
    families = family_rollup(candidates, evaluations)
    clusters = candidate_clusters(candidates, relationships)
    family_score_by_name = {row["family"]: row for row in families}
    cluster_by_candidate: dict[str, dict[str, Any]] = {}
    for cluster in clusters:
        for member in cluster["members"]:
            cluster_by_candidate[member["id"]] = {
                "cluster_id": cluster["cluster_id"],
                "cluster_size": cluster["cluster_size"],
                "leader": cluster.get("leader"),
                "dominant_family": cluster.get("dominant_family"),
                "relationship_mix": cluster.get("relationship_mix") or {},
                "members": cluster["members"],
            }

    relationship_summary = Counter(rel["relationship_type"] for rel in relationships)
    relationships_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rel in relationships:
        relationships_by_candidate[rel["left_candidate_id"]].append(rel)
        relationships_by_candidate[rel["right_candidate_id"]].append(rel)

    candidate_context = []
    for candidate in candidates:
        rels = sorted(relationships_by_candidate.get(candidate["id"], []), key=lambda row: (-(row.get("strength") or 0.0), row.get("relationship_type") or ""))
        related = []
        lineage = []
        for rel in rels:
            other_id = rel["right_candidate_id"] if rel["left_candidate_id"] == candidate["id"] else rel["left_candidate_id"]
            other_name = rel["right_name"] if rel["left_candidate_id"] == candidate["id"] else rel["left_name"]
            item = {
                "candidate_id": other_id,
                "candidate_name": other_name,
                "relationship_type": rel.get("relationship_type"),
                "strength": rel.get("strength"),
                "details": rel.get("details") or {},
            }
            related.append(item)
            if rel.get("relationship_type") in {REL_DUPLICATE, REL_REFINEMENT, REL_HYBRID}:
                lineage.append(item)
        family = candidate.get("family") or "other"
        candidate_context.append(
            {
                "candidate_id": candidate["id"],
                "candidate_name": candidate.get("name"),
                "family": family,
                "status": candidate.get("status"),
                "family_score": (family_score_by_name.get(family) or {}).get("family_score"),
                "relationship_count": len(rels),
                "lineage_count": len(lineage),
                "cluster": cluster_by_candidate.get(candidate["id"]),
                "related_candidates": related,
                "lineage": lineage,
            }
        )

    return {
        "families": families,
        "clusters": clusters,
        "relationship_summary": dict(relationship_summary),
        "candidate_context": candidate_context,
    }


def _backfill_same_family_relationships(store: Any, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    family_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        family_groups[candidate.get('family') or 'other'].append(candidate)
    created = []
    for family, members in family_groups.items():
        if len(members) < 2:
            continue
        strength = min(0.95, 0.2 + 0.1 * len(members))
        members = sorted(members, key=lambda row: row.get('name') or '')
        for idx, left in enumerate(members):
            for right in members[idx + 1:]:
                payload = {
                    'left_candidate_id': left['id'],
                    'right_candidate_id': right['id'],
                    'relationship_type': REL_SAME_FAMILY,
                    'run_id': None,
                    'strength': round(strength, 6),
                    'details': {'family': family, 'family_size': len(members), 'source': 'backfill'},
                }
                store.upsert_candidate_relationship(**payload)
                created.append(payload)
    return created


def infer_relationships_from_existing_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profiles = [_candidate_profile(candidate) for candidate in candidates]
    rows = []
    for idx, left in enumerate(profiles):
        for right in profiles[idx + 1 :]:
            shared_atoms = sorted(set(left['atoms']) & set(right['atoms']))
            if left['normalized_expression'] and left['normalized_expression'] == right['normalized_expression']:
                rows.append({
                    'left_candidate_id': left['id'], 'right_candidate_id': right['id'], 'relationship_type': REL_DUPLICATE,
                    'run_id': None, 'strength': 0.98,
                    'details': {'basis': 'normalized_expression_match', 'shared_atoms': shared_atoms, 'source': 'backfill'},
                })
                continue
            if left['atoms'] and right['atoms'] and left['atoms'] == right['atoms'] and left['family'] == right['family']:
                rows.append({
                    'left_candidate_id': left['id'], 'right_candidate_id': right['id'], 'relationship_type': REL_DUPLICATE,
                    'run_id': None, 'strength': 0.9,
                    'details': {'basis': 'semantic_atom_match', 'shared_atoms': shared_atoms, 'source': 'backfill'},
                })
                continue
            if left['atoms'] and right['atoms'] and left['atoms'].issubset(right['atoms']) and len(right['atoms']) > len(left['atoms']):
                rows.append({
                    'left_candidate_id': left['id'], 'right_candidate_id': right['id'], 'relationship_type': REL_REFINEMENT,
                    'run_id': None, 'strength': min(0.92, 0.62 + 0.1 * len(shared_atoms)),
                    'details': {'basis': 'atom_superset', 'parent_candidate': left['name'], 'child_candidate': right['name'], 'shared_atoms': shared_atoms, 'extra_atoms': sorted(set(right['atoms']) - set(left['atoms'])), 'source': 'backfill'},
                })
                continue
            if left['atoms'] and right['atoms'] and right['atoms'].issubset(left['atoms']) and len(left['atoms']) > len(right['atoms']):
                rows.append({
                    'left_candidate_id': left['id'], 'right_candidate_id': right['id'], 'relationship_type': REL_REFINEMENT,
                    'run_id': None, 'strength': min(0.92, 0.62 + 0.1 * len(shared_atoms)),
                    'details': {'basis': 'atom_superset', 'parent_candidate': right['name'], 'child_candidate': left['name'], 'shared_atoms': shared_atoms, 'extra_atoms': sorted(set(left['atoms']) - set(right['atoms'])), 'source': 'backfill'},
                })
                continue
            if left['operator_count'] > right['operator_count']:
                hybrid_candidate = left
            elif right['operator_count'] > left['operator_count']:
                hybrid_candidate = right
            else:
                hybrid_candidate = left if left['is_hybrid'] and len(left['atoms']) >= len(right['atoms']) else right
            source_candidate = right if hybrid_candidate is left else left
            if shared_atoms and hybrid_candidate['is_hybrid'] and hybrid_candidate['operator_count'] > 0 and set(source_candidate['atoms']) & set(hybrid_candidate['atoms']):
                rows.append({
                    'left_candidate_id': left['id'], 'right_candidate_id': right['id'], 'relationship_type': REL_HYBRID,
                    'run_id': None, 'strength': min(0.88, 0.55 + 0.08 * len(shared_atoms)),
                    'details': {'basis': 'hybrid_component_overlap', 'hybrid_candidate': hybrid_candidate['name'], 'component_candidate': source_candidate['name'], 'shared_atoms': shared_atoms, 'hybrid_atoms': sorted(hybrid_candidate['atoms']), 'source': 'backfill'},
                })
    return rows


def build_graph_artifacts(db_path: str | Path, output_dir: str | Path) -> dict[str, Any]:
    from factor_lab.storage import ExperimentStore

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    store = ExperimentStore(db_path)
    candidates = store.list_factor_candidates(limit=1000)
    evaluations = store.list_factor_evaluations(limit=5000)
    relationships = store.list_candidate_relationships(limit=5000)
    if not relationships and candidates:
        _backfill_same_family_relationships(store, candidates)
        relationships = store.list_candidate_relationships(limit=5000)
    if candidates:
        existing_keys = {
            (canonical_pair(rel['left_candidate_id'], rel['right_candidate_id'])[0], canonical_pair(rel['left_candidate_id'], rel['right_candidate_id'])[1], rel['relationship_type'])
            for rel in relationships
        }
        for payload in infer_relationships_from_existing_candidates(candidates):
            left_id, right_id = canonical_pair(payload['left_candidate_id'], payload['right_candidate_id'])
            key = (left_id, right_id, payload['relationship_type'])
            if key in existing_keys:
                continue
            store.upsert_candidate_relationship(**payload)
            existing_keys.add(key)
        relationships = store.list_candidate_relationships(limit=5000)
    graph_context = build_candidate_graph_context(candidates, evaluations, relationships)
    families = graph_context["families"]
    clusters = graph_context["clusters"]

    family_path = output_dir / "family_summary.json"
    cluster_path = output_dir / "candidate_clusters.json"
    relationship_path = output_dir / "candidate_relationships.json"
    context_path = output_dir / "candidate_graph_context.json"
    family_path.write_text(json.dumps(families, ensure_ascii=False, indent=2), encoding="utf-8")
    cluster_path.write_text(json.dumps(clusters, ensure_ascii=False, indent=2), encoding="utf-8")
    relationship_path.write_text(json.dumps(relationships, ensure_ascii=False, indent=2), encoding="utf-8")
    context_path.write_text(json.dumps(graph_context, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "family_summary_path": str(family_path),
        "candidate_clusters_path": str(cluster_path),
        "candidate_relationships_path": str(relationship_path),
        "candidate_graph_context_path": str(context_path),
        "family_count": len(families),
        "cluster_count": len(clusters),
        "relationship_count": len(relationships),
    }
