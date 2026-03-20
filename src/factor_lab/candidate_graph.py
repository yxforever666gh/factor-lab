from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REL_HIGH_CORR = "high_corr"
REL_CLUSTER = "cluster_peer"
REL_SAME_FAMILY = "same_family"


def canonical_pair(left: str, right: str) -> tuple[str, str]:
    return (left, right) if left <= right else (right, left)


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
        clusters.append(
            {
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
    return clusters


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
    families = family_rollup(candidates, evaluations)
    clusters = candidate_clusters(candidates, relationships)

    family_path = output_dir / "family_summary.json"
    cluster_path = output_dir / "candidate_clusters.json"
    relationship_path = output_dir / "candidate_relationships.json"
    family_path.write_text(json.dumps(families, ensure_ascii=False, indent=2), encoding="utf-8")
    cluster_path.write_text(json.dumps(clusters, ensure_ascii=False, indent=2), encoding="utf-8")
    relationship_path.write_text(json.dumps(relationships, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "family_summary_path": str(family_path),
        "candidate_clusters_path": str(cluster_path),
        "candidate_relationships_path": str(relationship_path),
        "family_count": len(families),
        "cluster_count": len(clusters),
        "relationship_count": len(relationships),
    }
