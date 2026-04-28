from factor_lab.candidate_failure_dossier import build_candidate_failure_dossier
from factor_lab.research_candidate_pool import _representative_focus_candidates


def test_candidate_failure_dossier_flags_decay_and_parent_non_incremental():
    candidate = {
        "id": "cand-1",
        "name": "hybrid_mom_quality",
        "avg_final_score": 1.0,
        "latest_recent_final_score": 1.2,
    }
    parent = {
        "id": "cand-2",
        "name": "mom_20",
        "avg_final_score": 1.4,
        "latest_recent_final_score": 1.5,
    }
    evaluations = [
        {
            "candidate_id": "cand-1",
            "window_label": "recent_45d",
            "final_score": 2.4,
            "status": "promising",
            "raw_rank_ic_mean": 0.18,
            "neutralized_rank_ic_mean": 0.05,
            "split_fail_count": 0,
            "created_at_utc": "2026-04-01T00:00:00+00:00",
        },
        {
            "candidate_id": "cand-1",
            "window_label": "recent_90d",
            "final_score": 0.3,
            "status": "fragile",
            "raw_rank_ic_mean": 0.12,
            "neutralized_rank_ic_mean": -0.01,
            "split_fail_count": 1,
            "created_at_utc": "2026-04-02T00:00:00+00:00",
        },
        {
            "candidate_id": "cand-1",
            "window_label": "recent_120d",
            "final_score": 0.1,
            "status": "fragile",
            "raw_rank_ic_mean": 0.1,
            "neutralized_rank_ic_mean": -0.02,
            "split_fail_count": 1,
            "created_at_utc": "2026-04-03T00:00:00+00:00",
        },
    ]
    relationships = [
        {
            "left_name": "hybrid_mom_quality",
            "right_name": "mom_20",
            "relationship_type": "refinement_of",
            "details": {"parent_candidate": "mom_20"},
        }
    ]

    dossier = build_candidate_failure_dossier(
        candidate,
        evaluations,
        relationships,
        {"hybrid_mom_quality": candidate, "mom_20": parent},
    )

    assert "short_to_medium_decay" in dossier["failure_modes"]
    assert "neutralized_break" in dossier["failure_modes"]
    assert dossier["parent_delta_status"] == "non_incremental"
    assert dossier["recommended_action"] == "diagnose"


def test_representative_focus_candidates_suppresses_nonrepresentative_and_suppress_action():
    names = ["variant_a", "rep_a", "rep_b"]
    candidate_context_by_name = {
        "variant_a": {"cluster": {"primary_candidate": "rep_a", "representative_candidates": ["rep_a"]}},
        "rep_a": {"cluster": {"primary_candidate": "rep_a", "representative_candidates": ["rep_a"]}},
        "rep_b": {"cluster": {"primary_candidate": "rep_b", "representative_candidates": ["rep_b"]}},
    }
    cluster_rep_map = {
        "rep_a": {"cluster_id": 1, "representative_rank": 1, "representative_count": 1, "is_primary_representative": True},
        "rep_b": {"cluster_id": 2, "representative_rank": 1, "representative_count": 1, "is_primary_representative": True},
    }
    failure_map = {
        "rep_b": {"recommended_action": "suppress"},
    }

    selected, notes = _representative_focus_candidates(names, candidate_context_by_name, cluster_rep_map, failure_map)

    assert selected == ["rep_a"]
    assert any(row.get("suppressed_into") == "rep_a" for row in notes)
