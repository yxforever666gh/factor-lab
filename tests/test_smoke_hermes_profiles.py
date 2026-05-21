from scripts.smoke_hermes_profiles import smoke_requests


def test_smoke_requests_cover_all_profiles(tmp_path):
    requests = smoke_requests(artifact_dir=tmp_path)
    assert [r.profile_name for r in requests] == [
        "factor-lab-researcher",
        "factor-lab-diagnostician",
        "factor-lab-reviewer",
        "factor-lab-data-steward",
    ]
    assert all(str(r.response_path).startswith(str(tmp_path)) for r in requests)
