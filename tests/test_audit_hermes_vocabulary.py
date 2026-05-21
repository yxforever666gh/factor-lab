from pathlib import Path

from scripts.audit_hermes_vocabulary import find_vocabulary_violations


def test_audit_finds_old_public_terms(tmp_path):
    p = tmp_path / "src" / "bad.py"
    p.parent.mkdir()
    p.write_text("failure" + "_analyst = 'x'\n")
    violations = find_vocabulary_violations([tmp_path])
    assert violations
    assert violations[0].term == ("failure" + "_analyst")


def test_audit_allows_legacy_compat(tmp_path):
    p = tmp_path / "src" / "factor_lab" / "legacy_compat" / "old.py"
    p.parent.mkdir(parents=True)
    p.write_text("failure" + "_analyst = 'x'\n")
    assert find_vocabulary_violations([tmp_path]) == []
