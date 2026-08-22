from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_webui_hardening_structure_exists_and_main_app_stays_bounded():
    webui_app = ROOT / "src" / "factor_lab" / "webui_app.py"
    line_count = len(webui_app.read_text(encoding="utf-8").splitlines())

    assert line_count <= 3000
    assert (ROOT / "src" / "factor_lab" / "webui" / "app.py").exists()
    assert (ROOT / "src" / "factor_lab" / "webui" / "services" / "env_settings.py").exists()
    assert (ROOT / "src" / "factor_lab" / "webui" / "services" / "service_restart.py").exists()
    for route_module in [
        "settings_data_sources.py",
        "settings_llm.py",
    ]:
        assert (ROOT / "src" / "factor_lab" / "webui" / "routes" / route_module).exists()


def test_script_namespace_policy_and_directories_exist():
    for dirname in ["prod", "ops", "reports", "devtools", "archive"]:
        namespace = ROOT / "scripts" / dirname
        assert namespace.exists()
        assert (namespace / "README.md").exists()

    doc = ROOT / "docs" / "ops" / "script-entrypoints.md"
    text = doc.read_text(encoding="utf-8")
    assert "Top-level scripts remain the compatibility surface" in text
    assert "scripts/reports/" in text


def test_retention_and_research_authority_docs_exist():
    required_docs = [
        "docs/ops/research-lines.md",
        "docs/ops/artifact-retention.md",
        "docs/ops/db-retention.md",
    ]
    for rel in required_docs:
        path = ROOT / rel
        text = path.read_text(encoding="utf-8")
        assert text.strip()

    research_lines = (ROOT / "docs" / "ops" / "research-lines.md").read_text(encoding="utf-8")
    assert "ASL" in research_lines
    assert "Harvest cycle-local verdicts" in research_lines

    artifact_policy = (ROOT / "docs" / "ops" / "artifact-retention.md").read_text(encoding="utf-8")
    assert "Archive-first procedure" in artifact_policy
    assert "artifacts/factor_lab.db" in artifact_policy

    db_policy = (ROOT / "docs" / "ops" / "db-retention.md").read_text(encoding="utf-8")
    assert "backup" in db_policy.lower()
    assert "VACUUM" in db_policy


def test_hardening_audit_inputs_are_version_controlled():
    # Generated audit reports live under ignored ``artifacts/`` and are not
    # fixtures.  A clean checkout must instead retain the generator and the
    # reviewed policy documents needed to reproduce or interpret an audit.
    required = [
        "scripts/ops/write_module_inventory.py",
        "docs/ops/research-lines.md",
        "docs/ops/artifact-retention.md",
        "docs/ops/db-retention.md",
    ]
    for relative_path in required:
        path = ROOT / relative_path
        assert path.exists(), relative_path
        assert path.stat().st_size > 0
