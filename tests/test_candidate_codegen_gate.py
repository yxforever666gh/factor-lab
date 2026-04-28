import json
from pathlib import Path

from scripts.check_candidate_codegen_policy import run_checks


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_candidate_codegen_gate_templates_exist():
    generator_template = REPO_ROOT / "configs" / "templates" / "candidate_generator_template.json"
    compiler_template = REPO_ROOT / "configs" / "templates" / "candidate_compiler_template.json"

    assert generator_template.exists()
    assert compiler_template.exists()


def test_candidate_codegen_policy_check_returns_clean_summary():
    result = run_checks()

    assert result["policy_name"] == "openclaw_candidate_generation_policy"
    assert "summary" in result
    assert result["summary"]["generator_mentions_cheap_screen"] is True
    assert result["summary"]["compiler_mentions_generation_context"] is True
