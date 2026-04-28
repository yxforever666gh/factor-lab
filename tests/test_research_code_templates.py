import json
from pathlib import Path

from scripts.check_workflow_codegen_policy import run_checks


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_factor_eval_template_exists():
    path = REPO_ROOT / "configs" / "templates" / "factor_eval_template.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["template_name"] == "factor_eval_template"
    assert "reuse cached factor values for rolling evaluation" in payload["required_structure"]


def test_portfolio_eval_template_exists():
    path = REPO_ROOT / "configs" / "templates" / "portfolio_eval_template.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["template_name"] == "portfolio_eval_template"
    assert "avoid recomputing factor expressions inside portfolio evaluation" in payload["required_structure"]


def test_workflow_codegen_policy_check_returns_summary():
    result = run_checks()

    assert result["policy_name"] == "openclaw_autonomous_research_coding_policy"
    assert "summary" in result
    assert "workflow_apply_factor_count" in result["summary"]
