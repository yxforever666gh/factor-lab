import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_candidate_generator_template_exists():
    path = REPO_ROOT / "configs" / "templates" / "candidate_generator_template.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["template_name"] == "candidate_generator_template"
    assert "attach cheap_screen result to every proposal before compilation" in payload["required_structure"]


def test_candidate_compiler_template_exists():
    path = REPO_ROOT / "configs" / "templates" / "candidate_compiler_template.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["template_name"] == "candidate_compiler_template"
    assert "compile only proposals that passed cheap_screen" in payload["required_structure"]
