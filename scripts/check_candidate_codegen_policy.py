from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GENERATOR_PATH = ROOT / "src" / "factor_lab" / "candidate_generator.py"
COMPILER_PATH = ROOT / "src" / "factor_lab" / "candidate_compiler.py"
GENERATOR_TEMPLATE_PATH = ROOT / "configs" / "templates" / "candidate_generator_template.json"
COMPILER_TEMPLATE_PATH = ROOT / "configs" / "templates" / "candidate_compiler_template.json"
POLICY_PATH = ROOT / "configs" / "candidate_generation_policy.json"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def run_checks() -> dict:
    generator = _read(GENERATOR_PATH)
    compiler = _read(COMPILER_PATH)
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8")) if POLICY_PATH.exists() else {}
    findings: list[dict] = []

    if "cheap_screen" not in generator:
        findings.append({
            "severity": "warning",
            "rule": "generator_missing_cheap_screen",
            "message": "candidate_generator.py does not appear to attach cheap_screen results to proposals.",
        })
    if "expected_information_gain" not in generator:
        findings.append({
            "severity": "warning",
            "rule": "generator_missing_expected_information_gain",
            "message": "candidate_generator.py should attach expected_information_gain to generated proposals.",
        })
    if "candidate_generation_context" not in compiler:
        findings.append({
            "severity": "warning",
            "rule": "compiler_missing_generation_context",
            "message": "candidate_compiler.py should preserve candidate_generation_context in compiled payloads.",
        })
    if "cheap_screen" not in compiler or 'pass' not in compiler:
        findings.append({
            "severity": "warning",
            "rule": "compiler_missing_cheap_screen_gate",
            "message": "candidate_compiler.py should filter proposals using cheap_screen pass/fail before compiling.",
        })
    if int(((policy.get("limits") or {}).get("max_operators_per_pair") or 0)) <= 0:
        findings.append({
            "severity": "warning",
            "rule": "policy_missing_pair_operator_limit",
            "message": "candidate_generation_policy.json should cap operator count per base pair.",
        })

    return {
        "policy_name": policy.get("name"),
        "checked_files": [str(GENERATOR_PATH), str(COMPILER_PATH)],
        "findings": findings,
        "summary": {
            "generator_mentions_cheap_screen": "cheap_screen" in generator,
            "generator_mentions_expected_gain": "expected_information_gain" in generator,
            "compiler_mentions_generation_context": "candidate_generation_context" in compiler,
            "compiler_mentions_cheap_screen": "cheap_screen" in compiler,
            "max_operators_per_pair": ((policy.get("limits") or {}).get("max_operators_per_pair")),
        },
    }


if __name__ == "__main__":
    result = run_checks()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["findings"]:
        raise SystemExit(1)
