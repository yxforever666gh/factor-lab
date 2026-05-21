#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

FORBIDDEN_TERMS = [
    "AgentRoleConfig",
    "DecisionProviderRouter",
    "agent_roles",
    "agent_briefs",
    "agent_runtime_hooks",
    "agent_schemas",
    "agent_responses",
    "llm_provider_router",
    "planner_agent",
    "failure_analyst",
    "data_quality",
    "real_llm",
    "openclaw",
    "old claw",
]
ALLOWED_PARTS = {"legacy_compat", "archive", ".git", "__pycache__", ".pytest_cache", "artifacts"}
ALLOWED_FILES = {
    "docs/plans/2026-05-22-hermes-agent-role-migration-plan.md",
    "scripts/audit_hermes_vocabulary.py",
    "tests/test_audit_hermes_vocabulary.py",
}
SUFFIXES = {".py", ".md", ".html", ".json", ".yaml", ".yml"}


@dataclass(frozen=True)
class VocabularyViolation:
    path: Path
    term: str
    line: int


def _allowed(path: Path) -> bool:
    text = str(path).replace("\\", "/")
    if text in ALLOWED_FILES or any(text.endswith("/" + f) for f in ALLOWED_FILES):
        return True
    return any(part in ALLOWED_PARTS for part in path.parts)


def find_vocabulary_violations(roots: list[str | Path] | None = None) -> list[VocabularyViolation]:
    roots = roots or ["src", "tests", "docs", "configs", "scripts"]
    forbidden = [(term, term.lower()) for term in FORBIDDEN_TERMS]
    violations: list[VocabularyViolation] = []
    for root in [Path(r) for r in roots]:
        if not root.exists():
            continue
        paths = [root] if root.is_file() else [p for p in root.rglob("*") if p.is_file()]
        for path in paths:
            if path.suffix not in SUFFIXES or _allowed(path):
                continue
            try:
                lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            except Exception:
                continue
            for lineno, line in enumerate(lines, 1):
                lowered = line.lower()
                for original, term in forbidden:
                    if term in lowered:
                        violations.append(VocabularyViolation(path, original, lineno))
    return violations


def main() -> int:
    violations = find_vocabulary_violations()
    for v in violations:
        print(f"{v.path}:{v.line}:{v.term}")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
