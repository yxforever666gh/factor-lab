#!/usr/bin/env python3
"""Write a Factor Lab module inventory for engineering hardening."""

from __future__ import annotations

import argparse
import ast
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCAN_DIRS = [Path("src/factor_lab"), Path("scripts"), Path("tests")]
SKIP_PARTS = {".git", ".venv", "__pycache__", ".pytest_cache", "artifacts"}


def categorize_path(path: Path) -> str:
    text = str(path).replace("\\", "/")
    name = path.name
    if "webui" in text or "webui_templates" in text:
        return "webui"
    if "autonomous_strategy" in text:
        return "autonomous_strategy"
    if "harvest" in text:
        return "harvest"
    if any(token in text for token in ["small_institutional", "hermes_correction", "risk_reduction", "simulated_portfolio"]):
        return "small_institutional"
    if any(token in text for token in ["tushare", "data_source", "data_cache", "feature_schema", "coverage_preflight"]):
        return "data_source"
    if any(token in text for token in ["portfolio", "scorecard", "bucket_aware"]):
        return "portfolio"
    if "research_queue" in text or "research_strategy" in text or "governance" in text:
        return "research_core"
    if text.startswith("scripts/"):
        if name.startswith("write_"):
            return "script_report_writer"
        if name.startswith("probe_") or name.startswith("smoke_"):
            return "script_devtool"
        if name.startswith("run_"):
            return "script_runner"
        if name.startswith("audit_") or name.startswith("inspect_") or name.startswith("check_"):
            return "script_ops"
        return "script_misc"
    if text.startswith("tests/"):
        return "test"
    return "misc"


def iter_python_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for scan_dir in SCAN_DIRS:
        base = root / scan_dir
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            rel_parts = set(path.relative_to(root).parts)
            if rel_parts & SKIP_PARTS:
                continue
            files.append(path)
    return sorted(files)


def _parse_ast(text: str) -> ast.AST | None:
    try:
        return ast.parse(text)
    except SyntaxError:
        return None


def extract_imports(text: str) -> list[str]:
    tree = _parse_ast(text)
    if tree is None:
        return []
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module)
    return sorted(imports)


def has_main_entrypoint(text: str) -> bool:
    return "__main__" in text and "if __name__" in text


def module_name_for_path(rel: Path) -> str | None:
    parts = rel.parts
    if len(parts) >= 3 and parts[0] == "src" and parts[1] == "factor_lab" and rel.suffix == ".py":
        stem_parts = ["factor_lab", *parts[2:]]
        stem_parts[-1] = Path(stem_parts[-1]).stem
        if stem_parts[-1] == "__init__":
            stem_parts = stem_parts[:-1]
        return ".".join(stem_parts)
    return None


def test_guess_for(rel: Path, root: Path) -> str | None:
    if not str(rel).startswith("src/factor_lab/") or rel.name == "__init__.py":
        return None
    candidate = root / "tests" / f"test_{rel.stem}.py"
    if candidate.exists():
        return str(candidate.relative_to(root))
    return None


def inventory_for_file(root: Path, path: Path) -> dict[str, Any]:
    rel = path.relative_to(root)
    text = path.read_text(encoding="utf-8", errors="ignore")
    return {
        "path": str(rel),
        "category": categorize_path(rel),
        "line_count": len(text.splitlines()),
        "imports": extract_imports(text),
        "module_name": module_name_for_path(rel),
        "has_main_entrypoint": has_main_entrypoint(text),
        "test_guess": test_guess_for(rel, root),
        "inbound_import_count": 0,
        "inbound_importers": [],
    }


def build_inventory(root: str | Path) -> dict[str, Any]:
    root = Path(root)
    rows = [inventory_for_file(root, path) for path in iter_python_files(root)]
    module_to_path = {row["module_name"]: row["path"] for row in rows if row.get("module_name")}
    inbound: dict[str, list[str]] = {row["path"]: [] for row in rows}
    for row in rows:
        importer = row["path"]
        for imported in row["imports"]:
            for module_name, module_path in module_to_path.items():
                if imported == module_name or imported.startswith(module_name + "."):
                    if importer != module_path:
                        inbound[module_path].append(importer)
    for row in rows:
        importers = sorted(set(inbound.get(row["path"], [])))
        row["inbound_importers"] = importers[:20]
        row["inbound_import_count"] = len(importers)
    categories = Counter(row["category"] for row in rows)
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "file_count": len(rows),
        "total_lines": sum(int(row["line_count"]) for row in rows),
        "categories": dict(sorted(categories.items())),
        "largest_files": sorted(
            [{"path": row["path"], "line_count": row["line_count"], "category": row["category"]} for row in rows],
            key=lambda item: int(item["line_count"]),
            reverse=True,
        )[:25],
    }
    return {"schema_version": 1, "summary": summary, "files": rows}


def to_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Module Inventory",
        "",
        f"generated_at_utc: {summary['generated_at_utc']}",
        f"file_count: {summary['file_count']}",
        f"total_lines: {summary['total_lines']}",
        "",
        "## Categories",
        "",
        "| Category | Files |",
        "|---|---:|",
    ]
    for category, count in summary["categories"].items():
        lines.append(f"| {category} | {count} |")
    lines.extend([
        "",
        "## Largest files",
        "",
        "| Lines | Category | Path |",
        "|---:|---|---|",
    ])
    for row in summary["largest_files"]:
        lines.append(f"| {row['line_count']} | {row['category']} | `{row['path']}` |")
    lines.extend([
        "",
        "## High inbound modules",
        "",
        "| Inbound | Lines | Category | Path | Test guess |",
        "|---:|---:|---|---|---|",
    ])
    high_inbound = sorted(payload["files"], key=lambda row: (int(row["inbound_import_count"]), int(row["line_count"])), reverse=True)[:25]
    for row in high_inbound:
        lines.append(f"| {row['inbound_import_count']} | {row['line_count']} | {row['category']} | `{row['path']}` | {row.get('test_guess') or '-'} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--output-dir", default="artifacts/engineering_hardening_2026-06-02")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = root / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = build_inventory(root)
    (output_dir / "module_inventory.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "module_inventory.md").write_text(to_markdown(payload), encoding="utf-8")
    print(f"wrote {output_dir / 'module_inventory.json'}")
    print(f"wrote {output_dir / 'module_inventory.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
