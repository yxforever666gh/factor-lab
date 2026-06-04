from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _load_module():
    import importlib.util

    path = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "write_module_inventory.py"
    spec = importlib.util.spec_from_file_location("write_module_inventory_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_categorize_paths_identifies_main_research_lines():
    mod = _load_module()

    assert mod.categorize_path(Path("src/factor_lab/webui_app.py")) == "webui"
    assert mod.categorize_path(Path("src/factor_lab/autonomous_strategy_lab.py")) == "autonomous_strategy"
    assert mod.categorize_path(Path("src/factor_lab/harvest_cycle_runner.py")) == "harvest"
    assert mod.categorize_path(Path("src/factor_lab/small_institutionalization_policy.py")) == "small_institutional"
    assert mod.categorize_path(Path("src/factor_lab/tushare_provider.py")) == "data_source"
    assert mod.categorize_path(Path("src/factor_lab/bucket_aware_portfolio.py")) == "portfolio"


def test_inventory_for_file_extracts_imports_and_entrypoint(tmp_path: Path):
    mod = _load_module()
    root = tmp_path
    path = root / "scripts" / "demo.py"
    path.parent.mkdir(parents=True)
    path.write_text(
        "import os\n"
        "from factor_lab.workflow import run_workflow\n"
        "\n"
        "def main():\n"
        "    return 0\n"
        "\n"
        "if __name__ == '__main__':\n"
        "    raise SystemExit(main())\n",
        encoding="utf-8",
    )

    row = mod.inventory_for_file(root, path)

    assert row["path"] == "scripts/demo.py"
    assert row["line_count"] == 8
    assert row["has_main_entrypoint"] is True
    assert "os" in row["imports"]
    assert "factor_lab.workflow" in row["imports"]


def test_build_inventory_records_inbound_imports_and_test_guess(tmp_path: Path):
    mod = _load_module()
    root = tmp_path
    src = root / "src" / "factor_lab"
    tests = root / "tests"
    src.mkdir(parents=True)
    tests.mkdir()
    (src / "alpha.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    (src / "beta.py").write_text("from factor_lab.alpha import f\n", encoding="utf-8")
    (tests / "test_alpha.py").write_text("from factor_lab.alpha import f\n", encoding="utf-8")

    payload = mod.build_inventory(root)
    by_path = {row["path"]: row for row in payload["files"]}

    assert payload["summary"]["file_count"] == 3
    assert by_path["src/factor_lab/alpha.py"]["inbound_import_count"] >= 2
    assert by_path["src/factor_lab/alpha.py"]["test_guess"] == "tests/test_alpha.py"


def test_cli_writes_json_and_markdown(tmp_path: Path):
    script = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "write_module_inventory.py"
    out = tmp_path / "inventory"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--root",
            str(Path(__file__).resolve().parents[1]),
            "--output-dir",
            str(out),
        ],
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads((out / "module_inventory.json").read_text(encoding="utf-8"))
    markdown = (out / "module_inventory.md").read_text(encoding="utf-8")
    assert payload["summary"]["file_count"] > 0
    assert "# Module Inventory" in markdown
    assert "webui_app.py" in markdown
