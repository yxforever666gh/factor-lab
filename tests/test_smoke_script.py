import importlib.util
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "smoke_test_factor_lab.py"
    module_name = "smoke_test_factor_lab_test"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_check_line_formats_pass_and_fail():
    mod = _load_module()

    assert mod.Check("x", True, "ok").line() == "PASS x: ok"
    assert mod.Check("x", False, "bad").line() == "FAIL x: bad"


def test_llm_config_redacts_api_key(tmp_path, monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "PROJECT_ROOT", tmp_path)
    (tmp_path / ".env").write_text(
        "FACTOR_LAB_LLM_MODEL=claude-opus-4-7\nFACTOR_LAB_LLM_API_KEY=super-secret\n",
        encoding="utf-8",
    )

    result = mod.check_llm_config_redacted()

    assert result.ok
    assert "super-secret" not in result.detail
    assert "***" in result.detail
