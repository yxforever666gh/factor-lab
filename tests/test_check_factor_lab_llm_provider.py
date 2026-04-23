import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from factor_lab.llm_provider_router import DecisionProviderRouter


def test_healthcheck_includes_normalized_fields():
    """Healthcheck output should include normalized provider metadata fields."""
    router = DecisionProviderRouter()
    payload = router.healthcheck()
    
    # Verify normalized fields exist
    assert "normalized_provider" in payload
    assert "configured_provider" in payload
    assert "effective_source" in payload
    assert isinstance(payload.get("normalized_provider"), str)
    assert isinstance(payload.get("configured_provider"), str)
    assert isinstance(payload.get("effective_source"), str)


def test_generic_provider_config_without_openclaw_vars(monkeypatch):
    """Generic provider config should be evaluable without any FACTOR_LAB_OPENCLAW_* variables."""
    # Clear all OpenClaw-specific env vars
    openclaw_vars = [k for k in os.environ.keys() if k.startswith("FACTOR_LAB_OPENCLAW_")]
    for var in openclaw_vars:
        monkeypatch.delenv(var, raising=False)
    
    # Set generic provider config
    monkeypatch.setenv("FACTOR_LAB_LLM_BASE_URL", "https://api.example.com/v1")
    monkeypatch.setenv("FACTOR_LAB_LLM_API_KEY", "test-key")
    monkeypatch.setenv("FACTOR_LAB_LLM_MODEL", "gpt-4o-mini")
    monkeypatch.setenv("FACTOR_LAB_DECISION_PROVIDER", "real_llm")
    
    # Should be able to instantiate router without errors
    router = DecisionProviderRouter()
    payload = router.healthcheck()
    
    # Verify it's using the generic provider
    assert payload["normalized_provider"] in ["real_llm", "heuristic"]  # may fall back to heuristic if no real connection
    assert payload["configured_provider"] == "real_llm"


def test_healthcheck_script_runs_without_error():
    """The healthcheck script should run successfully in current workspace."""
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "check_factor_lab_llm_provider.py"
    
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        timeout=10,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    )
    
    # Should exit successfully
    assert result.returncode == 0, f"Script failed with stderr: {result.stderr}"
    
    # Should output valid JSON
    try:
        payload = json.loads(result.stdout)
        assert "normalized_provider" in payload
        assert "configured_provider" in payload
    except json.JSONDecodeError as e:
        pytest.fail(f"Script output is not valid JSON: {e}\nOutput: {result.stdout}")
