import importlib.util
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "verify_de_openclaw_runtime.py"
    module_name = "verify_de_openclaw_runtime_test"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_provider_from_env_file_prefers_decision_provider(tmp_path):
    verifier = _load_module()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FACTOR_LAB_DECISION_PROVIDER=real_llm\n"
        "FACTOR_LAB_LLM_API_KEY=secret-should-not-be-read\n"
    )

    assert verifier.provider_from_env_file(env_file) == "real_llm"


def test_scan_openclaw_references_splits_blocked_and_concept_refs(tmp_path, monkeypatch):
    verifier = _load_module()
    (tmp_path / "src").mkdir()
    (tmp_path / "docs").mkdir()
    (tmp_path / "src" / "blocked.py").write_text(
        "OLD = '/home/admin/.openclaw/workspace/scripts/run_research_daemon.py'\n"
    )
    (tmp_path / "docs" / "concept.md").write_text(
        "OpenClaw agent-role architecture is retained as a design concept.\n"
    )

    blocked, concept_refs = verifier.scan_openclaw_references(tmp_path)

    assert ("old_workspace_path", "src/blocked.py") in blocked
    assert "docs/concept.md" in concept_refs


def test_check_provider_accepts_allowed_process_provider(monkeypatch):
    verifier = _load_module()
    monkeypatch.setattr(
        verifier,
        "process_lines",
        lambda: ["123 /usr/bin/python3 scripts/run_agent_briefs.py --provider real_llm"],
    )

    result = verifier.check_provider({"real_llm"})

    assert result.level == "PASS"
    assert result.name == "provider"


def test_check_no_old_workspace_process_fails_on_old_path(monkeypatch):
    verifier = _load_module()
    monkeypatch.setattr(
        verifier,
        "process_lines",
        lambda: ["123 /usr/bin/python3 /home/admin/.openclaw/workspace/scripts/run_research_daemon.py"],
    )

    result = verifier.check_no_old_workspace_process()

    assert result.level == "FAIL"
    assert result.name == "no_old_workspace_process"


def test_check_service_path_uses_systemctl_show(monkeypatch):
    verifier = _load_module()
    monkeypatch.setattr(
        verifier,
        "systemctl_show",
        lambda service: {
            "FragmentPath": "/home/admin/.config/systemd/user/factor-lab-web-ui.service",
            "WorkingDirectory": "/home/admin/factor-lab",
            "ExecStart": "{ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 /home/admin/factor-lab/scripts/run_web_ui.py ; }",
        },
    )

    result = verifier.check_service_path("factor-lab-web-ui.service", "webui_path")

    assert result.level == "PASS"
    assert result.name == "webui_path"
