from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from factor_lab.hermes_artifacts import write_hermes_response_artifact


_FORBIDDEN_PROFILE_MODEL_KEYS = {"model", "provider", "base_url", "api_key", "fallback_order"}


@dataclass(frozen=True)
class HermesMainModel:
    provider: str
    model: str


@dataclass(frozen=True)
class HermesRequest:
    request_id: str
    profile_key: str
    profile_name: str
    session_name: str
    toolsets: tuple[str, ...]
    skills: tuple[str, ...]
    briefing_path: Path
    response_path: Path
    model_source: str = "main"
    main_provider: str | None = None
    main_model: str | None = None
    timeout_seconds: int = 300


@dataclass(frozen=True)
class HermesResult:
    ok: bool
    request_id: str
    profile_key: str
    profile_name: str
    response_path: Path
    payload: dict[str, Any] | None
    raw_text: str
    exit_code: int | None
    error: str | None


class HermesClient:
    def __init__(self, *, config_path: str | Path | None = None):
        self.config_path = Path(config_path) if config_path is not None else _default_hermes_config_path()

    def read_main_model(self) -> HermesMainModel:
        data = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        model_cfg = data.get("model") or {}
        provider = str(model_cfg.get("provider") or "").strip()
        model = str(model_cfg.get("default") or model_cfg.get("model") or "").strip()
        if not provider or not model:
            raise RuntimeError("Hermes main model config is missing provider or model")
        return HermesMainModel(provider=provider, model=model)

    def _resolve_main_model(self, request: HermesRequest) -> HermesMainModel:
        if request.model_source != "main":
            raise RuntimeError("Factor Lab Hermes requests must use model_source=main")
        if request.main_provider and request.main_model:
            return HermesMainModel(provider=request.main_provider, model=request.main_model)
        return self.read_main_model()

    def build_command(self, request: HermesRequest, prompt: str) -> list[str]:
        main = self._resolve_main_model(request)
        return [
            "hermes",
            "--profile",
            request.profile_name,
            "--resume",
            request.session_name,
            "chat",
            "-q",
            prompt,
            "--provider",
            main.provider,
            "--model",
            main.model,
            "--toolsets",
            ",".join(request.toolsets),
            "--quiet",
        ]

    def run(self, request: HermesRequest, prompt: str) -> HermesResult:
        command = self.build_command(request, prompt)
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=request.timeout_seconds)
        except Exception as exc:
            return HermesResult(False, request.request_id, request.profile_key, request.profile_name, request.response_path, None, "", None, str(exc))
        raw = (completed.stdout or "").strip()
        if completed.returncode != 0 and "--resume" in command and "Session not found" in ((completed.stdout or "") + (completed.stderr or "")):
            retry_command = _without_resume(command)
            completed = subprocess.run(retry_command, capture_output=True, text=True, timeout=request.timeout_seconds)
            raw = (completed.stdout or "").strip()
        if completed.returncode != 0:
            return HermesResult(False, request.request_id, request.profile_key, request.profile_name, request.response_path, None, raw, completed.returncode, completed.stderr.strip() or "hermes_failed")
        try:
            payload = json.loads(raw)
        except Exception:
            start, end = raw.find("{"), raw.rfind("}")
            if start >= 0 and end > start:
                try:
                    payload = json.loads(raw[start : end + 1])
                except Exception as exc:
                    return HermesResult(False, request.request_id, request.profile_key, request.profile_name, request.response_path, None, raw, completed.returncode, f"unable_to_parse_json:{exc}")
            else:
                return HermesResult(False, request.request_id, request.profile_key, request.profile_name, request.response_path, None, raw, completed.returncode, "unable_to_parse_json")
        try:
            write_hermes_response_artifact(request.response_path, payload, request_id=request.request_id, profile_key=request.profile_key)
        except Exception as exc:
            return HermesResult(False, request.request_id, request.profile_key, request.profile_name, request.response_path, payload, raw, completed.returncode, str(exc))
        return HermesResult(True, request.request_id, request.profile_key, request.profile_name, request.response_path, payload, raw, completed.returncode, None)


def _without_resume(command: list[str]) -> list[str]:
    stripped = list(command)
    if "--resume" not in stripped:
        return stripped
    idx = stripped.index("--resume")
    del stripped[idx : idx + 2]
    return stripped


def _default_hermes_config_path() -> Path:
    home = Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")
    return home / "config.yaml"


def validate_profile_config_inherits_main(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    profiles = config.get("profiles") or {}
    if not isinstance(profiles, dict):
        return ["profiles_not_mapping"]
    for profile_name, profile_cfg in profiles.items():
        if not isinstance(profile_cfg, dict):
            continue
        for key in sorted(_FORBIDDEN_PROFILE_MODEL_KEYS.intersection(profile_cfg)):
            errors.append(f"{profile_name}:{key}_must_not_be_profile_local")
    return errors
