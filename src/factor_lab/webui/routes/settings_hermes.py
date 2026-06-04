from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse


RenderFunc = Callable[..., HTMLResponse]


def register_hermes_settings_routes(
    app: Any,
    *,
    render: RenderFunc,
    env_file: Callable[[], Path],
    load_agent_settings: Callable[[], dict[str, Any]],
    save_agent_settings: Callable[[dict[str, str]], dict[str, Any]],
    load_llm_settings: Callable[[], dict[str, Any]],
    enabled_profile_names: Callable[[list[dict[str, Any]]], list[str]],
    hermes_profile_fallback_warnings: Callable[[list[dict[str, Any]], list[str]], list[dict[str, Any]]],
    restart_research_daemon_after_settings_save: Callable[[], dict[str, Any]],
) -> None:
    """Register Hermes settings routes with lazy WebUI dependencies."""

    @app.get("/hermes", response_class=HTMLResponse)
    def hermes_page(saved: str | None = None, restart: str | None = None):
        settings = load_agent_settings()
        llm_settings = load_llm_settings()
        available_profile_names = enabled_profile_names(list(llm_settings.get("profiles") or []))
        agent_fallback_warnings = hermes_profile_fallback_warnings(list(settings.get("roles") or []), available_profile_names)
        role_slots = list(settings.get("roles") or [])
        while len(role_slots) < 3:
            role_slots.append({
                "name": "",
                "display_name": "",
                "enabled": True,
                "decision_types": "",
                "purpose": "",
                "system_prompt": "",
                "llm_fallback_order": "",
                "timeout_seconds": 90,
                "max_retries": 1,
                "strict_schema": True,
                "legacy_agent_id": "",
            })
        return render(
            "hermes.html",
            title="Hermes 设置",
            settings=settings,
            role_slots=role_slots,
            available_profile_names=available_profile_names,
            agent_fallback_warnings=agent_fallback_warnings,
            saved=saved == "1",
            restart_ok=restart == "1",
            restart_failed=restart == "0",
        )

    @app.post("/hermes")
    async def hermes_save(request: Request):
        from scripts.enable_hermes_native import desired_env_values

        path = env_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        requested = desired_env_values()
        lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
        updated_lines: list[str] = []
        seen: set[str] = set()
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in line:
                updated_lines.append(line)
                continue
            key, _ = line.split("=", 1)
            key = key.strip()
            if key in requested:
                updated_lines.append(f"{key}={requested[key]}")
                seen.add(key)
            else:
                updated_lines.append(line)
        for key, value in requested.items():
            if key not in seen:
                updated_lines.append(f"{key}={value}")
            os.environ[key] = value
        path.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")
        restart_result = restart_research_daemon_after_settings_save()
        restart_flag = "1" if restart_result.get("ok") else "0"
        return RedirectResponse(url=f"/hermes?saved=1&restart={restart_flag}", status_code=303)

    @app.get("/agents", response_class=HTMLResponse)
    def agents_page(saved: str | None = None, restart: str | None = None):
        suffix = []
        if saved is not None:
            suffix.append(f"saved={saved}")
        if restart is not None:
            suffix.append(f"restart={restart}")
        query = ("?" + "&".join(suffix)) if suffix else ""
        return RedirectResponse(url=f"/hermes{query}", status_code=307)

    @app.post("/agents")
    async def agents_save(request: Request):
        body = (await request.body()).decode("utf-8")
        parsed = parse_qs(body, keep_blank_values=True)
        save_agent_settings({key: values[-1] if values else "" for key, values in parsed.items()})
        restart_result = restart_research_daemon_after_settings_save()
        restart_flag = "1" if restart_result.get("ok") else "0"
        return RedirectResponse(url=f"/agents?saved=1&restart={restart_flag}", status_code=303)
