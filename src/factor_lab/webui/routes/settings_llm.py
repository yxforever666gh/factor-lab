from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib.parse import parse_qs

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse

from factor_lab.webui.security import csrf_token, enforce_settings_post


RenderFunc = Callable[..., HTMLResponse]


def _redacted_profile(profile: dict[str, Any]) -> dict[str, Any]:
    secret = str(profile.get("api_key") or "").strip()
    masked = "未配置" if not secret else "***" if len(secret) <= 8 else f"{secret[:4]}...{secret[-4:]}"
    return {**profile, "api_key": "", "api_key_configured": bool(secret), "api_key_masked": masked}


def register_llm_settings_routes(
    app: Any,
    *,
    render: RenderFunc,
    load_llm_settings: Callable[[], dict[str, Any]],
    save_llm_settings: Callable[[dict[str, str]], dict[str, Any]],
    restart_research_daemon_after_settings_save: Callable[[], dict[str, Any]],
    read_env_values: Callable[[], dict[str, str]],
    profiles_from_form: Callable[[dict[str, str], list[dict[str, Any]]], tuple[list[dict[str, Any]], str]],
    test_llm_profile_connection: Callable[[dict[str, Any]], dict[str, Any]],
    api_format_options: list[dict[str, str]],
) -> None:
    """Register LLM settings routes with injected WebUI dependencies.

    The lazy dependency pattern preserves existing tests that monkeypatch public
    functions on ``webui_app`` while allowing route bodies to move out of the
    main app module.
    """

    @app.get("/settings", response_class=HTMLResponse)
    def settings_page(saved: str | None = None, restart: str | None = None):
        settings = load_llm_settings()
        profile_slots = list(settings.get("profiles") or [])
        while len(profile_slots) < 5:
            profile_slots.append({"name": "", "base_url": "", "model": "", "api_format": "openai_responses", "api_key_masked": "未配置", "enabled": True})
        return render(
            "settings.html",
            title="大模型设置",
            settings=settings,
            profile_slots=profile_slots,
            provider_options=["direct_model", "heuristic", "mock"],
            api_format_options=api_format_options,
            test_result=None,
            saved=saved == "1",
            restart_ok=restart == "1",
            restart_failed=restart == "0",
            csrf_token=csrf_token(),
        )

    @app.post("/settings")
    async def settings_save(request: Request):
        body = (await request.body()).decode("utf-8")
        parsed = parse_qs(body, keep_blank_values=True)
        form = {key: values[-1] if values else "" for key, values in parsed.items()}
        enforce_settings_post(request, form)
        save_llm_settings(form)
        restart_result = restart_research_daemon_after_settings_save()
        restart_flag = "1" if restart_result.get("ok") else "0"
        return RedirectResponse(url=f"/settings?saved=1&restart={restart_flag}", status_code=303)

    @app.post("/settings/test-model", response_class=HTMLResponse)
    async def settings_test_model(request: Request):
        body = (await request.body()).decode("utf-8")
        parsed = parse_qs(body, keep_blank_values=True)
        form = {key: values[-1] if values else "" for key, values in parsed.items()}
        enforce_settings_post(request, form)
        existing_values = read_env_values()
        raw_existing_profiles = existing_values.get("FACTOR_LAB_LLM_PROFILES_JSON") or os.environ.get("FACTOR_LAB_LLM_PROFILES_JSON") or ""
        try:
            existing_profiles = json.loads(raw_existing_profiles) if raw_existing_profiles else []
        except Exception:
            existing_profiles = []
        if not isinstance(existing_profiles, list):
            existing_profiles = []
        try:
            profile_index = int(form.get("profile_test_index") or 0)
        except ValueError:
            profile_index = 0
        profiles, _ = profiles_from_form(form, existing_profiles)

        # Build the tested connection from the actual form row.  In particular,
        # never let the normal "blank means preserve" save behavior attach a
        # stored secret to a newly edited endpoint.  A stored key may be reused
        # only when both the profile identity and Base URL are unchanged.
        submitted_name = str(form.get(f"profile_name_{profile_index}") or "").strip()
        submitted_base_url = str(form.get(f"profile_base_url_{profile_index}") or "").strip()
        submitted_key = str(form.get(f"profile_api_key_{profile_index}") or "").strip()
        submitted_ref = ""
        existing = next(
            (
                item
                for item in existing_profiles
                if str(item.get("name") or "").strip() == submitted_name
            ),
            {},
        )
        if (
            not submitted_key
            and existing
            and str(existing.get("base_url") or "").strip() == submitted_base_url
        ):
            submitted_key = str(existing.get("api_key") or "").strip()
            submitted_ref = str(existing.get("credential_ref") or "").strip()
        profile = {
            "name": submitted_name,
            "base_url": submitted_base_url,
            "model": str(form.get(f"profile_model_{profile_index}") or "").strip(),
            "api_key": submitted_key,
            "credential_ref": submitted_ref,
            "api_format": str(
                form.get(f"profile_api_format_{profile_index}") or "openai_responses"
            ).strip(),
            "enabled": form.get(f"profile_enabled_{profile_index}")
            in {"on", "1", "true", "yes"},
        }
        test_result = test_llm_profile_connection(profile)
        settings = load_llm_settings()
        profile_slots = [_redacted_profile(profile) for profile in profiles]
        while len(profile_slots) < 5:
            profile_slots.append({"name": "", "base_url": "", "model": "", "api_format": "openai_responses", "api_key_masked": "未配置", "enabled": True})
        return render(
            "settings.html",
            title="大模型设置",
            settings=settings,
            profile_slots=profile_slots,
            provider_options=["direct_model", "heuristic", "mock"],
            api_format_options=api_format_options,
            test_result=test_result,
            saved=False,
            restart_ok=False,
            restart_failed=False,
            csrf_token=csrf_token(),
        )
