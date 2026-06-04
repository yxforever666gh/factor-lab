from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib.parse import parse_qs

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse


RenderFunc = Callable[..., HTMLResponse]


def register_data_source_routes(
    app: Any,
    *,
    render: RenderFunc,
    load_data_source_settings: Callable[[], dict[str, Any]],
    save_data_source_settings: Callable[[dict[str, str]], dict[str, Any]],
    restart_research_daemon_after_settings_save: Callable[[], dict[str, Any]],
    read_env_values: Callable[[], dict[str, str]],
    data_source_profiles_from_form: Callable[[dict[str, str], list[dict[str, Any]]], tuple[list[dict[str, Any]], str]],
    redacted_data_source_profile: Callable[[dict[str, Any]], dict[str, Any]],
    test_data_source_connection: Callable[[dict[str, Any]], dict[str, Any]],
    source_type_options: list[dict[str, str]],
) -> None:
    """Register data-source settings routes.

    Dependencies are injected from ``webui_app`` so this route module can be
    imported without reaching into WebUI globals.  This keeps existing
    ``webui_app.env_file`` monkeypatch tests working while moving route bodies
    out of the giant app module.
    """

    @app.get("/data-sources", response_class=HTMLResponse)
    def data_sources_page(saved: str | None = None, restart: str | None = None):
        settings = load_data_source_settings()
        profile_slots = list(settings.get("profiles") or [])
        while len(profile_slots) < 5:
            profile_slots.append({"name": "", "source_type": "tushare", "api_key_masked": "未配置", "enabled": True, "notes": ""})
        return render(
            "data_sources.html",
            title="数据源设置",
            settings=settings,
            profile_slots=profile_slots,
            source_type_options=source_type_options,
            test_result=None,
            saved=saved == "1",
            restart_ok=restart == "1",
            restart_failed=restart == "0",
        )

    @app.post("/data-sources")
    async def data_sources_save(request: Request):
        body = (await request.body()).decode("utf-8")
        parsed = parse_qs(body, keep_blank_values=True)
        save_data_source_settings({key: values[-1] if values else "" for key, values in parsed.items()})
        restart_result = restart_research_daemon_after_settings_save()
        restart_flag = "1" if restart_result.get("ok") else "0"
        return RedirectResponse(url=f"/data-sources?saved=1&restart={restart_flag}", status_code=303)

    @app.post("/data-sources/test", response_class=HTMLResponse)
    async def data_sources_test(request: Request):
        body = (await request.body()).decode("utf-8")
        parsed = parse_qs(body, keep_blank_values=True)
        form = {key: values[-1] if values else "" for key, values in parsed.items()}
        existing_values = read_env_values()
        raw_existing_profiles = existing_values.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or os.environ.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or ""
        try:
            existing_profiles = json.loads(raw_existing_profiles) if raw_existing_profiles else []
        except Exception:
            existing_profiles = []
        if not isinstance(existing_profiles, list):
            existing_profiles = []
        profiles, _ = data_source_profiles_from_form(form, existing_profiles)
        try:
            source_index = int(form.get("source_test_index") or 0)
        except ValueError:
            source_index = 0
        profile = profiles[source_index] if 0 <= source_index < len(profiles) else {}
        test_result = test_data_source_connection(profile)
        settings = load_data_source_settings()
        profile_slots = list(profiles)
        while len(profile_slots) < 5:
            profile_slots.append({"name": "", "source_type": "tushare", "api_key_masked": "未配置", "enabled": True, "notes": ""})
        return render(
            "data_sources.html",
            title="数据源设置",
            settings=settings,
            profile_slots=[redacted_data_source_profile(profile) if profile.get("api_key") else {**profile, "api_key_masked": "未配置", "api_key_configured": False} for profile in profile_slots],
            source_type_options=source_type_options,
            test_result=test_result,
            saved=False,
            restart_ok=False,
            restart_failed=False,
        )
