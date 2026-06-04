from __future__ import annotations

from typing import Any, Callable
from pathlib import Path
import json
import urllib.error
import urllib.request

LLM_ENV_KEYS = [
    "FACTOR_LAB_DECISION_PROVIDER",
    "FACTOR_LAB_LIVE_DECISION_PROVIDER",
    "FACTOR_LAB_OBSERVATION_DECISION_PROVIDER",
    "FACTOR_LAB_LLM_BASE_URL",
    "FACTOR_LAB_LLM_MODEL",
    "FACTOR_LAB_LLM_API_KEY",
    "FACTOR_LAB_LLM_API_FORMAT",
]

LLM_PROFILE_ENV_KEYS = [
    "FACTOR_LAB_LLM_PROFILES_JSON",
    "FACTOR_LAB_LLM_FALLBACK_ORDER",
]

HERMES_PROFILE_SETTING_ENV_KEYS = [
    "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
    "FACTOR_LAB_HERMES_PROFILE_ORDER",
]

LLM_FORM_TO_ENV = {
    "decision_provider": "FACTOR_LAB_DECISION_PROVIDER",
    "live_decision_provider": "FACTOR_LAB_LIVE_DECISION_PROVIDER",
    "observation_decision_provider": "FACTOR_LAB_OBSERVATION_DECISION_PROVIDER",
}

DATA_SOURCE_ENV_KEYS = [
    "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON",
    "FACTOR_LAB_DATA_SOURCE_ORDER",
    "FACTOR_LAB_PRIMARY_DATA_SOURCE",
    "TUSHARE_TOKEN",
    "DIEMENG_API_KEY",
]

DATA_SOURCE_TYPE_OPTIONS = [
    {"value": "tushare", "label": "Tushare"},
    {"value": "diemeng", "label": "Diemeng / 迪蒙"},
    {"value": "custom", "label": "Custom"},
]

LEGACY_DATA_SOURCE_KEY_ENV = {
    "tushare": "TUSHARE_TOKEN",
    "diemeng": "DIEMENG_API_KEY",
}

LLM_API_FORMAT_OPTIONS = [
    {"value": "openai_responses", "label": "OpenAI Responses"},
    {"value": "openai", "label": "OpenAI Chat Completions"},
    {"value": "anthropic", "label": "Anthropic Messages"},
]


def read_env_values(env_file_func: Callable[[], str | Path], path: str | Path | None = None) -> dict[str, str]:
    env_path = Path(path) if path is not None else Path(env_file_func())
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def mask_secret(value: str | None) -> str:
    secret = (value or "").strip()
    if not secret:
        return "未配置"
    if len(secret) <= 8:
        return "***"
    return f"{secret[:4]}...{secret[-4:]}"


def split_csv(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def coerce_boolish(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on", "enabled"}:
            return True
        if lowered in {"0", "false", "no", "off", "disabled"}:
            return False
    return bool(value)


def normalize_llm_api_format(value: Any, model: str | None = None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"responses", "openai_response"}:
        raw = "openai_responses"
    if raw in {"chat", "chat_completions", "chat_completion", "openai_chat", "openai_chat_completions"}:
        raw = "openai"
    if raw in {"messages", "anthropic_messages", "claude"}:
        raw = "anthropic"
    if raw in {"openai", "openai_responses", "anthropic"}:
        return raw
    model_text = str(model or "").strip().lower()
    if model_text.startswith("claude") or "opus" in model_text:
        return "anthropic"
    if model_text.startswith("gpt-5"):
        return "openai_responses"
    return "openai"


def normalize_data_source_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"tushare", "diemeng", "custom"}:
        return raw
    return "custom"


def ordered_data_source_profiles(profiles: list[dict[str, Any]], order: str) -> list[dict[str, Any]]:
    names = split_csv(order)
    if not names:
        return profiles
    by_name = {str(profile.get("name") or ""): profile for profile in profiles}
    ordered = [by_name[name] for name in names if name in by_name]
    ordered_names = {str(profile.get("name") or "") for profile in ordered}
    ordered.extend(profile for profile in profiles if str(profile.get("name") or "") not in ordered_names)
    return ordered


def redacted_data_source_profile(profile: dict[str, Any]) -> dict[str, Any]:
    api_key = str(profile.get("api_key") or "")
    return {
        "name": str(profile.get("name") or ""),
        "source_type": normalize_data_source_type(profile.get("source_type")),
        "api_key": "",
        "api_key_configured": bool(api_key),
        "api_key_masked": mask_secret(api_key),
        "enabled": coerce_boolish(profile.get("enabled", True), default=True),
        "notes": str(profile.get("notes") or ""),
        "extra": profile.get("extra") if isinstance(profile.get("extra"), dict) else {},
    }


def load_data_source_profiles(values: dict[str, str], environ: dict[str, str]) -> tuple[list[dict[str, Any]], str]:
    raw_profiles = values.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or environ.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or ""
    order = values.get("FACTOR_LAB_DATA_SOURCE_ORDER") or environ.get("FACTOR_LAB_DATA_SOURCE_ORDER") or ""
    profiles: list[dict[str, Any]] = []
    if raw_profiles:
        try:
            parsed = json.loads(raw_profiles)
        except Exception:
            parsed = []
        if isinstance(parsed, list):
            for index, item in enumerate(parsed):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or f"source-{index + 1}").strip()
                source_type = normalize_data_source_type(item.get("source_type"))
                api_key = str(item.get("api_key") or "")
                notes = str(item.get("notes") or "")
                enabled = coerce_boolish(item.get("enabled", True), default=True)
                if not any([name, api_key, notes]) and source_type == "custom":
                    continue
                profiles.append({
                    "name": name,
                    "source_type": source_type,
                    "api_key": api_key,
                    "enabled": enabled,
                    "notes": notes,
                    "extra": item.get("extra") if isinstance(item.get("extra"), dict) else {},
                })
    if not profiles:
        legacy_tushare = values.get("TUSHARE_TOKEN") or environ.get("TUSHARE_TOKEN") or ""
        legacy_diemeng = values.get("DIEMENG_API_KEY") or environ.get("DIEMENG_API_KEY") or ""
        if legacy_tushare:
            profiles.append({"name": "primary-tushare", "source_type": "tushare", "api_key": legacy_tushare, "enabled": True, "notes": "", "extra": {}})
        if legacy_diemeng:
            profiles.append({"name": "primary-diemeng", "source_type": "diemeng", "api_key": legacy_diemeng, "enabled": True, "notes": "", "extra": {}})
    if order:
        profiles = ordered_data_source_profiles(profiles, order)
    elif profiles:
        order = ",".join(str(profile.get("name")) for profile in profiles if profile.get("name") and coerce_boolish(profile.get("enabled", True), default=True))
    return profiles, order


def data_source_profiles_from_form(form: dict[str, str], existing_profiles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    existing_keys: dict[tuple[str, str], str] = {}
    for profile in existing_profiles:
        source_type = normalize_data_source_type(profile.get("source_type"))
        name = str(profile.get("name") or "")
        existing_keys[(source_type, name)] = str(profile.get("api_key") or "")
    profiles: list[dict[str, Any]] = []
    for index in range(20):
        source_type = normalize_data_source_type(form.get(f"source_type_{index}"))
        name = (form.get(f"source_name_{index}") or "").strip()
        api_key = (form.get(f"source_api_key_{index}") or "").strip()
        notes = (form.get(f"source_notes_{index}") or "").strip()
        enabled = form.get(f"source_enabled_{index}") in {"on", "1", "true", "yes"}
        if not any([name, api_key, notes]):
            continue
        if not name:
            name = f"{source_type}-{index + 1}"
        if not api_key:
            api_key = existing_keys.get((source_type, name), "")
        profiles.append({
            "name": name,
            "source_type": source_type,
            "api_key": api_key,
            "enabled": enabled,
            "notes": notes,
            "extra": {},
            "order": (form.get(f"source_order_{index}") or "").strip(),
            "_index": index,
        })
    explicit_order = any(str(profile.get("order") or "").strip() for profile in profiles)
    if explicit_order:
        def order_key(profile: dict[str, Any]) -> tuple[int, int]:
            try:
                return (int(str(profile.get("order") or "9999")), int(profile.get("_index") or 0))
            except ValueError:
                return (9999, int(profile.get("_index") or 0))
        profiles = sorted(profiles, key=order_key)
    else:
        order = (form.get("data_source_order") or "").strip()
        profiles = ordered_data_source_profiles(profiles, order)
    for profile in profiles:
        profile.pop("order", None)
        profile.pop("_index", None)
    enabled_names = [str(profile.get("name") or "") for profile in profiles if str(profile.get("name") or "") and coerce_boolish(profile.get("enabled", True), default=True)]
    order = ",".join(enabled_names)
    return profiles, order


def first_enabled_source_profile(profiles: list[dict[str, Any]], source_type: str | None = None) -> dict[str, Any]:
    normalized = normalize_data_source_type(source_type) if source_type else None
    for profile in profiles:
        if normalized and normalize_data_source_type(profile.get("source_type")) != normalized:
            continue
        if coerce_boolish(profile.get("enabled", True), default=True):
            return profile
    return {}


def load_data_source_settings(env_file_func: Callable[[], str | Path], environ: dict[str, str]) -> dict[str, Any]:
    values = read_env_values(env_file_func)
    profiles, order = load_data_source_profiles(values, environ)
    primary = first_enabled_source_profile(profiles)
    return {
        "profiles": [redacted_data_source_profile(profile) for profile in profiles],
        "order": order,
        "primary_data_source": str(primary.get("source_type") or values.get("FACTOR_LAB_PRIMARY_DATA_SOURCE") or environ.get("FACTOR_LAB_PRIMARY_DATA_SOURCE") or ""),
        "env_file": str(env_file_func()),
    }


def save_data_source_settings(form: dict[str, str], env_file_func: Callable[[], str | Path], environ: dict[str, str]) -> dict[str, Any]:
    path = Path(env_file_func())
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_values = read_env_values(env_file_func, path)
    raw_existing_profiles = existing_values.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or environ.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or ""
    try:
        existing_profiles = json.loads(raw_existing_profiles) if raw_existing_profiles else []
    except Exception:
        existing_profiles = []
    if not isinstance(existing_profiles, list):
        existing_profiles = []
    if not existing_profiles:
        legacy_tushare = existing_values.get("TUSHARE_TOKEN") or environ.get("TUSHARE_TOKEN") or ""
        legacy_diemeng = existing_values.get("DIEMENG_API_KEY") or environ.get("DIEMENG_API_KEY") or ""
        if legacy_tushare:
            existing_profiles.append({"name": "primary-tushare", "source_type": "tushare", "api_key": legacy_tushare})
        if legacy_diemeng:
            existing_profiles.append({"name": "primary-diemeng", "source_type": "diemeng", "api_key": legacy_diemeng})
    profiles, order = data_source_profiles_from_form(form, existing_profiles)
    primary = first_enabled_source_profile(profiles)
    requested: dict[str, str] = {
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(profiles, ensure_ascii=False, separators=(",", ":")),
        "FACTOR_LAB_DATA_SOURCE_ORDER": order,
        "FACTOR_LAB_PRIMARY_DATA_SOURCE": str(primary.get("source_type") or ""),
    }
    for source_type, env_key in LEGACY_DATA_SOURCE_KEY_ENV.items():
        profile = first_enabled_source_profile(profiles, source_type)
        current_value = existing_values.get(env_key) or environ.get(env_key) or ""
        requested[env_key] = str(profile.get("api_key") or current_value or "")
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    updated_lines: list[str] = []
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
    for key in DATA_SOURCE_ENV_KEYS:
        if key not in seen:
            updated_lines.append(f"{key}={requested.get(key, '')}")
    path.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    for key, value in requested.items():
        environ[key] = value
    return load_data_source_settings(env_file_func, environ)


def test_data_source_connection(profile: dict[str, Any]) -> dict[str, Any]:
    source_type = normalize_data_source_type(profile.get("source_type"))
    name = str(profile.get("name") or source_type or "data-source")
    api_key = str(profile.get("api_key") or "").strip()
    if not api_key:
        return {"ok": False, "message": "数据源测试失败：Token/API Key 未填写。", "source_type": source_type, "name": name}
    if source_type == "tushare":
        try:
            import tushare as ts
            pro = ts.pro_api(api_key)
            pro.query("trade_cal", exchange="SSE", start_date="20240101", end_date="20240105", fields="cal_date,is_open")
            return {"ok": True, "message": "数据源测试成功", "source_type": source_type, "name": name}
        except Exception as exc:
            return {"ok": False, "message": f"数据源测试失败：{type(exc).__name__}: {exc}", "source_type": source_type, "name": name}
    return {"ok": True, "message": "数据源配置格式有效；该类型暂无实时探测。", "source_type": source_type, "name": name}


def ordered_profile_list(profiles: list[dict[str, Any]], fallback_order: str) -> list[dict[str, Any]]:
    order = split_csv(fallback_order)
    if not order:
        return profiles
    by_name = {str(profile.get("name") or ""): profile for profile in profiles}
    ordered = [by_name[name] for name in order if name in by_name]
    ordered_names = {str(profile.get("name") or "") for profile in ordered}
    ordered.extend(profile for profile in profiles if str(profile.get("name") or "") not in ordered_names)
    return ordered


def enabled_profile_names(profiles: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    for profile in profiles:
        name = str(profile.get("name") or "").strip()
        if name and coerce_boolish(profile.get("enabled", True), default=True):
            names.append(name)
    return names


def first_enabled_profile(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    for profile in profiles:
        if coerce_boolish(profile.get("enabled", True), default=True):
            return profile
    return profiles[0] if profiles else {}


def load_llm_profiles(values: dict[str, str], environ: dict[str, str]) -> tuple[list[dict[str, Any]], str]:
    has_legacy_file_profile = bool(values.get("FACTOR_LAB_LLM_BASE_URL") or values.get("FACTOR_LAB_LLM_API_KEY") or values.get("FACTOR_LAB_LLM_MODEL"))
    raw_profiles = values.get("FACTOR_LAB_LLM_PROFILES_JSON") or ("" if has_legacy_file_profile else environ.get("FACTOR_LAB_LLM_PROFILES_JSON")) or ""
    fallback_order = values.get("FACTOR_LAB_LLM_FALLBACK_ORDER") or ("" if has_legacy_file_profile else environ.get("FACTOR_LAB_LLM_FALLBACK_ORDER")) or ""
    profiles: list[dict[str, Any]] = []
    if raw_profiles:
        try:
            parsed = json.loads(raw_profiles)
        except Exception:
            parsed = []
        if isinstance(parsed, list):
            for index, item in enumerate(parsed):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or f"profile-{index + 1}").strip()
                if not name:
                    continue
                api_key = str(item.get("api_key") or "")
                profiles.append({
                    "name": name,
                    "base_url": str(item.get("base_url") or ""),
                    "model": str(item.get("model") or ""),
                    "api_format": normalize_llm_api_format(item.get("api_format"), item.get("model")),
                    "api_key": "",
                    "api_key_configured": bool(api_key),
                    "api_key_masked": mask_secret(api_key),
                    "enabled": coerce_boolish(item.get("enabled", True), default=True),
                })
    if not profiles:
        api_key = values.get("FACTOR_LAB_LLM_API_KEY") or environ.get("FACTOR_LAB_LLM_API_KEY") or ""
        profiles.append({
            "name": values.get("FACTOR_LAB_LLM_PROFILE_NAME") or environ.get("FACTOR_LAB_LLM_PROFILE_NAME") or "default",
            "base_url": values.get("FACTOR_LAB_LLM_BASE_URL") or environ.get("FACTOR_LAB_LLM_BASE_URL") or "",
            "model": values.get("FACTOR_LAB_LLM_MODEL") or environ.get("FACTOR_LAB_LLM_MODEL") or "",
            "api_format": normalize_llm_api_format(values.get("FACTOR_LAB_LLM_API_FORMAT") or environ.get("FACTOR_LAB_LLM_API_FORMAT"), values.get("FACTOR_LAB_LLM_MODEL") or environ.get("FACTOR_LAB_LLM_MODEL")),
            "api_key": "",
            "api_key_configured": bool(api_key),
            "api_key_masked": mask_secret(api_key),
            "enabled": True,
        })
    return ordered_profile_list(profiles, fallback_order), fallback_order or ",".join(str(profile.get("name")) for profile in profiles if profile.get("name"))


def profiles_from_form(form: dict[str, str], existing_profiles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    existing_keys = {str(profile.get("name") or ""): str(profile.get("api_key") or "") for profile in existing_profiles}
    profiles: list[dict[str, Any]] = []
    for index in range(10):
        name = (form.get(f"profile_name_{index}") or "").strip()
        base_url = (form.get(f"profile_base_url_{index}") or "").strip().rstrip("/")
        model = (form.get(f"profile_model_{index}") or "").strip()
        api_format = normalize_llm_api_format(form.get(f"profile_api_format_{index}"), model)
        api_key = (form.get(f"profile_api_key_{index}") or "").strip()
        enabled = form.get(f"profile_enabled_{index}") in {"on", "1", "true", "yes"}
        if not any([name, base_url, model, api_key]):
            continue
        if not name:
            name = f"profile-{index + 1}"
        if not api_key:
            api_key = existing_keys.get(name, "")
        profiles.append({"name": name, "base_url": base_url, "model": model, "api_format": api_format, "api_key": api_key, "enabled": enabled, "order": (form.get(f"profile_order_{index}") or "").strip(), "_index": index})
    explicit_order = any(str(profile.get("order") or "").strip() for profile in profiles)
    if explicit_order:
        def order_key(profile: dict[str, Any]) -> tuple[int, int]:
            try:
                return (int(str(profile.get("order") or "9999")), int(profile.get("_index") or 0))
            except ValueError:
                return (9999, int(profile.get("_index") or 0))
        profiles = sorted(profiles, key=order_key)
        for profile in profiles:
            profile.pop("order", None)
            profile.pop("_index", None)
        fallback_order = ",".join(profile["name"] for profile in profiles if coerce_boolish(profile.get("enabled", True), default=True))
    else:
        fallback_order = (form.get("fallback_order") or ",".join(profile["name"] for profile in profiles if coerce_boolish(profile.get("enabled", True), default=True))).strip()
        profiles = ordered_profile_list(profiles, fallback_order)
    enabled_names = enabled_profile_names(profiles)
    enabled_set = set(enabled_names)
    fallback_order = ",".join(name for name in split_csv(fallback_order) if name in enabled_set) or ",".join(enabled_names)
    return profiles, fallback_order


def load_llm_settings(env_file_func: Callable[[], str | Path], environ: dict[str, str]) -> dict[str, Any]:
    values = read_env_values(env_file_func)
    merged = {key: values.get(key) or environ.get(key) or "" for key in [*LLM_ENV_KEYS, *LLM_PROFILE_ENV_KEYS]}
    profiles, fallback_order = load_llm_profiles(values, environ)
    first_profile = first_enabled_profile(profiles) if profiles else {}
    api_key_configured = any(bool(profile.get("api_key_configured")) for profile in profiles)
    return {
        "decision_provider": merged.get("FACTOR_LAB_DECISION_PROVIDER") or "direct_model",
        "live_decision_provider": merged.get("FACTOR_LAB_LIVE_DECISION_PROVIDER") or merged.get("FACTOR_LAB_DECISION_PROVIDER") or "direct_model",
        "observation_decision_provider": merged.get("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER") or merged.get("FACTOR_LAB_DECISION_PROVIDER") or "direct_model",
        "base_url": first_profile.get("base_url") or merged.get("FACTOR_LAB_LLM_BASE_URL", ""),
        "model": first_profile.get("model") or merged.get("FACTOR_LAB_LLM_MODEL", ""),
        "api_key": "",
        "api_key_configured": api_key_configured,
        "api_key_masked": first_profile.get("api_key_masked") or mask_secret(merged.get("FACTOR_LAB_LLM_API_KEY")),
        "profiles": profiles,
        "fallback_order": fallback_order,
        "env_file": str(env_file_func()),
    }


def save_llm_settings(
    form: dict[str, str],
    env_file_func: Callable[[], str | Path],
    environ: dict[str, str],
    sync_hermes_profiles_func: Callable[[dict[str, str], list[dict[str, Any]], str, str], dict[str, str]] | None = None,
) -> dict[str, Any]:
    path = Path(env_file_func())
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_values = read_env_values(env_file_func, path)
    old_fallback_order = existing_values.get("FACTOR_LAB_LLM_FALLBACK_ORDER") or environ.get("FACTOR_LAB_LLM_FALLBACK_ORDER") or ""
    raw_existing_profiles = environ.get("FACTOR_LAB_LLM_PROFILES_JSON") or existing_values.get("FACTOR_LAB_LLM_PROFILES_JSON", "")
    try:
        existing_profiles = json.loads(raw_existing_profiles) if raw_existing_profiles else []
    except Exception:
        existing_profiles = []
    if not isinstance(existing_profiles, list):
        existing_profiles = []
    if not existing_profiles:
        existing_profiles = [{
            "name": "default",
            "api_key": environ.get("FACTOR_LAB_LLM_API_KEY") or existing_values.get("FACTOR_LAB_LLM_API_KEY", ""),
        }]
    if any(key.startswith("profile_") for key in form):
        profiles, fallback_order = profiles_from_form(form, existing_profiles)
    else:
        current_api_key = environ.get("FACTOR_LAB_LLM_API_KEY") or existing_values.get("FACTOR_LAB_LLM_API_KEY", "")
        profiles = [{
            "name": "default",
            "base_url": (form.get("base_url") or "").strip(),
            "model": (form.get("model") or "").strip(),
            "api_format": normalize_llm_api_format(form.get("api_format"), form.get("model")),
            "api_key": (form.get("api_key") or "").strip() or current_api_key,
            "enabled": True,
        }]
        fallback_order = "default"
    primary = first_enabled_profile(profiles) if profiles else {"base_url": "", "model": "", "api_key": ""}
    requested: dict[str, str] = {}
    for form_key, env_key in LLM_FORM_TO_ENV.items():
        requested[env_key] = (form.get(form_key) or "").strip()
    requested.update({
        "FACTOR_LAB_LLM_BASE_URL": str(primary.get("base_url") or ""),
        "FACTOR_LAB_LLM_MODEL": str(primary.get("model") or ""),
        "FACTOR_LAB_LLM_API_KEY": str(primary.get("api_key") or ""),
        "FACTOR_LAB_LLM_API_FORMAT": str(primary.get("api_format") or normalize_llm_api_format(None, primary.get("model"))),
        "FACTOR_LAB_LLM_PROFILES_JSON": json.dumps(profiles, ensure_ascii=False, separators=(",", ":")),
        "FACTOR_LAB_LLM_FALLBACK_ORDER": fallback_order,
    })
    synced_hermes_profile_values = sync_hermes_profiles_func(existing_values, profiles, old_fallback_order, fallback_order) if sync_hermes_profiles_func else {}
    requested.update(synced_hermes_profile_values)

    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    updated_lines: list[str] = []
    managed_keys = [*LLM_ENV_KEYS, *LLM_PROFILE_ENV_KEYS, *(HERMES_PROFILE_SETTING_ENV_KEYS if synced_hermes_profile_values else [])]
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
    for key in managed_keys:
        if key not in seen:
            updated_lines.append(f"{key}={requested.get(key, '')}")
    path.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    for key, value in requested.items():
        environ[key] = value
    return load_llm_settings(env_file_func, environ)


def test_llm_profile_connection(profile: dict[str, Any]) -> dict[str, Any]:
    from factor_lab.hermes_decision_router import HermesDecisionRouter

    base_url = str(profile.get("base_url") or "").strip().rstrip("/")
    model = str(profile.get("model") or "").strip()
    api_key = str(profile.get("api_key") or "").strip()
    api_format = normalize_llm_api_format(profile.get("api_format"), model)
    if not base_url or not model or not api_key:
        return {"ok": False, "message": "模型测试失败：Base URL、Model、API Key 必须填写。", "api_format": api_format, "model": model}

    router = HermesDecisionRouter(provider="direct_model", model=model)
    url = router._direct_model_endpoint_url(base_url, api_format)
    if api_format == "anthropic":
        body = {
            "model": model,
            "system": "You are a connection test endpoint.",
            "messages": [{"role": "user", "content": "Reply with OK only."}],
            "max_tokens": 16,
            "temperature": 0,
        }
        headers = router._direct_model_headers(api_key, auth_scheme="anthropic")
    elif api_format == "openai_responses":
        body = {
            "model": model,
            "input": [
                {"role": "system", "content": "You are a connection test endpoint."},
                {"role": "user", "content": "Reply with OK only."},
            ],
            "temperature": 0,
        }
        headers = router._direct_model_headers(api_key)
    else:
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a connection test endpoint."},
                {"role": "user", "content": "Reply with OK only."},
            ],
            "temperature": 0,
        }
        headers = router._direct_model_headers(api_key)
    req = urllib.request.Request(url=url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            raw_text = response.read().decode("utf-8", errors="ignore")
        return {"ok": True, "message": "模型测试成功", "api_format": api_format, "model": model, "endpoint": url, "response_preview": raw_text[:300]}
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="ignore")[:500]
        return {"ok": False, "message": f"模型测试失败：http_error:{exc.code}", "api_format": api_format, "model": model, "endpoint": url, "error": body_text}
    except Exception as exc:
        return {"ok": False, "message": f"模型测试失败：{type(exc).__name__}: {exc}", "api_format": api_format, "model": model, "endpoint": url}
