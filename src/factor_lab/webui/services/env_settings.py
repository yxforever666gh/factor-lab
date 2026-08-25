from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import json
import os
import re
import secrets
import stat
import urllib.error
import urllib.request

from factor_lab.research_os.data_sources import (
    DatasetContract,
    DiemengSourceAdapter,
    FieldContract,
    SourceHealth,
    harden_tushare_client_transport,
    normalize_diemeng_base_url,
    tushare_client_uses_direct_transport,
    validate_production_diemeng_base_url,
    validate_tushare_https_origin,
)
from factor_lab.research_os.data_sync import resolve_credential
from factor_lab.research_os.credentials import (
    CredentialResolutionError,
    resolve_credential_ref,
)

LLM_ENV_KEYS = [
    "FACTOR_LAB_DECISION_PROVIDER",
    "FACTOR_LAB_LIVE_DECISION_PROVIDER",
    "FACTOR_LAB_OBSERVATION_DECISION_PROVIDER",
    "FACTOR_LAB_LLM_BASE_URL",
    "FACTOR_LAB_LLM_MODEL",
    "FACTOR_LAB_LLM_API_KEY",
    "FACTOR_LAB_LLM_API_KEY_REF",
    "FACTOR_LAB_LLM_API_FORMAT",
]

LLM_PROFILE_ENV_KEYS = [
    "FACTOR_LAB_LLM_PROFILES_JSON",
    "FACTOR_LAB_LLM_FALLBACK_ORDER",
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
    "FACTOR_LAB_SECRETS_DIR",
    "TUSHARE_TOKEN",
    "TUSHARE_TOKEN_FILE",
    "DIEMENG_API_KEY",
    "DIEMENG_API_KEY_FILE",
]

DATA_SOURCE_TYPE_OPTIONS = [
    {"value": "tushare", "label": "Tushare"},
    {"value": "akshare", "label": "AkShare"},
    {"value": "diemeng", "label": "梦蝶 / 灵启数据"},
    {"value": "local_file", "label": "本地 PIT 文件"},
]

LEGACY_DATA_SOURCE_KEY_ENV = {
    "tushare": "TUSHARE_TOKEN",
    "diemeng": "DIEMENG_API_KEY",
}

DATA_SOURCE_FILE_ENV = {
    "tushare": "TUSHARE_TOKEN_FILE",
    "diemeng": "DIEMENG_API_KEY_FILE",
}

_SAFE_SECRET_NAME = re.compile(r"[^A-Za-z0-9_.-]+")
_VALID_SECRET_NAME = re.compile(r"[A-Za-z0-9_.-]+")
_CANONICAL_SOURCE_SECRET = {
    "tushare": "tushare_token",
    "diemeng": "diemeng_api_key",
}
_CANONICAL_LLM_SECRET = "llm_api_key"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)

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
    _assert_no_link_components(env_path.parent)
    if _is_link_or_reparse(env_path):
        raise ValueError("设置文件不能是符号链接或重解析点")
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


def _secrets_dir(environ: dict[str, str]) -> Path:
    configured = str(environ.get("FACTOR_LAB_SECRETS_DIR") or "").strip()
    if configured:
        return Path(configured)
    if os.name == "nt":
        return Path(r"H:\Program Data\factor-lab-runtime\secrets")
    return Path("/run/secrets")


def _secret_name(source_type: str, name: str) -> str:
    slug = _SAFE_SECRET_NAME.sub("-", str(name).strip()).strip("-._")
    if not slug:
        slug = source_type
    return f"source-{source_type}-{slug}"[:128]


def _llm_secret_name(name: str) -> str:
    slug = _SAFE_SECRET_NAME.sub("-", str(name).strip()).strip("-._")
    return f"llm-{slug or 'profile'}"[:128]


def _is_link_or_reparse(path: Path) -> bool:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(info.st_mode) or bool(
        int(getattr(info, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _assert_no_link_components(path: Path) -> None:
    absolute = path.absolute()
    candidates = [absolute, *absolute.parents]
    for candidate in reversed(candidates):
        if candidate.exists() and _is_link_or_reparse(candidate):
            raise ValueError(
                "设置或凭据路径不能穿过符号链接或重解析点"
            )


def _safe_private_directory(path: Path) -> Path:
    _assert_no_link_components(path.parent)
    if path.exists() and _is_link_or_reparse(path):
        raise ValueError("凭据目录不能是符号链接或重解析点")
    path.mkdir(parents=True, exist_ok=True)
    if _is_link_or_reparse(path) or not path.is_dir():
        raise ValueError("凭据目录必须是普通目录")
    try:
        path.chmod(0o700)
    except OSError:
        pass
    return path


def _atomic_write_private(path: Path, content: str) -> None:
    """Atomically replace a private regular file without following links.

    The random temporary file is created with O_EXCL inside a non-link editor
    directory.  Existing targets may be regular files only; symlinks and
    Windows reparse points fail closed before replacement.
    """

    parent = _safe_private_directory(path.parent)
    if path.exists() or _is_link_or_reparse(path):
        if _is_link_or_reparse(path):
            raise ValueError("设置或凭据目标不能是符号链接或重解析点")
        if not path.is_file():
            raise ValueError("设置或凭据目标必须是普通文件")

    temporary: Path | None = None
    descriptor: int | None = None
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= int(getattr(os, "O_NOFOLLOW", 0))
    for _ in range(16):
        candidate = parent / f".{path.name}.{secrets.token_hex(12)}.tmp"
        try:
            descriptor = os.open(candidate, flags, 0o600)
        except FileExistsError:
            continue
        temporary = candidate
        break
    if descriptor is None or temporary is None:
        raise OSError("无法创建独占临时设置文件")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = None
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            temporary.chmod(0o600)
        except OSError:
            pass
        if path.exists() or _is_link_or_reparse(path):
            if _is_link_or_reparse(path) or not path.is_file():
                raise ValueError("设置或凭据目标在写入期间变为不安全对象")
        os.replace(temporary, path)
        temporary = None
        if _is_link_or_reparse(path) or not path.is_file():
            raise ValueError("原子替换未生成普通文件")
        try:
            path.chmod(0o600)
        except OSError:
            pass
        directory_flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
        try:
            directory_descriptor = os.open(parent, directory_flags)
        except OSError:
            directory_descriptor = None
        if directory_descriptor is not None:
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _credential_ref_name(reference: str) -> str:
    normalized = str(reference or "").strip()
    if not normalized:
        return ""
    if not normalized.startswith("secret://"):
        raise ValueError("credential_ref 必须使用 secret://")
    name = normalized.removeprefix("secret://")
    if not name or not _VALID_SECRET_NAME.fullmatch(name):
        raise ValueError("credential_ref 包含不安全的凭据名称")
    return name


def _validate_source_credential_ref(reference: str, source_type: str) -> str:
    name = _credential_ref_name(reference)
    if not name:
        return ""
    normalized_type = normalize_data_source_type(source_type)
    canonical = _CANONICAL_SOURCE_SECRET.get(normalized_type)
    prefix = f"source-{normalized_type}-"
    if name != canonical and not (name.startswith(prefix) and len(name) > len(prefix)):
        raise ValueError(
            f"{normalized_type} 数据源只能引用自身的受管凭据"
        )
    return f"secret://{name}"


def _validate_llm_credential_ref(reference: str) -> str:
    name = _credential_ref_name(reference)
    if not name:
        return ""
    if name != _CANONICAL_LLM_SECRET and not (
        name.startswith("llm-") and len(name) > len("llm-")
    ):
        raise ValueError("模型配置只能引用受管 LLM 凭据")
    return f"secret://{name}"


def _write_profile_secret(
    *,
    source_type: str,
    name: str,
    value: str,
    environ: dict[str, str],
    credential_ref: str = "",
    canonical: bool = False,
) -> str:
    secret = str(value or "").strip()
    if not secret or secret == "replace-me":
        raise ValueError("数据源凭据为空或仍是占位符")
    root = _safe_private_directory(_secrets_dir(environ))
    validated_ref = _validate_source_credential_ref(credential_ref, source_type)
    referenced_name = _credential_ref_name(validated_ref)
    if referenced_name:
        secret_name = referenced_name
    elif canonical and source_type in _CANONICAL_SOURCE_SECRET:
        secret_name = _CANONICAL_SOURCE_SECRET[source_type]
    else:
        secret_name = _secret_name(source_type, name)
    target = root / secret_name
    _atomic_write_private(target, secret + "\n")
    return f"secret://{secret_name}"


def _profile_credential(profile: dict[str, Any], environ: dict[str, str]) -> str:
    inline = str(profile.get("api_key") or "").strip()
    if inline:
        return inline
    credential_ref = str(profile.get("credential_ref") or "").strip()
    if not credential_ref:
        return ""
    source_type = normalize_data_source_type(profile.get("source_type"))
    try:
        credential_ref = _validate_source_credential_ref(
            credential_ref, source_type
        )
    except ValueError:
        return ""
    env_name = LEGACY_DATA_SOURCE_KEY_ENV.get(source_type, "DATA_SOURCE_API_KEY")
    try:
        return resolve_credential(
            credential_ref=credential_ref,
            env_name=env_name,
            env={
                **environ,
                "FACTOR_LAB_SECRETS_DIR": str(_secrets_dir(environ)),
            },
        )
    except (OSError, ValueError):
        return ""


def _write_llm_profile_secret(
    *,
    name: str,
    value: str,
    environ: dict[str, str],
    credential_ref: str = "",
    canonical: bool = False,
) -> str:
    secret = str(value or "").strip()
    if not secret or secret == "replace-me":
        raise ValueError("模型凭据为空或仍是占位符")
    root = _safe_private_directory(_secrets_dir(environ))
    validated_ref = _validate_llm_credential_ref(credential_ref)
    referenced_name = _credential_ref_name(validated_ref)
    if referenced_name:
        secret_name = referenced_name
    elif canonical:
        secret_name = _CANONICAL_LLM_SECRET
    else:
        secret_name = _llm_secret_name(name)
    target = root / secret_name
    _atomic_write_private(target, secret + "\n")
    return f"secret://{secret_name}"


def resolve_llm_profile_credential(
    profile: dict[str, Any], environ: dict[str, str]
) -> str:
    inline = str(profile.get("api_key") or "").strip()
    if inline:
        return inline
    reference = str(profile.get("credential_ref") or "").strip()
    if not reference:
        return ""
    try:
        reference = _validate_llm_credential_ref(reference)
        return resolve_credential_ref(
            reference,
            env=environ,
            secrets_root=_secrets_dir(environ),
            allow_plain_env=False,
        )
    except (CredentialResolutionError, OSError, ValueError):
        return ""


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
    if raw in {"tushare", "akshare", "local_file", "diemeng", "custom"}:
        return raw
    return "custom"


def _data_source_extra(value: Any) -> dict[str, str]:
    source = value if isinstance(value, dict) else {}
    # Keep the profile schema deliberately small.  Endpoint and field mapping
    # remain in reviewed orchestration config rather than being editable from
    # the WebUI.
    return {
        "root": str(source.get("root") or "").strip(),
        "probe_file": str(source.get("probe_file") or "").strip(),
        "base_url": str(source.get("base_url") or "").strip(),
    }


def _safe_probe_error(exc: Exception, *, secret: str = "") -> str:
    message = f"{type(exc).__name__}: {exc}"
    if secret:
        message = message.replace(secret, "***")
    return message[:500]


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
    source_type = normalize_data_source_type(profile.get("source_type"))
    credential_ref = _validate_source_credential_ref(
        str(profile.get("credential_ref") or ""), source_type
    )
    extra = _data_source_extra(profile.get("extra"))
    return {
        "name": str(profile.get("name") or ""),
        "source_type": source_type,
        "api_key": "",
        "api_key_configured": bool(api_key or credential_ref),
        "api_key_masked": mask_secret(api_key) if api_key else (
            "已安全配置" if credential_ref else "未配置"
        ),
        "credential_ref": credential_ref,
        "enabled": coerce_boolish(profile.get("enabled", True), default=True),
        "notes": str(profile.get("notes") or ""),
        "extra": extra,
        "root": extra["root"],
        "probe_file": extra["probe_file"],
        "base_url": extra["base_url"],
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
                credential_ref = _validate_source_credential_ref(
                    str(item.get("credential_ref") or ""), source_type
                )
                notes = str(item.get("notes") or "")
                enabled = coerce_boolish(item.get("enabled", True), default=True)
                if not any([name, api_key, notes]) and source_type == "custom":
                    continue
                profiles.append({
                    "name": name,
                    "source_type": source_type,
                    "api_key": api_key,
                    "credential_ref": credential_ref,
                    "enabled": enabled,
                    "notes": notes,
                    "extra": _data_source_extra(item.get("extra")),
                })
    if not profiles:
        legacy_tushare = values.get("TUSHARE_TOKEN") or environ.get("TUSHARE_TOKEN") or ""
        legacy_diemeng = values.get("DIEMENG_API_KEY") or environ.get("DIEMENG_API_KEY") or ""
        if legacy_tushare:
            profiles.append({"name": "primary-tushare", "source_type": "tushare", "api_key": legacy_tushare, "credential_ref": "", "enabled": True, "notes": "", "extra": {}})
        if legacy_diemeng:
            profiles.append({"name": "primary-diemeng", "source_type": "diemeng", "api_key": legacy_diemeng, "credential_ref": "", "enabled": True, "notes": "", "extra": {}})
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
    existing_extras = {
        (
            normalize_data_source_type(profile.get("source_type")),
            str(profile.get("name") or ""),
        ): _data_source_extra(profile.get("extra"))
        for profile in existing_profiles
    }
    existing_refs = {
        (
            normalize_data_source_type(profile.get("source_type")),
            str(profile.get("name") or ""),
        ): str(profile.get("credential_ref") or "")
        for profile in existing_profiles
    }
    profiles: list[dict[str, Any]] = []
    for index in range(20):
        source_type = normalize_data_source_type(form.get(f"source_type_{index}"))
        name = (form.get(f"source_name_{index}") or "").strip()
        api_key = (form.get(f"source_api_key_{index}") or "").strip()
        notes = (form.get(f"source_notes_{index}") or "").strip()
        root = (form.get(f"source_root_{index}") or "").strip()
        probe_file = (form.get(f"source_probe_file_{index}") or "").strip()
        base_url = (form.get(f"source_base_url_{index}") or "").strip()
        enabled = form.get(f"source_enabled_{index}") in {"on", "1", "true", "yes"}
        if not any([name, api_key, notes, root, probe_file, base_url]):
            continue
        if not name:
            name = f"{source_type}-{index + 1}"
        indexed_existing = (
            existing_profiles[index]
            if index < len(existing_profiles) and isinstance(existing_profiles[index], dict)
            else {}
        )
        indexed_type = normalize_data_source_type(indexed_existing.get("source_type"))
        if not api_key:
            api_key = existing_keys.get((source_type, name), "")
            if not api_key and indexed_type == source_type:
                api_key = str(indexed_existing.get("api_key") or "")
        previous_extra = existing_extras.get((source_type, name), {})
        if not root:
            root = str(previous_extra.get("root") or "")
        if not probe_file:
            probe_file = str(previous_extra.get("probe_file") or "")
        if not base_url:
            base_url = str(previous_extra.get("base_url") or "")
        credential_ref = existing_refs.get((source_type, name), "")
        if not credential_ref and indexed_type == source_type:
            credential_ref = str(indexed_existing.get("credential_ref") or "")
        profiles.append({
            "name": name,
            "source_type": source_type,
            "api_key": api_key,
            "credential_ref": credential_ref,
            "enabled": enabled,
            "notes": notes,
            "extra": {"root": root, "probe_file": probe_file, "base_url": base_url},
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
    seen_credential_types: set[str] = set()
    for profile in profiles:
        source_type = normalize_data_source_type(profile.get("source_type"))
        extra = _data_source_extra(profile.get("extra"))
        if source_type == "diemeng" and extra["base_url"]:
            extra["base_url"] = normalize_diemeng_base_url(extra["base_url"])
        if source_type == "local_file" and extra["root"]:
            root = Path(extra["root"])
            if not root.is_absolute():
                root = (path.parent / root).resolve()
            extra["root"] = str(root)
        profile["extra"] = extra

        # The form may contain a new key, or an old profile may still contain
        # an inline migration-era key.  Persist either exactly once into the
        # restricted secret directory, then keep only the opaque reference in
        # settings and process environment state.
        inline = str(profile.get("api_key") or "").strip()
        credential_ref = _validate_source_credential_ref(
            str(profile.get("credential_ref") or "").strip(), source_type
        )
        if inline:
            credential_ref = _write_profile_secret(
                source_type=source_type,
                name=str(profile.get("name") or source_type),
                value=inline,
                environ=environ,
                credential_ref=credential_ref,
                canonical=source_type not in seen_credential_types,
            )
        if source_type in _CANONICAL_SOURCE_SECRET:
            seen_credential_types.add(source_type)
        profile["credential_ref"] = credential_ref
        profile["api_key"] = ""

    primary = first_enabled_source_profile(profiles)
    requested: dict[str, str] = {
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(profiles, ensure_ascii=False, separators=(",", ":")),
        "FACTOR_LAB_DATA_SOURCE_ORDER": order,
        "FACTOR_LAB_PRIMARY_DATA_SOURCE": str(primary.get("source_type") or ""),
        # Secret roots are process/deployment configuration, never shared
        # profile state.  A WebUI editor mount and a worker /run/secrets mount
        # may deliberately differ while resolving the same secret names.
        "FACTOR_LAB_SECRETS_DIR": "",
    }
    for source_type, env_key in LEGACY_DATA_SOURCE_KEY_ENV.items():
        # Erase migration-era plaintext variables.  Research OS consumes the
        # credential_ref from the profile; Compose supplies *_FILE overrides
        # for legacy subprocesses that still need one.
        requested[env_key] = ""
        requested[DATA_SOURCE_FILE_ENV[source_type]] = ""
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
    _atomic_write_private(path, "\n".join(updated_lines).rstrip() + "\n")
    for key, value in requested.items():
        if key != "FACTOR_LAB_SECRETS_DIR":
            environ[key] = value
    return load_data_source_settings(env_file_func, environ)


def test_data_source_connection(
    profile: dict[str, Any], environ: dict[str, str] | None = None
) -> dict[str, Any]:
    active_environ = environ if environ is not None else os.environ
    source_type = normalize_data_source_type(profile.get("source_type"))
    name = str(profile.get("name") or source_type or "data-source")
    environment = (
        str(active_environ.get("FACTOR_LAB_ENVIRONMENT") or "")
        .strip()
        .casefold()
    )
    production_role = (
        str(active_environ.get("FACTOR_LAB_PRODUCTION_ROLE") or "")
        .strip()
        .casefold()
    )
    if production_role and production_role != "webui":
        # This helper is a WebUI-only credential-bearing operation.  An
        # unknown/misrouted production role must not downgrade it to the local
        # transport path merely because the exact ``webui`` marker is absent.
        return {
            "ok": False,
            "message": "数据源测试失败：生产 WebUI 角色配置无效。",
            "source_type": source_type,
            "name": name,
        }
    strict_webui_transport = bool(
        environment == "production" or production_role == "webui"
    )

    # A test button is still a credential-bearing provider request.  The
    # production WebUI must inspect the public route contract before resolving
    # a secret reference, otherwise the pinned HTTP-only Tushare SDK (or an
    # attacker-controlled Diemeng form URL) can exfiltrate a credential before
    # the worker-side production validator ever sees the profile.
    diemeng_base_url: str | None = None
    tushare_origin: str | None = None
    if source_type == "tushare":
        try:
            tushare_origin = validate_tushare_https_origin(
                "https://api.tushare.pro/dataapi"
            )
        except Exception:
            return {
                "ok": False,
                "message": "数据源测试失败：Tushare HTTPS 传输配置无效。",
                "source_type": source_type,
                "name": name,
            }
    if source_type == "diemeng":
        extra = _data_source_extra(profile.get("extra"))
        candidate_url = extra["base_url"] or "https://mg.diemeng.chat"
        if strict_webui_transport:
            try:
                diemeng_base_url = validate_production_diemeng_base_url(
                    candidate_url
                )
            except Exception:
                return {
                    "ok": False,
                    "message": "数据源测试失败：生产环境仅允许已审查的梦蝶 HTTPS 地址。",
                    "source_type": source_type,
                    "name": name,
                }
        else:
            diemeng_base_url = candidate_url

    api_key = _profile_credential(profile, active_environ)
    if source_type == "tushare":
        if not api_key:
            return {"ok": False, "message": "数据源测试失败：Token 未填写。", "source_type": source_type, "name": name}
        try:
            import tushare as ts
            pro = ts.pro_api(api_key)
            setattr(pro, "_DataApi__http_url", tushare_origin)
            pro = harden_tushare_client_transport(pro)
            if strict_webui_transport and not tushare_client_uses_direct_transport(pro):
                raise RuntimeError(
                    "Tushare production client transport is not sealed"
                )
            frame = pro.query("trade_cal", exchange="SSE", start_date="20240102", end_date="20240103", fields="cal_date,is_open")
            if frame is None or not hasattr(frame, "empty") or frame.empty:
                raise RuntimeError("trade_cal 返回空结果")
            return {"ok": True, "message": "数据源测试成功", "source_type": source_type, "name": name}
        except Exception as exc:
            return {"ok": False, "message": f"数据源测试失败：{_safe_probe_error(exc, secret=api_key)}", "source_type": source_type, "name": name}
    if source_type == "diemeng":
        if not api_key:
            return {"ok": False, "message": "数据源测试失败：API Key 未填写。", "source_type": source_type, "name": name}
        assert diemeng_base_url is not None
        try:
            contract = DatasetContract(
                dataset="trade_calendar",
                key_fields=("date",),
                fields=(
                    FieldContract(name="date", dtype="string", nullable=False),
                    FieldContract(name="is_open", dtype="int64", nullable=False),
                ),
                event_time_field="date",
                release_timing="provider calendar endpoint",
            )
            adapter = DiemengSourceAdapter(
                base_url=diemeng_base_url,
                api_key=api_key,
                contracts=(contract,),
                endpoint_map={"trade_calendar": "/basic/calendar"},
                response_paths={"trade_calendar": "data"},
                probe_dataset="trade_calendar",
                probe_parameters={
                    "start_time": "2024-01-02",
                    "end_time": "2024-01-03",
                },
                timeout_seconds=20,
                max_attempts=1,
            )
            result = adapter.probe()
            if result.health is not SourceHealth.HEALTHY:
                raise RuntimeError(result.message)
            return {
                "ok": True,
                "message": "数据源测试成功（交易日历，两交易日）",
                "source_type": source_type,
                "name": name,
            }
        except Exception as exc:
            return {
                "ok": False,
                "message": f"数据源测试失败：{_safe_probe_error(exc, secret=api_key)}",
                "source_type": source_type,
                "name": name,
            }
    if source_type == "akshare":
        try:
            import akshare as ak
            frame = ak.stock_zh_a_hist(
                symbol="000001",
                period="daily",
                start_date="20240102",
                end_date="20240103",
                adjust="",
            )
            if frame is None or not hasattr(frame, "empty") or frame.empty:
                raise RuntimeError("有界日线探测返回空结果")
            return {"ok": True, "message": "数据源测试成功（000001，两交易日）", "source_type": source_type, "name": name}
        except Exception as exc:
            return {"ok": False, "message": f"数据源测试失败：{_safe_probe_error(exc)}", "source_type": source_type, "name": name}
    if source_type == "local_file":
        extra = _data_source_extra(profile.get("extra"))
        root_text = extra["root"]
        relative_text = extra["probe_file"]
        if not root_text or not relative_text:
            return {"ok": False, "message": "数据源测试失败：必须填写根目录和根内测试文件。", "source_type": source_type, "name": name}
        try:
            root = Path(root_text).resolve(strict=True)
            relative = Path(relative_text)
            if relative.is_absolute():
                raise ValueError("测试文件必须是相对路径")
            target = (root / relative).resolve(strict=True)
            target.relative_to(root)
            if not root.is_dir() or not target.is_file():
                raise FileNotFoundError("根目录或测试文件不存在")
            if target.suffix.lower() not in {".parquet", ".pq", ".csv", ".json", ".jsonl", ".ndjson"}:
                raise ValueError("测试文件格式不受支持")
            with target.open("rb") as handle:
                if not handle.read(1):
                    raise ValueError("测试文件为空")
            return {"ok": True, "message": "本地 PIT 文件可读且位于限定根目录内", "source_type": source_type, "name": name}
        except Exception as exc:
            return {"ok": False, "message": f"数据源测试失败：{_safe_probe_error(exc)}", "source_type": source_type, "name": name}
    return {"ok": False, "message": f"数据源测试失败：类型 {source_type!r} 不受 Research OS 支持。", "source_type": source_type, "name": name}


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
                credential_ref = _validate_llm_credential_ref(
                    str(item.get("credential_ref") or "")
                )
                profiles.append({
                    "name": name,
                    "base_url": str(item.get("base_url") or ""),
                    "model": str(item.get("model") or ""),
                    "api_format": normalize_llm_api_format(item.get("api_format"), item.get("model")),
                    "api_key": "",
                    "credential_ref": credential_ref,
                    "api_key_configured": bool(api_key or credential_ref),
                    "api_key_masked": (
                        mask_secret(api_key) if api_key else (
                            "已安全配置" if credential_ref else "未配置"
                        )
                    ),
                    "enabled": coerce_boolish(item.get("enabled", True), default=True),
                })
    if not profiles:
        api_key = values.get("FACTOR_LAB_LLM_API_KEY") or environ.get("FACTOR_LAB_LLM_API_KEY") or ""
        credential_ref = _validate_llm_credential_ref(
            values.get("FACTOR_LAB_LLM_API_KEY_REF")
            or environ.get("FACTOR_LAB_LLM_API_KEY_REF")
            or ""
        )
        profiles.append({
            "name": values.get("FACTOR_LAB_LLM_PROFILE_NAME") or environ.get("FACTOR_LAB_LLM_PROFILE_NAME") or "default",
            "base_url": values.get("FACTOR_LAB_LLM_BASE_URL") or environ.get("FACTOR_LAB_LLM_BASE_URL") or "",
            "model": values.get("FACTOR_LAB_LLM_MODEL") or environ.get("FACTOR_LAB_LLM_MODEL") or "",
            "api_format": normalize_llm_api_format(values.get("FACTOR_LAB_LLM_API_FORMAT") or environ.get("FACTOR_LAB_LLM_API_FORMAT"), values.get("FACTOR_LAB_LLM_MODEL") or environ.get("FACTOR_LAB_LLM_MODEL")),
            "api_key": "",
            "credential_ref": credential_ref,
            "api_key_configured": bool(api_key or credential_ref),
            "api_key_masked": (
                mask_secret(api_key) if api_key else (
                    "已安全配置" if credential_ref else "未配置"
                )
            ),
            "enabled": True,
        })
    return ordered_profile_list(profiles, fallback_order), fallback_order or ",".join(str(profile.get("name")) for profile in profiles if profile.get("name"))


def profiles_from_form(form: dict[str, str], existing_profiles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    existing_keys = {str(profile.get("name") or ""): str(profile.get("api_key") or "") for profile in existing_profiles}
    existing_refs = {
        str(profile.get("name") or ""): str(profile.get("credential_ref") or "")
        for profile in existing_profiles
    }
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
        profiles.append({"name": name, "base_url": base_url, "model": model, "api_format": api_format, "api_key": api_key, "credential_ref": existing_refs.get(name, ""), "enabled": enabled, "order": (form.get(f"profile_order_{index}") or "").strip(), "_index": index})
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
) -> dict[str, Any]:
    path = Path(env_file_func())
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_values = read_env_values(env_file_func, path)
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
            "credential_ref": environ.get("FACTOR_LAB_LLM_API_KEY_REF") or existing_values.get("FACTOR_LAB_LLM_API_KEY_REF", ""),
        }]
    if any(key.startswith("profile_") for key in form):
        profiles, fallback_order = profiles_from_form(form, existing_profiles)
    else:
        current_api_key = environ.get("FACTOR_LAB_LLM_API_KEY") or existing_values.get("FACTOR_LAB_LLM_API_KEY", "")
        current_ref = environ.get("FACTOR_LAB_LLM_API_KEY_REF") or existing_values.get("FACTOR_LAB_LLM_API_KEY_REF", "")
        profiles = [{
            "name": "default",
            "base_url": (form.get("base_url") or "").strip(),
            "model": (form.get("model") or "").strip(),
            "api_format": normalize_llm_api_format(form.get("api_format"), form.get("model")),
            "api_key": (form.get("api_key") or "").strip() or current_api_key,
            "credential_ref": current_ref,
            "enabled": True,
        }]
        fallback_order = "default"
    secured_profiles: list[dict[str, Any]] = []
    for index, raw_profile in enumerate(profiles):
        profile = dict(raw_profile)
        profile.pop("order", None)
        profile.pop("_index", None)
        inline = str(profile.get("api_key") or "").strip()
        credential_ref = _validate_llm_credential_ref(
            str(profile.get("credential_ref") or "").strip()
        )
        if inline:
            credential_ref = _write_llm_profile_secret(
                name=str(profile.get("name") or f"profile-{index + 1}"),
                value=inline,
                environ=environ,
                credential_ref=credential_ref,
                canonical=index == 0,
            )
        profile["api_key"] = ""
        profile["credential_ref"] = credential_ref
        secured_profiles.append(profile)
    profiles = secured_profiles
    primary = first_enabled_profile(profiles) if profiles else {"base_url": "", "model": "", "api_key": ""}
    requested: dict[str, str] = {}
    for form_key, env_key in LLM_FORM_TO_ENV.items():
        requested[env_key] = (form.get(form_key) or "").strip()
    requested.update({
        "FACTOR_LAB_LLM_BASE_URL": str(primary.get("base_url") or ""),
        "FACTOR_LAB_LLM_MODEL": str(primary.get("model") or ""),
        "FACTOR_LAB_LLM_API_KEY": "",
        "FACTOR_LAB_LLM_API_KEY_REF": str(primary.get("credential_ref") or ""),
        "FACTOR_LAB_LLM_API_FORMAT": str(primary.get("api_format") or normalize_llm_api_format(None, primary.get("model"))),
        "FACTOR_LAB_LLM_PROFILES_JSON": json.dumps(profiles, ensure_ascii=False, separators=(",", ":")),
        "FACTOR_LAB_LLM_FALLBACK_ORDER": fallback_order,
    })
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    updated_lines: list[str] = []
    managed_keys = [*LLM_ENV_KEYS, *LLM_PROFILE_ENV_KEYS]
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
    _atomic_write_private(path, "\n".join(updated_lines).rstrip() + "\n")
    for key, value in requested.items():
        environ[key] = value
    return load_llm_settings(env_file_func, environ)


def test_llm_profile_connection(
    profile: dict[str, Any], *, environ: dict[str, str] | None = None
) -> dict[str, Any]:
    from factor_lab.hermes_decision_router import HermesDecisionRouter

    base_url = str(profile.get("base_url") or "").strip().rstrip("/")
    model = str(profile.get("model") or "").strip()
    api_key = resolve_llm_profile_credential(profile, environ or os.environ)
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
