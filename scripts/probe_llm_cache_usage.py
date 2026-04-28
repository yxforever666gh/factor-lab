#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.llm_provider_router import DecisionProviderRouter
from factor_lab.paths import artifacts_dir, env_file


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _normalize_api_format(value: Any, model: str | None = None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"responses", "openai_response"}:
        raw = "openai_responses"
    if raw in {"chat", "chat_completions", "chat_completion", "openai_chat", "openai_chat_completions"}:
        raw = "openai"
    if raw in {"messages", "anthropic_messages", "claude"}:
        raw = "anthropic"
    if raw in {"openai", "openai_responses", "anthropic"}:
        return raw
    model_text = str(model or "").lower()
    if model_text.startswith("claude") or "opus" in model_text:
        return "anthropic"
    if model_text.startswith("gpt-5"):
        return "openai_responses"
    return "openai"


def _cache_observability(usage: dict[str, Any]) -> dict[str, Any]:
    raw_usage = usage.get("raw_usage") if isinstance(usage.get("raw_usage"), dict) else {}
    raw_text = json.dumps(raw_usage, ensure_ascii=False, sort_keys=True)
    cache_like_raw_keys = sorted(
        key
        for key in _flatten_keys(raw_usage)
        if any(part in key.lower() for part in ["cache", "cached"])
    )
    return {
        "usage_source": usage.get("usage_source"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
        "cached_tokens": usage.get("cached_tokens"),
        "cache_creation_tokens": usage.get("cache_creation_tokens"),
        "uncached_prompt_tokens": usage.get("uncached_prompt_tokens"),
        "has_cached_tokens_field": usage.get("cached_tokens") is not None,
        "has_cache_creation_tokens_field": usage.get("cache_creation_tokens") is not None,
        "cache_usage_observable": usage.get("cached_tokens") is not None or usage.get("cache_creation_tokens") is not None,
        "raw_cache_like_keys": cache_like_raw_keys,
        "raw_usage": raw_usage,
        "raw_usage_chars": len(raw_text),
    }


def _flatten_keys(value: Any, prefix: str = "") -> list[str]:
    keys: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            keys.append(name)
            keys.extend(_flatten_keys(child, name))
    elif isinstance(value, list):
        for index, child in enumerate(value[:5]):
            keys.extend(_flatten_keys(child, f"{prefix}[{index}]"))
    return keys


def _profile_public(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": profile.get("name"),
        "base_url": profile.get("base_url"),
        "model": profile.get("model"),
        "api_format": profile.get("api_format"),
        "enabled": bool(profile.get("enabled", True)),
    }


def _build_request_body(api_format: str, model: str, stable_prompt: str, max_output_tokens: int) -> dict[str, Any]:
    system_prompt = "You are a cache observability probe. Return exactly: OK"
    if api_format == "anthropic":
        return {
            "model": model,
            "system": system_prompt,
            "messages": [{"role": "user", "content": stable_prompt}],
            "temperature": 0,
            "max_tokens": max_output_tokens,
        }
    if api_format == "openai_responses":
        return {
            "model": model,
            "input": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": stable_prompt},
            ],
            "temperature": 0,
            "max_output_tokens": max_output_tokens,
        }
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": stable_prompt},
        ],
        "temperature": 0,
        "max_tokens": max_output_tokens,
    }


def _probe_once(
    router: DecisionProviderRouter,
    profile: dict[str, Any],
    stable_prompt: str,
    timeout: float,
    max_output_tokens: int,
) -> dict[str, Any]:
    base_url = str(profile.get("base_url") or "").rstrip("/")
    api_key = str(profile.get("api_key") or "")
    model = str(profile.get("model") or router.model)
    api_format = _normalize_api_format(profile.get("api_format"), model)
    started = time.perf_counter()
    request_body = _build_request_body(api_format, model, stable_prompt, max_output_tokens)
    auth_scheme = "anthropic" if api_format == "anthropic" else "bearer"
    req = urllib.request.Request(
        url=router._real_llm_endpoint_url(base_url, api_format),
        data=json.dumps(request_body).encode("utf-8"),
        headers=router._real_llm_headers(api_key, auth_scheme=auth_scheme),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = json.loads(response.read().decode("utf-8", errors="ignore"))
        usage = router._extract_llm_usage(raw if isinstance(raw, dict) else {}, api_format)
        result = {
            "ok": True,
            "http_status": 200,
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "usage": _cache_observability(usage),
        }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        usage = router._usage_from_http_error_body(body, api_format)
        result = {
            "ok": False,
            "http_status": exc.code,
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "error_type": f"http_error:{exc.code}",
            "error_message_preview": body[:500],
            "usage": _cache_observability(usage),
        }
    except Exception as exc:
        result = {
            "ok": False,
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "error_type": type(exc).__name__,
            "error_message_preview": str(exc)[:500],
            "usage": _cache_observability({"usage_source": "missing", "raw_usage": {}}),
        }
    return result


def _stable_prompt() -> str:
    repeated = "Factor Lab cache observability stable prefix. " * 220
    return (
        "Return exactly OK. This prompt is intentionally long and identical across probe attempts so "
        "providers that expose prompt-cache usage have a chance to report cache read/write fields.\n\n"
        + repeated
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe whether configured LLM providers return cache usage fields.")
    parser.add_argument("--profiles", default="", help="Comma-separated profile names to probe. Default: all enabled profiles.")
    parser.add_argument("--repeats", type=int, default=2, help="Identical calls per profile. Default: 2")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-output-tokens", type=int, default=32)
    parser.add_argument("--output", default="", help="Output JSON path. Default: artifacts/llm_cache_usage_probe.json")
    args = parser.parse_args()

    _load_env_file(env_file())
    router = DecisionProviderRouter(provider="real_llm")
    profiles = router._real_llm_profiles()
    wanted = {item.strip() for item in args.profiles.split(",") if item.strip()}
    if wanted:
        profiles = [profile for profile in profiles if str(profile.get("name") or "") in wanted]

    prompt = _stable_prompt()
    report: dict[str, Any] = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "probe_kind": "llm_cache_usage_observability",
        "prompt_chars": len(prompt),
        "estimated_prompt_tokens_4c": len(prompt) // 4,
        "repeats": max(args.repeats, 1),
        "profiles": [],
    }

    for profile in profiles:
        if not profile.get("enabled", True):
            continue
        row = {"profile": _profile_public(profile), "attempts": []}
        for attempt in range(max(args.repeats, 1)):
            result = _probe_once(router, profile, prompt, args.timeout, args.max_output_tokens)
            result["attempt"] = attempt + 1
            row["attempts"].append(result)
        row["cache_usage_observable"] = any(
            attempt.get("usage", {}).get("cache_usage_observable") for attempt in row["attempts"]
        )
        row["returned_cache_like_raw_keys"] = sorted(
            {
                key
                for attempt in row["attempts"]
                for key in attempt.get("usage", {}).get("raw_cache_like_keys", [])
            }
        )
        report["profiles"].append(row)

    out = Path(args.output) if args.output else artifacts_dir() / "llm_cache_usage_probe.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    print(f"wrote={out}")
    print(f"profiles={len(report['profiles'])} prompt_chars={report['prompt_chars']} repeats={report['repeats']}")
    for row in report["profiles"]:
        profile = row["profile"]
        attempts = row["attempts"]
        statuses = [str(attempt.get("http_status") or attempt.get("error_type") or "ok") for attempt in attempts]
        cache_values = [attempt.get("usage", {}).get("cached_tokens") for attempt in attempts]
        cache_missing = [attempt.get("usage", {}).get("cached_tokens") is None for attempt in attempts]
        creation_values = [attempt.get("usage", {}).get("cache_creation_tokens") for attempt in attempts]
        print(
            f"{profile.get('name')} model={profile.get('model')} api_format={profile.get('api_format')} "
            f"observable={row['cache_usage_observable']} statuses={statuses} "
            f"cached_tokens={cache_values} cached_missing={cache_missing} "
            f"cache_creation_tokens={creation_values} raw_cache_keys={row['returned_cache_like_raw_keys']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
