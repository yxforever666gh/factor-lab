from __future__ import annotations

import json
import os
from typing import Any

# Published API prices per 1M tokens. Update with FACTOR_LAB_LLM_PRICING_JSON if vendor pricing changes.
# OpenAI GPT-5 family: input $1.25, cached input $0.125, output $10 per 1M tokens.
# Opus 4.7 family: input $15, cache write 5m $18.75, cache read $1.50, output $75 per 1M tokens.
DEFAULT_LLM_PRICING: dict[str, dict[str, Any]] = {
    "gpt-5": {
        "pricing_family": "gpt-5",
        "match": ["gpt-5", "gpt5"],
        "input_per_mtok_usd": 1.25,
        "cached_input_per_mtok_usd": 0.125,
        "cache_creation_per_mtok_usd": 1.25,
        "output_per_mtok_usd": 10.0,
    },
    "opus4.7": {
        "pricing_family": "opus4.7",
        "match": ["opus4.7", "opus-4.7", "opus_4.7", "claude-opus-4.7", "claude-opus4.7", "opus"],
        "input_per_mtok_usd": 15.0,
        "cached_input_per_mtok_usd": 1.50,
        "cache_creation_per_mtok_usd": 18.75,
        "output_per_mtok_usd": 75.0,
    },
}


def _int_value(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _pricing_catalog() -> dict[str, dict[str, Any]]:
    catalog = {key: dict(value) for key, value in DEFAULT_LLM_PRICING.items()}
    raw = (os.environ.get("FACTOR_LAB_LLM_PRICING_JSON") or "").strip()
    if not raw:
        return catalog
    try:
        override = json.loads(raw)
    except Exception:
        return catalog
    if isinstance(override, dict):
        for key, value in override.items():
            if isinstance(value, dict):
                merged = dict(catalog.get(str(key), {}))
                merged.update(value)
                merged.setdefault("pricing_family", str(key))
                catalog[str(key)] = merged
    return catalog


def pricing_for_model(model: str | None) -> dict[str, Any]:
    normalized = str(model or "").strip().lower()
    catalog = _pricing_catalog()
    for key, pricing in catalog.items():
        patterns = pricing.get("match") or [key]
        if any(str(pattern).lower() in normalized for pattern in patterns):
            return dict(pricing)
    return {
        "pricing_family": "unknown",
        "input_per_mtok_usd": 0.0,
        "cached_input_per_mtok_usd": 0.0,
        "cache_creation_per_mtok_usd": 0.0,
        "output_per_mtok_usd": 0.0,
    }


def estimate_llm_cost_usd(model: str | None, usage: dict[str, Any] | None) -> dict[str, Any]:
    usage = usage or {}
    pricing = pricing_for_model(model)
    prompt_tokens = _int_value(usage.get("prompt_tokens"))
    cached_tokens = _int_value(usage.get("cached_tokens"))
    cache_creation_tokens = _int_value(usage.get("cache_creation_tokens"))
    completion_tokens = _int_value(usage.get("completion_tokens"))

    # OpenAI-style usage includes cached tokens inside prompt_tokens; Anthropic-style usage usually exposes
    # cache creation/read separately. If cache_creation_tokens is present, treat prompt_tokens as regular input.
    if cache_creation_tokens > 0:
        uncached_input_tokens = prompt_tokens
    else:
        uncached_input_tokens = max(prompt_tokens - cached_tokens, 0)

    input_cost = uncached_input_tokens * float(pricing.get("input_per_mtok_usd") or 0.0) / 1_000_000
    cached_cost = cached_tokens * float(pricing.get("cached_input_per_mtok_usd") or 0.0) / 1_000_000
    cache_creation_cost = cache_creation_tokens * float(pricing.get("cache_creation_per_mtok_usd") or 0.0) / 1_000_000
    output_cost = completion_tokens * float(pricing.get("output_per_mtok_usd") or 0.0) / 1_000_000
    total_cost = input_cost + cached_cost + cache_creation_cost + output_cost

    return {
        "pricing_family": pricing.get("pricing_family") or "unknown",
        "input_per_mtok_usd": pricing.get("input_per_mtok_usd"),
        "cached_input_per_mtok_usd": pricing.get("cached_input_per_mtok_usd"),
        "cache_creation_per_mtok_usd": pricing.get("cache_creation_per_mtok_usd"),
        "output_per_mtok_usd": pricing.get("output_per_mtok_usd"),
        "uncached_input_tokens": uncached_input_tokens,
        "cached_input_tokens": cached_tokens,
        "cache_creation_tokens": cache_creation_tokens,
        "output_tokens": completion_tokens,
        "input_cost_usd": round(input_cost, 8),
        "cached_input_cost_usd": round(cached_cost, 8),
        "cache_creation_cost_usd": round(cache_creation_cost, 8),
        "output_cost_usd": round(output_cost, 8),
        "estimated_cost_usd": round(total_cost, 8),
    }
