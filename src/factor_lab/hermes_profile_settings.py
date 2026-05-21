from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class HermesProfileSetting:
    name: str
    display_name: str
    enabled: bool
    decision_types: list[str]
    purpose: str
    system_prompt: str
    llm_fallback_order: list[str]
    timeout_seconds: int
    max_retries: int
    strict_schema: bool
    legacy_agent_id: str | None = None


def _split_csv(raw: str | None) -> list[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _default_fallback_order() -> list[str]:
    return _split_csv(os.environ.get("FACTOR_LAB_LLM_FALLBACK_ORDER", ""))


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on", "enabled"}:
            return True
        if lowered in {"0", "false", "no", "off", "disabled"}:
            return False
    if value is None:
        return default
    return bool(value)


def _coerce_int(value: Any, default: int, *, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, parsed)


def _coerce_str_list(value: Any, default: list[str] | None = None) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return _split_csv(value)
    return list(default or [])


def default_hermes_profile_settings() -> list[HermesProfileSetting]:
    order = _default_fallback_order()
    return [
        HermesProfileSetting(
            name="planner",
            display_name="规划 Agent",
            enabled=True,
            decision_types=["planner"],
            purpose="生成下一轮研究计划、任务排序、候选因子扩展建议。",
            system_prompt="你是 Factor Lab 的规划 agent。必须基于输入 snapshot，不得编造指标。输出必须符合 planner schema。",
            llm_fallback_order=order,
            timeout_seconds=90,
            max_retries=1,
            strict_schema=True,
            legacy_agent_id=os.environ.get("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner"),
        ),
        HermesProfileSetting(
            name="diagnostician",
            display_name="失败诊断 Agent",
            enabled=True,
            decision_types=["diagnostician"],
            purpose="分析失败 run、失败 candidate、数据/LLM/配置错误，并给出恢复建议。",
            system_prompt="你是 Factor Lab 的失败诊断 agent。优先定位根因：数据、配置、模型、schema、回测、候选质量。输出必须符合 diagnostician schema。",
            llm_fallback_order=order,
            timeout_seconds=90,
            max_retries=1,
            strict_schema=True,
            legacy_agent_id=os.environ.get("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure"),
        ),
        HermesProfileSetting(
            name="reviewer",
            display_name="质量复核 Agent",
            enabled=True,
            decision_types=["reviewer"],
            purpose="复核候选因子质量，识别重复、过拟合、弱增量、稳健性不足，并给出继续验证/降权/归档建议。",
            system_prompt="你是 Factor Lab 的质量复核 agent。必须基于候选因子、评分、相关性、稳健性、approved universe 与失败档案判断质量，不得编造指标。输出必须符合 reviewer schema。",
            llm_fallback_order=order,
            timeout_seconds=90,
            max_retries=1,
            strict_schema=True,
            legacy_agent_id=None,
        ),
        HermesProfileSetting(
            name="data_steward",
            display_name="数据质量 Agent",
            enabled=True,
            decision_types=["data_steward"],
            purpose="诊断数据源、样本覆盖、字段缺失、日期区间、Tushare 路由与缓存状态，区分数据问题和策略问题。",
            system_prompt="你是 Factor Lab 的数据质量 agent。优先定位数据链路根因：token、权限、网络、空数据、字段缺失、coverage、缓存陈旧、日期范围。输出必须符合 data_steward schema。",
            llm_fallback_order=order,
            timeout_seconds=90,
            max_retries=1,
            strict_schema=True,
            legacy_agent_id=None,
        ),
    ]


def _default_role_by_name(name: str) -> HermesProfileSetting | None:
    for role in default_hermes_profile_settings():
        if role.name == name:
            return role
    return None


def _coerce_role(payload: dict[str, Any]) -> HermesProfileSetting:
    raw_name = str(payload.get("name") or "").strip()
    name = raw_name or "agent"
    default = _default_role_by_name(name)
    decision_types_default = default.decision_types if default else [name]
    return HermesProfileSetting(
        name=name,
        display_name=str(payload.get("display_name") or (default.display_name if default else name)).strip(),
        enabled=_coerce_bool(payload.get("enabled"), default.enabled if default else True),
        decision_types=_coerce_str_list(payload.get("decision_types"), decision_types_default),
        purpose=str(payload.get("purpose") or (default.purpose if default else "")).strip(),
        system_prompt=str(payload.get("system_prompt") or (default.system_prompt if default else "")).strip(),
        llm_fallback_order=_coerce_str_list(
            payload.get("llm_fallback_order"),
            default.llm_fallback_order if default else _default_fallback_order(),
        ),
        timeout_seconds=_coerce_int(payload.get("timeout_seconds"), default.timeout_seconds if default else 90, minimum=1),
        max_retries=_coerce_int(payload.get("max_retries"), default.max_retries if default else 1, minimum=0),
        strict_schema=_coerce_bool(payload.get("strict_schema"), default.strict_schema if default else True),
        legacy_agent_id=(
            str(payload.get("legacy_agent_id")).strip()
            if payload.get("legacy_agent_id") is not None and str(payload.get("legacy_agent_id")).strip()
            else (default.legacy_agent_id if default else None)
        ),
    )


def load_hermes_profile_settings() -> list[HermesProfileSetting]:
    raw = os.environ.get("FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON", "").strip()
    if not raw:
        return default_hermes_profile_settings()
    try:
        payload = json.loads(raw)
    except Exception:
        return default_hermes_profile_settings()
    if not isinstance(payload, list):
        return default_hermes_profile_settings()
    roles = [_coerce_role(item) for item in payload if isinstance(item, dict)]
    return roles or default_hermes_profile_settings()


def select_hermes_profile_setting(decision_type: str) -> HermesProfileSetting | None:
    normalized = str(decision_type or "").strip()
    for role in load_hermes_profile_settings():
        if role.enabled and normalized in role.decision_types:
            return role
    return None


def hermes_profile_settings_to_json(roles: list[HermesProfileSetting]) -> str:
    return json.dumps([asdict(role) for role in roles], ensure_ascii=False, separators=(",", ":"))
