from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class EndpointProbeResult:
    provider: str
    source_id: str
    endpoint: str
    success: bool
    rows: int
    columns: tuple[str, ...]
    date_fields: tuple[str, ...]
    error: str | None = None
    sample: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "source_id": self.source_id,
            "endpoint": self.endpoint,
            "success": self.success,
            "rows": self.rows,
            "columns": list(self.columns),
            "date_fields": list(self.date_fields),
            "error": self.error,
            "sample": list(self.sample),
        }


@dataclass(frozen=True)
class SourceCandidateDecision:
    source_id: str
    display_name: str
    provider: str
    recommendation: str
    score: int
    pit_control: str
    frequency_hint: str
    mechanism_fit: str
    blockers: tuple[str, ...]
    best_endpoint: EndpointProbeResult | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "display_name": self.display_name,
            "provider": self.provider,
            "recommendation": self.recommendation,
            "score": self.score,
            "pit_control": self.pit_control,
            "frequency_hint": self.frequency_hint,
            "mechanism_fit": self.mechanism_fit,
            "blockers": list(self.blockers),
            "best_endpoint": self.best_endpoint.to_dict() if self.best_endpoint else None,
        }


@dataclass(frozen=True)
class CandidateSpec:
    source_id: str
    display_name: str
    provider: str
    endpoints: tuple[str, ...]
    mechanism_fit: str
    frequency_hint: str
    preferred_for_low_crowding: bool = False


TUSHARE_CANDIDATES: tuple[CandidateSpec, ...] = (
    CandidateSpec("margin_financing", "融资融券", "tushare", ("margin_detail", "margin"), "crowding/leverage/sentiment", "daily", True),
    CandidateSpec("shareholder_count", "股东户数", "tushare", ("stk_holdernumber",), "ownership concentration / crowding", "quarterly_or_event", True),
    CandidateSpec("institutional_holding", "机构持仓", "tushare", ("top10_holders", "top10_floatholders"), "institutional ownership / crowding", "quarterly", True),
    CandidateSpec("earnings_preannouncement", "业绩预告", "tushare", ("forecast",), "earnings revision/event", "event"),
    CandidateSpec("earnings_express", "业绩快报", "tushare", ("express",), "earnings revision/event", "event"),
    CandidateSpec("dragon_tiger", "龙虎榜", "tushare", ("top_list",), "hot money flow/event", "daily_event"),
    CandidateSpec("block_trade", "大宗交易", "tushare", ("block_trade",), "large trade flow/event", "daily_event"),
    CandidateSpec("buyback", "回购", "tushare", ("repurchase",), "capital action/governance", "event"),
    CandidateSpec("pledge", "质押", "tushare", ("pledge_stat", "pledge_detail"), "risk/governance", "event_or_periodic"),
    CandidateSpec("insider_trade", "高管/股东增减持", "tushare", ("stk_holdertrade",), "insider behavior/governance", "event"),
    CandidateSpec("analyst_forecast", "分析师预期", "tushare", ("report_rc",), "expectation revision", "event_or_periodic"),
)

DIEMENG_CANDIDATES: tuple[CandidateSpec, ...] = tuple(
    CandidateSpec(c.source_id, c.display_name, "diemeng", tuple(f"/stock/{e}" for e in c.endpoints), c.mechanism_fit, c.frequency_hint, c.preferred_for_low_crowding)
    for c in TUSHARE_CANDIDATES
)

DATE_FIELD_CANDIDATES = (
    "trade_date",
    "ann_date",
    "f_ann_date",
    "end_date",
    "period",
    "report_date",
    "list_date",
    "start_date",
    "enddate",
)


def safe_records_from_frame(df: Any, limit: int = 3) -> tuple[dict[str, Any], ...]:
    if df is None or not hasattr(df, "head"):
        return ()
    try:
        records = df.head(limit).to_dict(orient="records")
    except Exception:
        return ()
    out: list[dict[str, Any]] = []
    for rec in records:
        clean: dict[str, Any] = {}
        for k, v in rec.items():
            if hasattr(v, "item"):
                try:
                    v = v.item()
                except Exception:
                    pass
            clean[str(k)] = None if str(v) == "nan" else v
        out.append(clean)
    return tuple(out)


def detect_date_fields(columns: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    colset = {str(c) for c in columns}
    return tuple(c for c in DATE_FIELD_CANDIDATES if c in colset)


def classify_pit_control(date_fields: tuple[str, ...]) -> str:
    fields = set(date_fields)
    if "trade_date" in fields:
        return "trade_date_observable"
    if "ann_date" in fields or "f_ann_date" in fields:
        return "announcement_date_pit"
    if "end_date" in fields and not ({"ann_date", "f_ann_date"} & fields):
        return "end_date_only_not_pit_safe"
    if date_fields:
        return "date_available_needs_review"
    return "no_date_field_found"


def choose_recommendation(score: int, blockers: list[str]) -> str:
    if "no_successful_endpoint" in blockers:
        return "blocked_no_access_or_no_rows"
    if "not_pit_safe" in blockers:
        return "manual_review_before_research"
    if score >= 80:
        return "mvp_candidate"
    if score >= 60:
        return "secondary_candidate"
    return "monitor_or_later"


def score_candidate(spec: CandidateSpec, best: EndpointProbeResult | None) -> SourceCandidateDecision:
    blockers: list[str] = []
    score = 0
    pit_control = "unknown"
    if best is None or not best.success or best.rows <= 0:
        blockers.append("no_successful_endpoint")
    else:
        score += 25
        score += min(25, best.rows)
        pit_control = classify_pit_control(best.date_fields)
        if pit_control in {"trade_date_observable", "announcement_date_pit"}:
            score += 25
        else:
            blockers.append("not_pit_safe")
        if spec.preferred_for_low_crowding:
            score += 15
        if spec.frequency_hint in {"daily", "daily_event"}:
            score += 10
    recommendation = choose_recommendation(score, blockers)
    return SourceCandidateDecision(
        source_id=spec.source_id,
        display_name=spec.display_name,
        provider=spec.provider,
        recommendation=recommendation,
        score=score,
        pit_control=pit_control,
        frequency_hint=spec.frequency_hint,
        mechanism_fit=spec.mechanism_fit,
        blockers=tuple(blockers),
        best_endpoint=best,
    )


def build_final_decision(decisions: list[SourceCandidateDecision]) -> dict[str, Any]:
    margin = [d for d in decisions if d.source_id == "margin_financing" and d.recommendation == "mvp_candidate"]
    if margin:
        best_margin = sorted(margin, key=lambda d: d.score, reverse=True)[0]
        return {
            "decision": "proceed_margin_mvp",
            "reason": "融资融券接口可用，且具备可审计日期字段，适合支撑 low-crowding 机制 MVP。",
            "selected_source": best_margin.to_dict(),
        }
    mvp = [d for d in decisions if d.recommendation == "mvp_candidate"]
    if mvp:
        best = sorted(mvp, key=lambda d: d.score, reverse=True)[0]
        return {
            "decision": "proceed_other_source_mvp",
            "reason": "融资融券未成为 MVP，但其他新信息源更适合下一轮接入。",
            "selected_source": best.to_dict(),
        }
    if any(d.best_endpoint is not None and d.best_endpoint.success for d in decisions):
        return {
            "decision": "source_requires_manual_review",
            "reason": "存在可访问源，但日期/PIT/样本质量不足，不能直接进入因子研究。",
            "selected_source": None,
        }
    return {
        "decision": "source_access_blocked",
        "reason": "候选新增数据源未取得成功样本；需要检查接口名、权限或 provider 文档。",
        "selected_source": None,
    }


def rank_decisions(decisions: list[SourceCandidateDecision]) -> list[SourceCandidateDecision]:
    return sorted(decisions, key=lambda d: (d.recommendation == "mvp_candidate", d.score), reverse=True)


TushareCaller = Callable[[str], EndpointProbeResult]
DiemengCaller = Callable[[str], EndpointProbeResult]


def evaluate_specs(specs: tuple[CandidateSpec, ...], caller: Callable[[CandidateSpec, str], EndpointProbeResult]) -> list[SourceCandidateDecision]:
    decisions: list[SourceCandidateDecision] = []
    for spec in specs:
        probes = [caller(spec, endpoint) for endpoint in spec.endpoints]
        successes = [p for p in probes if p.success and p.rows > 0]
        best = sorted(successes, key=lambda p: p.rows, reverse=True)[0] if successes else (probes[0] if probes else None)
        decisions.append(score_candidate(spec, best))
    return decisions
