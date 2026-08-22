from __future__ import annotations

import sqlite3
import json
import hashlib
import os
import subprocess
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Asia/Shanghai")

from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

from factor_lab.candidate_graph import build_graph_artifacts, build_candidate_graph_context, candidate_clusters, family_rollup
from factor_lab.factor_candidates import summarize_candidate_status
from factor_lab.db_views import ensure_views
from factor_lab.ops import latest_task_states, trigger_script
from factor_lab.storage import ExperimentStore
from factor_lab.opportunity_diagnostics import build_opportunity_metrics, build_opportunity_review, build_opportunity_archive_diagnostics
from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.candidate_failure_dossier import build_candidate_failure_dossier
from factor_lab.novelty_judge import load_novelty_judgments
from factor_lab.allocator_governance_auditor import load_allocator_governance_audit
from factor_lab.decision_ab_judge import load_decision_ab_artifacts
from factor_lab.diagnostician_enhancement import load_diagnostician_enhancement
from factor_lab.paths import env_file
from factor_lab.llm_pricing import estimate_llm_cost_usd
from factor_lab.research_quality_summary import build_research_quality_summary
from factor_lab.harvest_report import build_harvest_report
from factor_lab.autonomous_strategy_lab_report import build_autonomous_strategy_lab_report
from factor_lab.webui.app import create_app, register_startup_cache, render_template
from factor_lab.webui.routes.settings_data_sources import register_data_source_routes
from factor_lab.webui.routes.settings_llm import register_llm_settings_routes
from factor_lab.webui.services.service_restart import restart_research_daemon_after_settings_save as _restart_research_daemon_after_settings_save
from factor_lab.webui.services.research_os_read_model import (
    load_research_os_read_model as _load_research_os_read_model,
)
from factor_lab.webui.services.env_settings import (
    DATA_SOURCE_ENV_KEYS,
    DATA_SOURCE_TYPE_OPTIONS,
    LLM_ENV_KEYS,
    LLM_PROFILE_ENV_KEYS,
    LLM_API_FORMAT_OPTIONS,
    profiles_from_form as _profiles_from_form,
    data_source_profiles_from_form as _data_source_profiles_from_form,
    redacted_data_source_profile as _redacted_data_source_profile,
    load_data_source_settings as _service_load_data_source_settings,
    save_data_source_settings as _service_save_data_source_settings,
    test_data_source_connection as _service_test_data_source_connection,
    load_llm_settings as _service_load_llm_settings,
    save_llm_settings as _service_save_llm_settings,
    test_llm_profile_connection as _service_test_llm_profile_connection,
    read_env_values as _service_read_env_values,
)


def pretty_json_text(value: Any, empty_text: str = "暂无数据。") -> str:
    if value in (None, "", [], {}):
        return empty_text
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def _read_env_values(path: Path | None = None) -> dict[str, str]:
    return _service_read_env_values(env_file_func=env_file, path=path)


def load_data_source_settings() -> dict[str, Any]:
    return _service_load_data_source_settings(env_file_func=env_file, environ=os.environ)


def save_data_source_settings(form: dict[str, str]) -> dict[str, Any]:
    return _service_save_data_source_settings(form, env_file_func=env_file, environ=os.environ)


def test_data_source_connection(profile: dict[str, Any]) -> dict[str, Any]:
    return _service_test_data_source_connection(profile, environ=os.environ)


def load_llm_settings() -> dict[str, Any]:
    return _service_load_llm_settings(env_file_func=env_file, environ=os.environ)


def save_llm_settings(form: dict[str, str]) -> dict[str, Any]:
    return _service_save_llm_settings(
        form,
        env_file_func=env_file,
        environ=os.environ,
    )


def restart_research_daemon_after_settings_save() -> dict[str, Any]:
    return _restart_research_daemon_after_settings_save(subprocess_run=subprocess.run)


def test_llm_profile_connection(profile: dict[str, Any]) -> dict[str, Any]:
    return _service_test_llm_profile_connection(profile)


def format_bj_time(value: str | None) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return value
        bj = dt.astimezone(LOCAL_TZ)
        return bj.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return value


def localize_times(value: Any) -> Any:
    if isinstance(value, list):
        return [localize_times(v) for v in value]
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if isinstance(v, str) and (k.endswith("_utc") or k.endswith("_at_utc") or k in {"created_at_utc", "started_at_utc", "finished_at_utc", "updated_at_utc", "recorded_at_utc"}):
                out[k] = format_bj_time(v)
            else:
                out[k] = localize_times(v)
        return out
    return value


def portfolio_positions(current: dict[str, Any]) -> list[dict[str, Any]]:
    positions = current.get("positions") or []
    return sorted(positions, key=lambda row: row.get("weight", 0), reverse=True)


def _tail_lines(path: Path, max_lines: int, chunk_size: int = 8192) -> list[str]:
    if max_lines <= 0 or not path.exists():
        return []
    with path.open("rb") as handle:
        handle.seek(0, 2)
        file_size = handle.tell()
        if file_size == 0:
            return []
        position = file_size
        buffer = b""
        line_count = 0
        while position > 0 and line_count <= max_lines:
            read_size = min(chunk_size, position)
            position -= read_size
            handle.seek(position)
            chunk = handle.read(read_size)
            buffer = chunk + buffer
            line_count = buffer.count(b"\n")
        lines = buffer.splitlines()
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
        return [line.decode("utf-8", errors="ignore") for line in lines]


def read_jsonl(path: Path, tail_lines: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    lines: list[str]
    if tail_lines is not None:
        lines = _tail_lines(path, tail_lines)
    else:
        lines = path.read_text(encoding="utf-8").splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def sparkline_svg(values: list[float | int], width: int = 240, height: int = 64, color: str = "#84a8ff") -> str:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return ""
    if len(clean) == 1:
        clean = clean * 2
    min_v = min(clean)
    max_v = max(clean)
    span = max(max_v - min_v, 1e-9)
    points = []
    for idx, value in enumerate(clean):
        x = idx * (width - 8) / max(len(clean) - 1, 1) + 4
        y = height - 4 - ((value - min_v) / span) * (height - 8)
        points.append(f"{x:.1f},{y:.1f}")
    polyline = " ".join(points)
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">'
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="10" fill="#1b2444" />'
        f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{polyline}" />'
        f'</svg>'
    )


def score_timeline_svg(series: list[dict[str, Any]], width: int = 960, height: int = 320, target_lines: list[dict[str, Any]] | None = None, point_label_step: int | None = None) -> str:
    if not series:
        return ""
    all_points = [point for item in series for point in item.get('points', []) if point.get('ts') and point.get('score') is not None]
    if not all_points:
        return ""
    parsed = []
    for point in all_points:
        try:
            parsed.append((datetime.fromisoformat(point['ts']), float(point['score'])))
        except Exception:
            continue
    if not parsed:
        return ""
    min_ts = min(ts for ts, _ in parsed)
    max_ts = max(ts for ts, _ in parsed)
    if min_ts == max_ts:
        max_ts = min_ts + timedelta(minutes=1)

    pad_left, pad_right, pad_top, pad_bottom = 56, 18, 18, 36
    inner_w = width - pad_left - pad_right
    inner_h = height - pad_top - pad_bottom
    total_seconds = max((max_ts - min_ts).total_seconds(), 1)

    def x_pos(ts: datetime) -> float:
        return pad_left + ((ts - min_ts).total_seconds() / total_seconds) * inner_w

    def y_pos(score: float) -> float:
        bounded = max(0.0, min(100.0, float(score)))
        return pad_top + ((100.0 - bounded) / 100.0) * inner_h

    bands = [
        (70, 100, '#163a2d', '强'),
        (40, 70, '#40351a', '中'),
        (0, 40, '#3d1f28', '弱'),
    ]
    band_svg = []
    for low, high, fill, label in bands:
        y1 = y_pos(high)
        y2 = y_pos(low)
        band_svg.append(f'<rect x="{pad_left}" y="{y1:.1f}" width="{inner_w:.1f}" height="{(y2-y1):.1f}" fill="{fill}" opacity="0.45" />')
        band_svg.append(f'<text x="12" y="{((y1+y2)/2)+4:.1f}" fill="#9aa7d1" font-size="12">{label}</text>')

    grid_svg = []
    for score in [0, 20, 40, 60, 80, 100]:
        y = y_pos(score)
        grid_svg.append(f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width-pad_right}" y2="{y:.1f}" stroke="#2b3765" stroke-width="1" opacity="0.9" />')
        grid_svg.append(f'<text x="{pad_left-8}" y="{y+4:.1f}" text-anchor="end" fill="#9aa7d1" font-size="11">{score}</text>')

    target_svg = []
    for idx, target in enumerate(target_lines or []):
        try:
            score = float(target.get('score'))
        except Exception:
            continue
        y = y_pos(score)
        color = target.get('color', '#9aa7d1')
        label = target.get('label', f'target-{idx+1}')
        target_svg.append(f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width-pad_right}" y2="{y:.1f}" stroke="{color}" stroke-width="1.5" stroke-dasharray="6 6" opacity="0.95" />')
        target_svg.append(f'<text x="{width-pad_right-4}" y="{y-6:.1f}" text-anchor="end" fill="{color}" font-size="11">{label}</text>')

    tick_times = [min_ts, min_ts + (max_ts - min_ts) / 2, max_ts]
    tick_svg = []
    for ts in tick_times:
        x = x_pos(ts)
        label = ts.astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M')
        tick_svg.append(f'<line x1="{x:.1f}" y1="{pad_top}" x2="{x:.1f}" y2="{height-pad_bottom}" stroke="#2b3765" stroke-width="1" opacity="0.9" />')
        tick_svg.append(f'<text x="{x:.1f}" y="{height-10}" text-anchor="middle" fill="#9aa7d1" font-size="11">{label}</text>')

    legend_svg = []
    for idx, item in enumerate(series):
        x = pad_left + idx * 160
        color = item.get('color', '#84a8ff')
        name = item.get('name', f'series-{idx+1}')
        legend_svg.append(f'<line x1="{x}" y1="10" x2="{x+18}" y2="10" stroke="{color}" stroke-width="3" />')
        legend_svg.append(f'<text x="{x+24}" y="14" fill="#ebf0ff" font-size="12">{name}</text>')

    series_svg = []
    for item in series:
        color = item.get('color', '#84a8ff')
        points = []
        for point in item.get('points', []):
            try:
                ts = datetime.fromisoformat(point['ts'])
                score = float(point['score'])
            except Exception:
                continue
            points.append((x_pos(ts), y_pos(score), score))
        if not points:
            continue
        if len(points) == 1:
            x, y, score = points[0]
            series_svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" />')
            series_svg.append(f'<text x="{x+6:.1f}" y="{y-6:.1f}" fill="{color}" font-size="11">{score:.1f}</text>')
            continue
        polyline = ' '.join(f'{x:.1f},{y:.1f}' for x, y, _ in points)
        series_svg.append(f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{polyline}" />')
        for idx, (x, y, score) in enumerate(points):
            if idx >= len(points) - 4:
                series_svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{color}" />')
            if point_label_step and (idx % max(point_label_step, 1) == 0 or idx == len(points) - 1):
                series_svg.append(f'<text x="{x+6:.1f}" y="{y-6:.1f}" fill="{color}" font-size="11">{score:.1f}</text>')
        if not point_label_step:
            x, y, score = points[-1]
            series_svg.append(f'<text x="{x+6:.1f}" y="{y-6:.1f}" fill="{color}" font-size="11">{score:.1f}</text>')

    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">'
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="14" fill="#1b2444" />'
        + ''.join(band_svg)
        + ''.join(grid_svg)
        + ''.join(target_svg)
        + ''.join(tick_svg)
        + ''.join(series_svg)
        + ''.join(legend_svg)
        + '</svg>'
    )


def compute_weekly_report(health: dict[str, Any] | None = None) -> dict[str, Any]:
    health = health or get_cached_health_metrics()
    conn = get_conn()
    try:
        week_runs = [dict(row) for row in conn.execute(
            """
            SELECT run_id, created_at_utc, config_path, status, start_date, end_date, dataset_rows
            FROM workflow_runs
            WHERE created_at_utc >= datetime('now', '-7 day')
            ORDER BY created_at_utc DESC
            """
        ).fetchall()]
        recent_finished = [row for row in week_runs if row.get('status') == 'finished']

        # Unify "latest run" semantics across the UI: prefer the latest *finished* run.
        # Fallback to the latest run (any status) only if nothing finished in the window.
        finished_week_runs = [row for row in week_runs if row.get('status') == 'finished']
        latest_run_id = (finished_week_runs[0]['run_id'] if finished_week_runs else (week_runs[0]['run_id'] if week_runs else None))
        prev_run_id = (finished_week_runs[1]['run_id'] if len(finished_week_runs) > 1 else (week_runs[1]['run_id'] if len(week_runs) > 1 else None))

        def candidate_set(run_id: str | None) -> set[str]:
            if not run_id:
                return set()
            rows = conn.execute(
                "SELECT factor_name FROM factor_results WHERE run_id = ? AND variant = 'candidate' ORDER BY factor_name ASC",
                (run_id,),
            ).fetchall()
            return {row[0] for row in rows}

        latest_candidates = candidate_set(latest_run_id)
        prev_candidates = candidate_set(prev_run_id)
        entered = sorted(latest_candidates - prev_candidates)
        left = sorted(prev_candidates - latest_candidates)

        weekly_best_strategies = [dict(row) for row in conn.execute(
            """
            SELECT strategy_name, ROUND(AVG(sharpe), 6) AS avg_sharpe, ROUND(AVG(annual_return), 6) AS avg_return, COUNT(*) AS runs
            FROM portfolio_results
            WHERE run_id IN (
                SELECT run_id FROM workflow_runs WHERE created_at_utc >= datetime('now', '-7 day')
            )
            GROUP BY strategy_name
            ORDER BY avg_sharpe DESC
            LIMIT 5
            """
        ).fetchall()]

        heartbeat_rows = read_jsonl(DB_PATH.parent / 'system_heartbeat.jsonl', tail_lines=20)
        cycle_heartbeats = [row for row in heartbeat_rows if row.get('scope') == 'scheduled_cycle']
        llm_heartbeats = [row for row in heartbeat_rows if row.get('scope') == 'llm_cycle']

        return {
            'summary': {
                'runs_7d': len(week_runs),
                'finished_7d': len(recent_finished),
                'success_rate_7d': round(len(recent_finished) / len(week_runs), 4) if week_runs else None,
                'stable_candidate_count': health['research_progress']['stable_candidate_count'],
                'candidate_entered': entered,
                'candidate_left': left,
                'paper_stability_label': (health['portfolio_progress']['paper_stability'] or {}).get('label'),
                'recommendation_hit_rate': health['research_progress'].get('recommendation_hit_rate'),
                'cycle_heartbeats_7d': len(cycle_heartbeats),
                'llm_heartbeats_7d': len(llm_heartbeats),
            },
            'weekly_best_strategies': weekly_best_strategies,
            'week_runs': week_runs[:12],
            'heartbeat_rows': heartbeat_rows[::-1],
        }
    finally:
        conn.close()


def _read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


_PAYLOAD_CACHE: dict[str, tuple[float, Any]] = {}


def _cached_payload(key: str, ttl_seconds: float, builder) -> Any:
    now = time.time()
    cached = _PAYLOAD_CACHE.get(key)
    if cached and (now - cached[0]) <= ttl_seconds:
        return deepcopy(cached[1])
    value = builder()
    _PAYLOAD_CACHE[key] = (now, value)
    return deepcopy(value)


def get_cached_health_metrics() -> dict[str, Any]:
    return _cached_payload('health_metrics', 15.0, compute_health_metrics)


def get_cached_promotion_scorecard(limit: int) -> dict[str, Any]:
    return _cached_payload(
        f'promotion_scorecard:{limit}',
        30.0,
        lambda: build_promotion_scorecard(DB_PATH, limit=limit),
    )


def _decision_effective_source(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("decision_metadata") or {}
    return metadata.get("effective_source") or metadata.get("source") or payload.get("decision_source")


def _decision_layer_payload(base: Path) -> dict[str, Any]:
    live_health = _read_json_file(base / "llm_provider_health_live.json", {})
    observation_health = _read_json_file(base / "llm_provider_health.json", {})
    hermes_decision_artifacts = _read_json_file(base / "hermes_decision_artifacts.json", {})
    decision_impact = _read_json_file(base / "decision_impact_report.json", {})

    def normalize_provider_health(payload: dict[str, Any]) -> dict[str, Any]:
        probe = payload.get("probe") or {}
        effective_source = payload.get("effective_source") or payload.get("recommended_effective_source")
        return {
            "configured_provider": payload.get("configured_provider"),
            "effective_source": effective_source,
            "degraded_to_heuristic": bool(payload.get("degraded_to_heuristic")),
            "probe_attempted": bool(probe.get("attempted")),
            "probe_skipped": bool(probe.get("skipped")),
            "probe_ok": probe.get("ok"),
            "probe_latency_ms": probe.get("latency_ms"),
            "probe_error": probe.get("error"),
        }

    def normalize_agent(payload: dict[str, Any]) -> dict[str, Any]:
        metadata = payload.get("decision_metadata") or {}
        return {
            "effective_source": _decision_effective_source(payload),
            "configured_provider": metadata.get("configured_provider"),
            "degraded_to_heuristic": bool(metadata.get("degraded_to_heuristic")),
            "fallback_reason": metadata.get("fallback_reason"),
            "provider_latency_ms": metadata.get("provider_latency_ms") or metadata.get("latency_ms"),
            "session_mode": metadata.get("session_mode"),
            "session_id": metadata.get("session_id"),
            "request_scope_id": metadata.get("request_scope_id"),
        }

    planner = normalize_agent(hermes_decision_artifacts.get("planner") or {})
    diagnostician = normalize_agent(hermes_decision_artifacts.get("diagnostician") or {})
    fallback_reasons = [reason for reason in [planner.get("fallback_reason"), diagnostician.get("fallback_reason")] if reason]

    return {
        "live": normalize_provider_health(live_health),
        "observation": normalize_provider_health(observation_health),
        "planner": planner,
        "diagnostician": diagnostician,
        "degraded": bool(
            normalize_provider_health(live_health).get("degraded_to_heuristic")
            or normalize_provider_health(observation_health).get("degraded_to_heuristic")
            or planner.get("degraded_to_heuristic")
            or diagnostician.get("degraded_to_heuristic")
        ),
        "last_fallback_reason": fallback_reasons[0] if fallback_reasons else None,
        "decision_impact_changed": bool((decision_impact.get("planner") or {}).get("changed") or (decision_impact.get("diagnostician") or {}).get("changed")),
    }



def _repair_layer_payload(base: Path) -> dict[str, Any]:
    feedback = _read_json_file(base / "repair_feedback.json", {})
    metrics = _read_json_file(base / "repair_metrics.json", {})
    verification = _read_json_file(base / "repair_verification.json", {})
    response = _read_json_file(base / "repair_agent_response.json", {})
    action_plan = _read_json_file(base / "repair_action_plan.json", {})
    return {
        "feedback": feedback,
        "metrics": metrics,
        "verification": verification,
        "response": response,
        "action_plan": action_plan,
        "active_incident_count": int(feedback.get("active_incident_count") or 0),
        "route_unhealthy": bool(feedback.get("route_unhealthy")),
        "restart_recently": bool(feedback.get("restart_recently")),
        "top_incident_types_24h": feedback.get("top_incident_types_24h") or [],
    }


def _load_health_paper_stability(base: Path, snapshot: dict[str, Any]) -> tuple[dict[str, Any], str]:
    live_path = base / 'paper_portfolio' / 'portfolio_stability_score.json'
    live_payload = _read_json_file(live_path, {})
    if live_payload:
        return live_payload, 'live_portfolio_stability'
    snapshot_payload = (snapshot or {}).get('paper_portfolio_stability', {}) or {}
    if snapshot_payload:
        return snapshot_payload, 'snapshot_fallback'
    return {}, 'missing'


def _build_research_progress_display(snapshot_score: float, timeline_score: float, *, timeline_candidate_run_count: int, timeline_candidate_factor_count: int, timeline_point_count: int) -> dict[str, Any]:
    sparse_timeline = timeline_point_count > 0 and (timeline_candidate_run_count == 0 or timeline_candidate_factor_count == 0)
    if sparse_timeline:
        return {
            'score': round(snapshot_score, 1),
            'basis': 'snapshot',
            'warning': '24h 时间线里没有有效 candidate 轨迹，主卡片已回退到当前快照分。',
            'timeline_sparse': True,
        }
    return {
        'score': round(timeline_score, 1),
        'basis': 'timeline',
        'warning': None,
        'timeline_sparse': False,
    }


_PROGRESS_OUTCOME_WEIGHTS = {
    'high_value_success': 100.0,
    'useful_success': 85.0,
    'high_value_failure': 70.0,
    'ordinary_failure': 28.0,
    'low_value_repeat': 8.0,
    'execution_failure': 0.0,
}


_PROGRESS_FEEDBACK_WEIGHTS = {
    'high_value_success': 100.0,
    'useful_success': 88.0,
    'high_value_failure': 72.0,
    'ordinary_failure': 30.0,
    'low_value_repeat': 10.0,
    'execution_failure': 0.0,
}


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _project_bucket_start(dt: datetime, mode: str) -> datetime:
    local = dt.astimezone(ZoneInfo('Asia/Shanghai'))
    if mode == '24h':
        return local.replace(minute=0, second=0, microsecond=0)
    return local.replace(hour=0, minute=0, second=0, microsecond=0)


def _progress_time_buckets(now: datetime, mode: str) -> list[datetime]:
    current = _project_bucket_start(now, mode)
    if mode == '24h':
        return [current - timedelta(hours=offset) for offset in range(23, -1, -1)]
    return [current - timedelta(days=offset) for offset in range(6, -1, -1)]


def _average_weighted_score(rows: list[dict[str, Any]], weights: dict[str, float]) -> float:
    if not rows:
        return 0.0
    values = [float(weights.get(str(row.get('outcome_class') or ''), 20.0)) for row in rows]
    return round(sum(values) / len(values), 1)


def _normalize_best_raw_score(best_raw_score: float | None) -> float:
    if best_raw_score is None:
        return 0.0
    return round(min(max(float(best_raw_score), 0.0) / 1.5, 1.0) * 100.0, 1)


def _portfolio_edge_score(candidate_sharpe: float | None, candidate_return: float | None, all_return: float | None) -> float:
    score = 0.0
    if candidate_sharpe is not None:
        score += min(max(float(candidate_sharpe), 0.0) * 8.0, 55.0)
    if candidate_return is not None and all_return is not None:
        edge = float(candidate_return) - float(all_return)
        score += min(max(edge, 0.0) * 80.0, 25.0)
        if float(candidate_return) >= 0.6 * float(all_return):
            score += 20.0
    return round(min(score, 100.0), 1)


def _build_project_progress_observation(
    *,
    conn: sqlite3.Connection,
    base: Path,
    research_tasks: list[dict[str, Any]],
    candidates_only_recent_sharpe: float | None,
    candidates_only_recent_return: float | None,
    all_factors_recent_return: float | None,
) -> dict[str, Any]:
    memory = _read_json_file(base / 'research_memory.json', {})
    generated_tasks = [
        row for row in research_tasks
        if ((row.get('payload') or {}).get('source') == 'candidate_generation')
    ]
    generated_outcomes = list(memory.get('generated_candidate_outcomes') or [])
    execution_feedback = list(memory.get('execution_feedback') or [])

    run_quality_rows = [dict(row) for row in conn.execute(
        """
        SELECT wr.run_id, wr.created_at_utc,
               MAX(CASE WHEN fr.variant = 'raw_scored' THEN fr.score END) AS best_raw_score
        FROM workflow_runs wr
        LEFT JOIN factor_results fr ON fr.run_id = wr.run_id
        WHERE wr.created_at_utc >= datetime('now', '-7 day')
        GROUP BY wr.run_id, wr.created_at_utc
        ORDER BY wr.created_at_utc ASC
        """
    ).fetchall()]
    portfolio_rows = [dict(row) for row in conn.execute(
        """
        SELECT wr.created_at_utc, pr.strategy_name,
               AVG(pr.sharpe) AS avg_sharpe,
               AVG(pr.annual_return) AS avg_return
        FROM workflow_runs wr
        JOIN portfolio_results pr ON pr.run_id = wr.run_id
        WHERE wr.created_at_utc >= datetime('now', '-7 day')
          AND pr.strategy_name IN ('long_short_top_bottom_candidates_only', 'long_short_top_bottom_all_factors')
        GROUP BY wr.created_at_utc, pr.strategy_name
        ORDER BY wr.created_at_utc ASC
        """
    ).fetchall()]

    now = datetime.now(timezone.utc)
    windows: dict[str, Any] = {}
    for mode in ('24h', '7d'):
        bucket_starts = _progress_time_buckets(now, mode)
        bucket_index = {bucket.isoformat(): idx for idx, bucket in enumerate(bucket_starts)}
        bucket_rows = [
            {
                'bucket': bucket,
                'label': bucket.astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M' if mode == '24h' else '%m-%d'),
                'generated_tasks': [],
                'generated_outcomes': [],
                'execution_feedback': [],
                'best_raw_scores': [],
                'candidate_sharpes': [],
                'candidate_returns': [],
                'all_returns': [],
            }
            for bucket in bucket_starts
        ]

        for row in generated_tasks:
            dt = _parse_iso_utc(row.get('created_at_utc'))
            if not dt:
                continue
            key = _project_bucket_start(dt, mode).isoformat()
            idx = bucket_index.get(key)
            if idx is not None:
                bucket_rows[idx]['generated_tasks'].append(row)

        for row in generated_outcomes:
            dt = _parse_iso_utc(row.get('updated_at_utc'))
            if not dt:
                continue
            key = _project_bucket_start(dt, mode).isoformat()
            idx = bucket_index.get(key)
            if idx is not None:
                bucket_rows[idx]['generated_outcomes'].append(row)

        for row in execution_feedback:
            dt = _parse_iso_utc(row.get('updated_at_utc'))
            if not dt:
                continue
            key = _project_bucket_start(dt, mode).isoformat()
            idx = bucket_index.get(key)
            if idx is not None:
                bucket_rows[idx]['execution_feedback'].append(row)

        for row in run_quality_rows:
            dt = _parse_iso_utc(row.get('created_at_utc'))
            if not dt:
                continue
            key = _project_bucket_start(dt, mode).isoformat()
            idx = bucket_index.get(key)
            if idx is not None and row.get('best_raw_score') is not None:
                bucket_rows[idx]['best_raw_scores'].append(float(row.get('best_raw_score') or 0.0))

        for row in portfolio_rows:
            dt = _parse_iso_utc(row.get('created_at_utc'))
            if not dt:
                continue
            key = _project_bucket_start(dt, mode).isoformat()
            idx = bucket_index.get(key)
            if idx is None:
                continue
            if row.get('strategy_name') == 'long_short_top_bottom_candidates_only':
                if row.get('avg_sharpe') is not None:
                    bucket_rows[idx]['candidate_sharpes'].append(float(row.get('avg_sharpe') or 0.0))
                if row.get('avg_return') is not None:
                    bucket_rows[idx]['candidate_returns'].append(float(row.get('avg_return') or 0.0))
            elif row.get('strategy_name') == 'long_short_top_bottom_all_factors' and row.get('avg_return') is not None:
                bucket_rows[idx]['all_returns'].append(float(row.get('avg_return') or 0.0))

        points = []
        for row in bucket_rows:
            outcome_score = _average_weighted_score(row['generated_outcomes'], _PROGRESS_OUTCOME_WEIGHTS)
            feedback_score = _average_weighted_score(row['execution_feedback'], _PROGRESS_FEEDBACK_WEIGHTS)
            best_raw_avg = round(sum(row['best_raw_scores']) / len(row['best_raw_scores']), 6) if row['best_raw_scores'] else None
            factor_discovery_score = round(
                min(100.0, outcome_score * 0.7 + _normalize_best_raw_score(best_raw_avg) * 0.3),
                1,
            ) if row['generated_outcomes'] or best_raw_avg is not None else 0.0
            candidate_sharpe_avg = round(sum(row['candidate_sharpes']) / len(row['candidate_sharpes']), 6) if row['candidate_sharpes'] else None
            candidate_return_avg = round(sum(row['candidate_returns']) / len(row['candidate_returns']), 6) if row['candidate_returns'] else None
            all_return_avg = round(sum(row['all_returns']) / len(row['all_returns']), 6) if row['all_returns'] else None
            portfolio_score = _portfolio_edge_score(candidate_sharpe_avg, candidate_return_avg, all_return_avg)
            total_score = round(factor_discovery_score * 0.4 + feedback_score * 0.35 + portfolio_score * 0.25, 1)
            row['factor_discovery_score'] = factor_discovery_score
            row['research_gain_score'] = feedback_score
            row['portfolio_edge_score'] = portfolio_score
            row['total_score'] = total_score
            row['best_raw_avg'] = best_raw_avg
            row['candidate_sharpe_avg'] = candidate_sharpe_avg
            row['candidate_return_avg'] = candidate_return_avg
            row['all_return_avg'] = all_return_avg
            points.append({'ts': row['bucket'].isoformat(), 'score': total_score})

        chart_svg = score_timeline_svg(
            [{'name': '项目进步总分', 'color': '#ffd166', 'points': points}],
            width=1100,
            height=320,
            point_label_step=3 if mode == '24h' else 1,
        )
        windows[mode] = {
            'mode': mode,
            'aggregation_label': '按小时均值' if mode == '24h' else '按天均值',
            'chart_svg': chart_svg,
            'rows': bucket_rows,
            'point_count': len(points),
        }

    cutoff_24h = now - timedelta(hours=24)
    outcomes_24h = [row for row in generated_outcomes if (_parse_iso_utc(row.get('updated_at_utc')) or now) >= cutoff_24h]
    feedback_24h = [row for row in execution_feedback if (_parse_iso_utc(row.get('updated_at_utc')) or now) >= cutoff_24h]
    tasks_24h = [row for row in generated_tasks if (_parse_iso_utc(row.get('created_at_utc')) or now) >= cutoff_24h]
    positive_outcomes_24h = [row for row in outcomes_24h if row.get('outcome_class') in {'high_value_success', 'useful_success'}]
    informative_outcomes_24h = [row for row in outcomes_24h if row.get('outcome_class') == 'high_value_failure']
    low_value_outcomes_24h = [row for row in outcomes_24h if row.get('outcome_class') == 'low_value_repeat']
    high_info_ratio_24h = round((len(positive_outcomes_24h) + len(informative_outcomes_24h)) / len(outcomes_24h), 4) if outcomes_24h else None

    return {
        'windows': windows,
        'current_24h': {
            'generated_task_count': len(tasks_24h),
            'generated_outcome_count': len(outcomes_24h),
            'positive_outcome_count': len(positive_outcomes_24h),
            'informative_outcome_count': len(informative_outcomes_24h),
            'low_value_repeat_count': len(low_value_outcomes_24h),
            'high_info_ratio': high_info_ratio_24h,
            'feedback_count': len(feedback_24h),
            'candidate_sharpe': candidates_only_recent_sharpe,
            'return_edge': round((float(candidates_only_recent_return or 0.0) - float(all_factors_recent_return or 0.0)), 6) if candidates_only_recent_return is not None and all_factors_recent_return is not None else None,
        },
        'current_scores': {
            'factor_discovery': windows['24h']['rows'][-1]['factor_discovery_score'] if windows['24h']['rows'] else 0.0,
            'research_gain': windows['24h']['rows'][-1]['research_gain_score'] if windows['24h']['rows'] else 0.0,
            'portfolio_edge': windows['24h']['rows'][-1]['portfolio_edge_score'] if windows['24h']['rows'] else 0.0,
            'overall': windows['24h']['rows'][-1]['total_score'] if windows['24h']['rows'] else 0.0,
        },
    }


def compute_health_metrics() -> dict[str, Any]:
    conn = get_conn()
    try:
        runs = [dict(row) for row in conn.execute(
            """
            SELECT run_id, created_at_utc, config_path, start_date, end_date,
                   status, factor_count, dataset_rows
            FROM workflow_runs
            ORDER BY created_at_utc DESC
            LIMIT 30
            """
        ).fetchall()]
        # Unify "latest run" semantics across the UI: prefer the latest *finished* run.
        latest_run = next((row for row in runs if row.get('status') == 'finished'), None) if runs else None
        recent_24h_total = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-1 day')"
        ).fetchone()[0]
        recent_24h_finished = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-1 day') AND status = 'finished'"
        ).fetchone()[0]
        recent_7d_total = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-7 day')"
        ).fetchone()[0]
        recent_7d_finished = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= datetime('now', '-7 day') AND status = 'finished'"
        ).fetchone()[0]

        candidate_rows = [dict(row) for row in conn.execute(
            """
            SELECT run_id, factor_name
            FROM factor_results
            WHERE variant = 'candidate'
            ORDER BY run_id ASC, factor_name ASC
            """
        ).fetchall()]
        candidate_by_run: dict[str, list[str]] = {}
        for row in candidate_rows:
            candidate_by_run.setdefault(row['run_id'], []).append(row['factor_name'])

        candidate_runs = [r for r in runs if r['run_id'] in candidate_by_run]
        latest_candidates = candidate_by_run.get(latest_run['run_id'], []) if latest_run else []
        previous_finished_run = None
        if latest_run:
            previous_finished_run = next((row for row in runs if row.get('status') == 'finished' and row.get('run_id') != latest_run.get('run_id')), None)
        previous_candidates = candidate_by_run.get((previous_finished_run or {}).get('run_id'), []) if previous_finished_run else []
        stable_candidate_count = conn.execute(
            "SELECT COUNT(*) FROM v_stable_candidates WHERE candidate_runs >= 2"
        ).fetchone()[0]

        recent_candidate_counts = [len(candidate_by_run.get(r['run_id'], [])) for r in runs[:7]]
        candidate_churn = 0
        if latest_candidates or previous_candidates:
            latest_set = set(latest_candidates)
            previous_set = set(previous_candidates)
            candidate_churn = len(latest_set.symmetric_difference(previous_set))

        strategy_rows = [dict(row) for row in conn.execute(
            """
            WITH ranked_portfolio AS (
                SELECT
                    w.created_at_utc,
                    p.run_id,
                    p.strategy_name,
                    p.sharpe,
                    p.annual_return,
                    p.max_drawdown,
                    p.avg_turnover,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.strategy_name
                        ORDER BY w.created_at_utc DESC
                    ) AS rn
                FROM portfolio_results p
                JOIN workflow_runs w ON w.run_id = p.run_id
            )
            SELECT created_at_utc, run_id, strategy_name, sharpe, annual_return, max_drawdown, avg_turnover
            FROM ranked_portfolio
            WHERE rn <= 12
            ORDER BY created_at_utc DESC
            """
        ).fetchall()]
        strategy_map: dict[str, list[dict[str, Any]]] = {}
        for row in strategy_rows:
            strategy_map.setdefault(row['strategy_name'], []).append(row)

        def avg_metric(strategy_name: str, field: str, limit: int = 5, as_of: str | None = None):
            rows = strategy_map.get(strategy_name, [])
            if as_of is not None:
                rows = [row for row in rows if (row.get('created_at_utc') or '') <= as_of]
            rows = rows[:limit]
            values = [row[field] for row in rows if row.get(field) is not None]
            return round(sum(values) / len(values), 6) if values else None

        candidates_only_recent_sharpe = avg_metric('long_short_top_bottom_candidates_only', 'sharpe')
        all_factors_recent_sharpe = avg_metric('long_short_top_bottom_all_factors', 'sharpe')
        candidates_only_recent_return = avg_metric('long_short_top_bottom_candidates_only', 'annual_return')
        all_factors_recent_return = avg_metric('long_short_top_bottom_all_factors', 'annual_return')

        factor_score_trend = [dict(row) for row in conn.execute(
            """
            SELECT factor_name, ROUND(AVG(score), 6) AS avg_score, COUNT(*) AS runs
            FROM factor_results
            WHERE variant = 'raw_scored'
            GROUP BY factor_name
            HAVING COUNT(*) >= 2
            ORDER BY avg_score DESC
            LIMIT 5
            """
        ).fetchall()]

        base = DB_PATH.parent
        llm_status_path = base / 'llm_status.json'
        snapshot_path = base / 'llm_input_snapshot.json'
        daemon_status_path = base / 'research_daemon_status.json'
        research_metrics_path = base / 'research_metrics.json'
        llm_status = _read_json_file(llm_status_path, {})
        snapshot = _read_json_file(snapshot_path, {})
        daemon_status = _read_json_file(daemon_status_path, {})
        research_metrics = _read_json_file(research_metrics_path, {})
        mode_labels = {
            'cpu_push': '冲 CPU',
            'target_zone': '目标区',
            'balanced': '平衡',
            'cpu_saturated': 'CPU 偏满',
            'memory_brake': '内存刹车',
            'memory_guard': '内存保护',
        }
        if daemon_status.get('mode'):
            daemon_status['mode_label'] = mode_labels.get(daemon_status.get('mode'), daemon_status.get('mode'))
        paper_stability, paper_stability_source = _load_health_paper_stability(base, snapshot)
        recommendation_history_tail = snapshot.get('recommendation_history_tail', []) or []
        positive_count = len([row for row in recommendation_history_tail if row.get('effectiveness') == 'positive'])
        recommendation_hit_rate = round(positive_count / len(recommendation_history_tail), 4) if recommendation_history_tail else None

        run_success_trend = []
        for row in runs[:12][::-1]:
            run_success_trend.append(1 if row.get('status') == 'finished' else 0)
        candidate_count_trend = [len(candidate_by_run.get(r['run_id'], [])) for r in runs[:12][::-1]]
        strategy_sharpe_trend = [
            row.get('sharpe') for row in strategy_map.get('long_short_top_bottom_candidates_only', [])[:12][::-1]
        ]

        timeline_runs = [dict(row) for row in conn.execute(
            """
            SELECT run_id, created_at_utc, status, dataset_rows
            FROM workflow_runs
            WHERE created_at_utc >= datetime('now', '-1 day')
            ORDER BY created_at_utc ASC
            """
        ).fetchall()]
        timeline_candidate_rows = [dict(row) for row in conn.execute(
            """
            SELECT fr.run_id, fr.factor_name, wr.created_at_utc
            FROM factor_results fr
            JOIN workflow_runs wr ON wr.run_id = fr.run_id
            WHERE fr.variant = 'candidate' AND wr.created_at_utc >= datetime('now', '-7 day')
            ORDER BY wr.created_at_utc ASC, fr.factor_name ASC
            """
        ).fetchall()]
        candidate_counts_by_ts: dict[str, dict[str, int]] = {}
        run_candidates_timeline: dict[str, list[str]] = {}
        for row in timeline_candidate_rows:
            ts = row['created_at_utc']
            factor_name = row['factor_name']
            candidate_counts_by_ts.setdefault(ts, {})
            candidate_counts_by_ts[ts][factor_name] = candidate_counts_by_ts[ts].get(factor_name, 0) + 1
            run_candidates_timeline.setdefault(row['run_id'], []).append(factor_name)

        stable_counter: dict[str, int] = {}
        stable_candidate_count_by_ts: dict[str, int] = {}
        for row in timeline_candidate_rows:
            ts = row['created_at_utc']
            factor_name = row['factor_name']
            stable_counter[factor_name] = stable_counter.get(factor_name, 0) + 1
            stable_candidate_count_by_ts[ts] = len([name for name, count in stable_counter.items() if count >= 2])

        timeline_series = {
            'run': [],
            'research': [],
            'portfolio': [],
        }
        timeline_candidate_run_count = 0
        timeline_candidate_factor_count = 0
        finished_seen = 0
        total_seen = 0
        prev_candidates_for_timeline: list[str] = []
        paper_stability_score = round(float(paper_stability.get('stability_score', 0) or 0) * 40, 1) if paper_stability else 0
        for row in timeline_runs:
            total_seen += 1
            if row.get('status') == 'finished':
                finished_seen += 1
            run_score = 0.0
            if total_seen:
                run_score += 60 * (finished_seen / total_seen)
            if row.get('status') == 'finished':
                run_score += 20
            if row.get('dataset_rows'):
                run_score += 20
            run_score = round(min(run_score, 100), 1)

            current_candidates = run_candidates_timeline.get(row['run_id'], [])
            current_stable = stable_candidate_count_by_ts.get(row['created_at_utc'], 0)
            if current_candidates:
                timeline_candidate_run_count += 1
                timeline_candidate_factor_count += len(current_candidates)
            candidate_churn_timeline = len(set(current_candidates).symmetric_difference(set(prev_candidates_for_timeline))) if (current_candidates or prev_candidates_for_timeline) else 0
            current_candidate_population = max(len(set(current_candidates)), len(set(prev_candidates_for_timeline)), 1)
            candidate_churn_ratio_timeline = min(candidate_churn_timeline / current_candidate_population, 1.0)
            research_score = 0.0
            research_score += min(current_stable * 7.0, 35.0)
            research_score += (1.0 - candidate_churn_ratio_timeline) * 20.0
            research_score += min(len(set(current_candidates)) / 5.0, 1.0) * 20.0
            research_score = round(min(research_score, 100), 1)
            if current_candidates:
                prev_candidates_for_timeline = current_candidates

            candidates_only_sharpe_at_ts = avg_metric('long_short_top_bottom_candidates_only', 'sharpe', as_of=row['created_at_utc'])
            candidates_only_return_at_ts = avg_metric('long_short_top_bottom_candidates_only', 'annual_return', as_of=row['created_at_utc'])
            all_factors_return_at_ts = avg_metric('long_short_top_bottom_all_factors', 'annual_return', as_of=row['created_at_utc'])
            portfolio_score = 0.0
            if candidates_only_sharpe_at_ts is not None:
                portfolio_score += min(max(candidates_only_sharpe_at_ts, 0) * 5, 40)
            portfolio_score += paper_stability_score
            if candidates_only_return_at_ts is not None and all_factors_return_at_ts is not None and candidates_only_return_at_ts >= 0.6 * all_factors_return_at_ts:
                portfolio_score += 20
            portfolio_score = round(min(portfolio_score, 100), 1)

            timeline_series['run'].append({'ts': row['created_at_utc'], 'score': run_score})
            timeline_series['research'].append({'ts': row['created_at_utc'], 'score': research_score})
            timeline_series['portfolio'].append({'ts': row['created_at_utc'], 'score': portfolio_score})

        timeline_chart = score_timeline_svg([
            {'name': '运行健康', 'color': '#3ddc97', 'points': timeline_series['run']},
            {'name': '研究进步', 'color': '#ffd166', 'points': timeline_series['research']},
            {'name': '组合进步', 'color': '#84a8ff', 'points': timeline_series['portfolio']},
        ])

        timeline_summary = {
            'window_label': '最近 24 小时',
            'point_count': len(timeline_runs),
            'run_delta': round((timeline_series['run'][-1]['score'] - timeline_series['run'][0]['score']), 1) if len(timeline_series['run']) >= 2 else 0,
            'research_delta': round((timeline_series['research'][-1]['score'] - timeline_series['research'][0]['score']), 1) if len(timeline_series['research']) >= 2 else 0,
            'portfolio_delta': round((timeline_series['portfolio'][-1]['score'] - timeline_series['portfolio'][0]['score']), 1) if len(timeline_series['portfolio']) >= 2 else 0,
            'run_current': timeline_series['run'][-1]['score'] if timeline_series['run'] else 0,
            'research_current': timeline_series['research'][-1]['score'] if timeline_series['research'] else 0,
            'portfolio_current': timeline_series['portfolio'][-1]['score'] if timeline_series['portfolio'] else 0,
        }

        heartbeat_rows = read_jsonl(DB_PATH.parent / 'system_heartbeat.jsonl', tail_lines=12)
        recent_heartbeat_rows = heartbeat_rows[-12:]

        daemon_status_history = read_jsonl(DB_PATH.parent / 'research_daemon_status_history.jsonl', tail_lines=240)
        recent_daemon_points = [row for row in daemon_status_history if row.get('updated_at_utc')][-240:]
        resource_timeline_series = {
            'cpu': [{'ts': row['updated_at_utc'], 'score': round(float(row.get('cpu_usage_ratio') or 0) * 100, 1)} for row in recent_daemon_points if row.get('cpu_usage_ratio') is not None],
            'memory': [],
            'throttle': [],
        }
        for row in recent_daemon_points:
            ts = row.get('updated_at_utc')
            mem_total = row.get('mem_total_mb') or 0
            mem_available = row.get('mem_available_mb') or 0
            if ts and mem_total:
                mem_used_ratio = max(0.0, min(1.0, 1.0 - (float(mem_available) / float(mem_total))))
                resource_timeline_series['memory'].append({'ts': ts, 'score': round(mem_used_ratio * 100, 1)})
            if ts and row.get('max_tasks_per_loop') is not None:
                throttle_score = min(float(row.get('max_tasks_per_loop') or 0) * 20.0 + float(row.get('batch_max_workers') or 0) * 10.0, 100.0)
                resource_timeline_series['throttle'].append({'ts': ts, 'score': round(throttle_score, 1)})

        resource_timeline_chart = score_timeline_svg([
            {'name': 'CPU 占用%', 'color': '#84a8ff', 'points': resource_timeline_series['cpu']},
            {'name': '内存占用%', 'color': '#ff6b6b', 'points': resource_timeline_series['memory']},
            {'name': '调度强度', 'color': '#3ddc97', 'points': resource_timeline_series['throttle']},
        ], target_lines=[{'score': 80, 'label': 'CPU 目标 80%', 'color': '#84a8ff'}])
        research_tasks = ExperimentStore(DB_PATH).list_research_tasks(limit=100)
        recent_finished_tasks = [t for t in research_tasks if t['status'] == 'finished']
        recent_failed_tasks = [t for t in research_tasks if t['status'] == 'failed']
        budget_guard_tasks = [
            t for t in research_tasks
            if 'generated_batch_budget_guard' in ((t.get('worker_note') or '') + ' ' + (t.get('last_error') or ''))
        ]
        blocked_family_heartbeats = [
            row for row in recent_heartbeat_rows
            if row.get('status') in {'circuit_open', 'circuit_open_family', 'idle_family_blocked'}
        ]
        project_progress = _build_project_progress_observation(
            conn=conn,
            base=base,
            research_tasks=research_tasks,
            candidates_only_recent_sharpe=candidates_only_recent_sharpe,
            candidates_only_recent_return=candidates_only_recent_return,
            all_factors_recent_return=all_factors_recent_return,
        )
        decision_layer = _decision_layer_payload(base)
        repair_layer = _repair_layer_payload(base)
        knowledge_gain_counter = {
            'stable_candidate_confirmed': 0,
            'repeated_graveyard_confirmed': 0,
            'neutralization_diagnosis_requested': 0,
            'exploration_candidate_survived': 0,
            'exploration_graveyard_identified': 0,
            'no_significant_information_gain': 0,
        }
        for task in research_tasks[:30]:
            payload = task.get('payload') or {}
            gains = [g for g in (payload.get('knowledge_gain') or []) if g]
            note = task.get('worker_note') or ''
            if 'knowledge_gain=' in note:
                gains.extend([x.strip() for x in note.split('knowledge_gain=', 1)[-1].split(',') if x.strip()])
            for gain in gains:
                if gain in knowledge_gain_counter:
                    knowledge_gain_counter[gain] += 1

        stall_state = {
            'queue_pending': len([t for t in research_tasks if t['status'] == 'pending']),
            'queue_running': len([t for t in research_tasks if t['status'] == 'running']),
            'recent_finished_tasks': len(recent_finished_tasks[:10]),
            'recent_failed_tasks': len(recent_failed_tasks[:10]),
            'budget_guarded_tasks': len(budget_guard_tasks),
            'blocked_family_events': len(blocked_family_heartbeats),
            'stalled': len(recent_finished_tasks[:6]) == 0 and len([t for t in research_tasks if t['status'] == 'pending']) == 0,
            'warning': len(recent_failed_tasks[:3]) >= 3,
        }

        run_health_score = 0
        if recent_24h_total:
            run_health_score += 60 * (recent_24h_finished / recent_24h_total)
        if latest_run and latest_run.get('status') == 'finished':
            run_health_score += 20
        if latest_run and latest_run.get('dataset_rows'):
            run_health_score += 20
        run_health_score = round(run_health_score, 1)

        stable_candidate_component = min(stable_candidate_count * 7.0, 35.0)
        candidate_population = max(len(set(latest_candidates)), len(set(previous_candidates)), 1)
        candidate_churn_ratio = min(candidate_churn / candidate_population, 1.0)
        convergence_component = round((1.0 - candidate_churn_ratio) * 20.0, 1)
        recommendation_component = round((recommendation_hit_rate or 0.0) * 25.0, 1) if recommendation_hit_rate is not None else 0.0
        candidate_activity_component = round(min(len(set(latest_candidates)) / 5.0, 1.0) * 20.0, 1)
        research_progress_snapshot_score = round(min(
            stable_candidate_component + convergence_component + recommendation_component + candidate_activity_component,
            100,
        ), 1)
        research_progress_timeline_score = round(float(timeline_summary['research_current']), 1)
        research_display = _build_research_progress_display(
            research_progress_snapshot_score,
            research_progress_timeline_score,
            timeline_candidate_run_count=timeline_candidate_run_count,
            timeline_candidate_factor_count=timeline_candidate_factor_count,
            timeline_point_count=len(timeline_runs),
        )

        portfolio_sharpe_component = min(max(candidates_only_recent_sharpe or 0.0, 0.0) * 5, 40) if candidates_only_recent_sharpe is not None else 0.0
        portfolio_stability_component = round(float(paper_stability.get('stability_score') or 0.0) * 40, 1) if paper_stability.get('stability_score') is not None else 0.0
        portfolio_return_component = 20.0 if candidates_only_recent_return is not None and all_factors_recent_return is not None and candidates_only_recent_return >= 0.6 * all_factors_recent_return else 0.0
        portfolio_progress_snapshot_score = round(min(portfolio_sharpe_component + portfolio_stability_component + portfolio_return_component, 100), 1)
        portfolio_progress_timeline_score = round(float(timeline_summary['portfolio_current']), 1)
        portfolio_progress_score = portfolio_progress_snapshot_score

        return {
            'latest_run': latest_run,
            'recent_runs': runs,
            'run_health': {
                'score': run_health_score,
                'runs_24h': recent_24h_total,
                'finished_24h': recent_24h_finished,
                'runs_7d': recent_7d_total,
                'finished_7d': recent_7d_finished,
                'success_rate_24h': round(recent_24h_finished / recent_24h_total, 4) if recent_24h_total else None,
                'success_rate_7d': round(recent_7d_finished / recent_7d_total, 4) if recent_7d_total else None,
                'latest_status': latest_run.get('status') if latest_run else None,
                'latest_end_date': latest_run.get('end_date') if latest_run else None,
                'latest_dataset_rows': latest_run.get('dataset_rows') if latest_run else None,
                'recent_heartbeat_rows': recent_heartbeat_rows[::-1],
                'run_success_trend': run_success_trend,
                'run_success_sparkline': sparkline_svg(run_success_trend, color='#3ddc97'),
                'daemon_status': daemon_status,
            },
            'autonomy_metrics': research_metrics.get('metrics') or {},
            'research_progress': {
                'score': research_display['score'],
                'snapshot_score': research_progress_snapshot_score,
                'timeline_score': research_progress_timeline_score,
                'score_basis': research_display['basis'],
                'score_warning': research_display['warning'],
                'stable_candidate_count': stable_candidate_count,
                'latest_candidates': latest_candidates,
                'previous_candidates': previous_candidates,
                'candidate_churn': candidate_churn,
                'recent_candidate_counts': recent_candidate_counts,
                'candidate_count_trend': candidate_count_trend,
                'candidate_count_sparkline': sparkline_svg(candidate_count_trend, color='#ffd166'),
                'factor_score_trend': factor_score_trend,
                'llm_status': llm_status.get('status'),
                'breakdown': {
                    'stable_candidate_component': round(stable_candidate_component, 1),
                    'convergence_component': round(convergence_component, 1),
                    'recommendation_component': round(recommendation_component, 1),
                    'candidate_activity_component': round(candidate_activity_component, 1),
                },
                'timeline_candidate_run_count': timeline_candidate_run_count,
                'timeline_candidate_factor_count': timeline_candidate_factor_count,
                'recommendation_hit_rate': recommendation_hit_rate,
                'recommendation_tail_size': len(recommendation_history_tail),
                'knowledge_gain_counter': knowledge_gain_counter,
                'stall_state': stall_state,
                'budget_guard_tasks': budget_guard_tasks[:12],
                'blocked_family_heartbeats': blocked_family_heartbeats[:12],
            },
            'portfolio_progress': {
                'score': portfolio_progress_score,
                'snapshot_score': portfolio_progress_snapshot_score,
                'timeline_score': portfolio_progress_timeline_score,
                'candidates_only_recent_sharpe': candidates_only_recent_sharpe,
                'all_factors_recent_sharpe': all_factors_recent_sharpe,
                'candidates_only_recent_return': candidates_only_recent_return,
                'all_factors_recent_return': all_factors_recent_return,
                'paper_stability': paper_stability,
                'paper_stability_source': paper_stability_source,
                'breakdown': {
                    'sharpe_component': round(portfolio_sharpe_component, 1),
                    'stability_component': round(portfolio_stability_component, 1),
                    'return_component': round(portfolio_return_component, 1),
                },
                'strategy_sharpe_trend': strategy_sharpe_trend,
                'strategy_sharpe_sparkline': sparkline_svg(strategy_sharpe_trend, color='#84a8ff'),
            },
            'score_timeline': {
                'chart_svg': timeline_chart,
                'summary': timeline_summary,
                'series': timeline_series,
            },
            'resource_timeline': {
                'chart_svg': resource_timeline_chart,
                'series': resource_timeline_series,
            },
            'project_progress_observation': project_progress,
            'decision_layer': decision_layer,
            'repair_layer': repair_layer,
        }
    finally:
        conn.close()


BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "webui_templates"
STATIC_DIR = BASE_DIR / "webui_static"
DB_PATH = Path(__file__).resolve().parents[2] / "artifacts" / "factor_lab.db"

PRIMARY_NAV = [
    {"id": "overview", "label": "总览", "href": "/", "icon": "01"},
    {"id": "research", "label": "研究", "href": "/research", "icon": "02"},
    {"id": "portfolio", "label": "组合", "href": "/portfolios", "icon": "03"},
    {"id": "runs", "label": "运行", "href": "/runs", "icon": "04"},
    {"id": "settings", "label": "设置", "href": "/data-sources", "icon": "05"},
]

SECONDARY_NAV = {
    "research": [
        {"id": "research", "label": "研究总览", "href": "/research"},
        {"id": "hypothesis-lineage", "label": "假设谱系", "href": "/research#hypothesis-lineage"},
        {"id": "trial-budget", "label": "统计预算", "href": "/research#trial-budget"},
        {"id": "experiments", "label": "实验与否证", "href": "/research#experiments"},
        {"id": "recovery", "label": "恢复流程", "href": "/research#recovery"},
    ],
    "portfolio": [
        {"id": "portfolios", "label": "Champion / Sleeve", "href": "/portfolios"},
    ],
    "runs": [
        {"id": "runs", "label": "Research OS 运行", "href": "/runs"},
    ],
    "settings": [
        {"id": "data-sources", "label": "数据源与快照", "href": "/data-sources"},
        {"id": "settings", "label": "模型配置", "href": "/settings"},
    ],
}

TEMPLATE_NAV = {
    "dashboard_quick.html": ("overview", "overview"),
    "research.html": ("research", "research"),
    "factors.html": ("research", "factors"),
    "candidates.html": ("research", "candidates"),
    "candidate_detail.html": ("research", "candidates"),
    "families.html": ("research", "families"),
    "candidate_clusters.html": ("research", "clusters"),
    "robustness.html": ("research", "robustness"),
    "research_quality.html": ("research", "research-quality"),
    "portfolios.html": ("portfolio", "portfolios"),
    "paper_portfolio.html": ("portfolio", "paper-portfolio"),
    "approved_universe.html": ("portfolio", "approved-universe"),
    "exposure.html": ("portfolio", "exposure"),
    "runs.html": ("runs", "runs"),
    "run_detail.html": ("runs", "runs"),
    "health.html": ("runs", "health"),
    "weekly.html": ("runs", "weekly"),
    "ops.html": ("runs", "ops"),
    "control.html": ("runs", "control"),
    "data_sources.html": ("settings", "data-sources"),
    "settings.html": ("settings", "settings"),
    "llm.html": ("settings", "llm"),
    "llm_usage.html": ("settings", "llm-usage"),
}

PAGE_SUBTITLES = {
    "overview": "先看研究结论，再处理异常与细节。",
    "research": "跟踪因子从实验到候选的研究过程。",
    "portfolio": "比较策略表现、风险暴露和模拟组合。",
    "runs": "查看执行记录、健康状态与本地操作。",
    "settings": "管理数据连接、模型与使用情况。",
}


def get_conn() -> sqlite3.Connection:
    ensure_views(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_all(query: str, params: tuple[Any, ...] = ()):
    conn = get_conn()
    try:
        return [dict(row) for row in conn.execute(query, params).fetchall()]
    finally:
        conn.close()


def fetch_one(query: str, params: tuple[Any, ...] = ()):
    conn = get_conn()
    try:
        row = conn.execute(query, params).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)

app = create_app()
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# Active WebUI requests have one authoritative state boundary: the Research OS
# PostgreSQL read model.  The handlers below this point are retained as legacy
# import/regression helpers, but pages without a PostgreSQL projection must not
# execute them.  Keeping this routing policy in one place makes it auditable and
# prevents a newly linked secondary page from silently reopening artifact JSON
# or the old ExperimentStore SQLite database.
RESEARCH_OS_GET_REDIRECTS = {
    "/dashboard-full": "/",
    "/cockpit": "/",
    "/factors": "/research",
    "/candidates": "/research",
    "/families": "/research",
    "/candidate-clusters": "/research",
    "/robustness": "/research",
    "/research-quality": "/research",
    "/paper-portfolio": "/portfolios",
    "/approved-universe": "/portfolios",
    "/exposure": "/portfolios",
    "/health": "/runs",
    "/weekly": "/runs",
    "/ops": "/runs",
    "/control": "/runs",
    "/harvest-agent/status": "/runs",
    "/autonomous-strategy-lab/status": "/runs",
    "/llm": "/data-sources",
    "/llm-usage": "/data-sources",
}
RESEARCH_OS_GET_PREFIX_REDIRECTS = (
    ("/runs/", "/runs"),
    ("/candidates/", "/research"),
)


def research_os_get_redirect_target(path: str) -> str | None:
    normalized = path.rstrip("/") or "/"
    exact = RESEARCH_OS_GET_REDIRECTS.get(normalized)
    if exact is not None:
        return exact
    for prefix, target in RESEARCH_OS_GET_PREFIX_REDIRECTS:
        if normalized.startswith(prefix):
            return target
    return None


@app.middleware("http")
async def enforce_research_os_get_boundary(request: Request, call_next):
    if request.method == "GET":
        target = research_os_get_redirect_target(request.url.path)
        if target is not None:
            return RedirectResponse(url=target, status_code=307)
    return await call_next(request)


def warm_dashboard_cache() -> None:
    # Keep startup and the overview lightweight. Heavy health data remains lazy.
    get_overview_snapshot()


register_startup_cache(app, warm_dashboard_cache)


def render(template_name: str, **context) -> HTMLResponse:
    section, page = TEMPLATE_NAV.get(template_name, ("overview", "overview"))
    defaults = {
        "primary_nav": PRIMARY_NAV,
        "secondary_nav": SECONDARY_NAV.get(section, []),
        "current_section": section,
        "current_page": page,
        "page_subtitle": PAGE_SUBTITLES.get(section),
        "back_href": "/runs" if template_name == "run_detail.html" else "/candidates" if template_name == "candidate_detail.html" else None,
        "back_label": "返回运行记录" if template_name == "run_detail.html" else "返回候选榜单" if template_name == "candidate_detail.html" else None,
    }
    defaults.update(context)
    return render_template(env, localize_times, template_name, **defaults)


register_data_source_routes(
    app,
    render=render,
    load_research_os_read_model=lambda: get_research_os_read_model(),
    load_data_source_settings=lambda: load_data_source_settings(),
    save_data_source_settings=lambda form: save_data_source_settings(form),
    restart_research_daemon_after_settings_save=lambda: restart_research_daemon_after_settings_save(),
    read_env_values=lambda: _read_env_values(),
    data_source_profiles_from_form=lambda form, existing_profiles: _data_source_profiles_from_form(form, existing_profiles),
    redacted_data_source_profile=lambda profile: _redacted_data_source_profile(profile),
    test_data_source_connection=lambda profile: test_data_source_connection(profile),
    source_type_options=DATA_SOURCE_TYPE_OPTIONS,
)

register_llm_settings_routes(
    app,
    render=render,
    load_llm_settings=lambda: load_llm_settings(),
    save_llm_settings=lambda form: save_llm_settings(form),
    restart_research_daemon_after_settings_save=lambda: restart_research_daemon_after_settings_save(),
    read_env_values=lambda: _read_env_values(),
    profiles_from_form=lambda form, existing_profiles: _profiles_from_form(form, existing_profiles),
    test_llm_profile_connection=lambda profile: test_llm_profile_connection(profile),
    api_format_options=LLM_API_FORMAT_OPTIONS,
)

def build_candidate_detail_context(store: ExperimentStore, candidate_id: str) -> dict[str, Any]:
    candidates = store.list_factor_candidates(limit=1000)
    evaluations = store.list_factor_evaluations(limit=5000)
    relationships = store.list_candidate_relationships(limit=5000)
    graph_context = build_candidate_graph_context(candidates, evaluations, relationships)
    candidate_context = next((row for row in graph_context.get('candidate_context', []) if row.get('candidate_id') == candidate_id), None) or {}
    family_lookup = {row.get('family'): row for row in graph_context.get('families', [])}
    cluster = candidate_context.get('cluster') or {}
    lineage = candidate_context.get('lineage', [])
    related_candidates = candidate_context.get('related_candidates', [])
    family_row = family_lookup.get(candidate_context.get('family')) or {}
    return {
        'candidate_context': candidate_context,
        'candidate_lineage': lineage,
        'related_candidates': related_candidates,
        'cluster_membership': cluster,
        'family_rollup': family_row,
        'relationship_summary': graph_context.get('relationship_summary', {}),
    }


def _quick_provider_status() -> tuple[str | None, str | None]:
    env_path = env_file()
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("FACTOR_LAB_AGENT_BACKEND="):
                return line.split("=", 1)[1].strip().strip('"').strip("'"), ".env:FACTOR_LAB_AGENT_BACKEND"
            if line.startswith("FACTOR_LAB_DECISION_PROVIDER="):
                return line.split("=", 1)[1].strip().strip('"').strip("'"), ".env:FACTOR_LAB_DECISION_PROVIDER"
    for key in ("FACTOR_LAB_AGENT_BACKEND", "FACTOR_LAB_DECISION_PROVIDER", "FACTOR_LAB_LIVE_DECISION_PROVIDER"):
        value = os.environ.get(key)
        if value:
            return value, f"env:{key}"
    return None, None


def _quick_daemon_status() -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "is-active", "factor-lab-research-daemon.service"],
            text=True,
            capture_output=True,
            timeout=0.5,
            check=False,
        )
        status = (proc.stdout or proc.stderr).strip() or "unknown"
        return {"active": status == "active", "label": status, "detail": "systemd user service"}
    except Exception as exc:
        return {"active": False, "label": "unknown", "detail": str(exc)}


def _quick_latest_runs(limit: int = 5) -> list[dict[str, Any]]:
    if not DB_PATH.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=0.2)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT run_id, created_at_utc, status, config_path FROM workflow_runs ORDER BY created_at_utc DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
    except Exception:
        return []


_OVERVIEW_CACHE: dict[str, Any] = {"key": None, "created_at": 0.0, "payload": None}
_RESEARCH_OS_READ_MODEL_CACHE: dict[str, Any] = {
    "key": None,
    "created_at": 0.0,
    "payload": None,
}


def _read_json_safely(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _artifact_run_snapshots(limit: int = 8) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for state_path in DB_PATH.parent.glob("*/task_state.json"):
        state = _read_json_safely(state_path, {})
        if not isinstance(state, dict) or state.get("status") != "finished":
            continue
        output_dir = Path(str(state.get("output_dir") or state_path.parent))
        if not output_dir.is_absolute():
            output_dir = DB_PATH.parent.parent / output_dir
        if not output_dir.exists():
            output_dir = state_path.parent
        config_path = Path(str(state.get("config_path") or ""))
        if not config_path.is_absolute():
            config_path = DB_PATH.parent.parent / config_path
        config = _read_json_safely(config_path, {})
        rows.append({
            "run_id": state.get("task_id") or state_path.parent.name,
            "created_at_utc": state.get("finished_at_utc") or state.get("started_at_utc"),
            "status": "finished",
            "config_path": state.get("config_path") or "-",
            "output_dir": str(output_dir),
            "data_source": config.get("data_source") or "sample",
        })
    rows.sort(key=lambda row: str(row.get("created_at_utc") or ""), reverse=True)
    return rows[:limit]


def _latest_db_run() -> dict[str, Any] | None:
    if not DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=0.2)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT run_id, created_at_utc, status, config_path, output_dir, data_source FROM workflow_runs WHERE status='finished' ORDER BY CASE WHEN data_source='tushare' THEN 0 ELSE 1 END, created_at_utc DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception:
        return None


def _overview_run_dir(run: dict[str, Any]) -> Path | None:
    raw = str(run.get("output_dir") or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = DB_PATH.parent.parent / path
    return path if path.exists() else None


def _build_legacy_overview_snapshot() -> dict[str, Any]:
    artifact_runs = _artifact_run_snapshots()
    latest_artifact = next((row for row in artifact_runs if row.get("data_source") == "tushare"), artifact_runs[0] if artifact_runs else None)
    latest_run = _latest_db_run() or latest_artifact
    latest_runs = _quick_latest_runs(limit=5) or artifact_runs[:5]
    queue_counts, latest_task = _quick_research_queue_snapshot()
    provider, provider_source = _quick_provider_status()
    payload: dict[str, Any] = {
        "latest_run": latest_run,
        "latest_runs": latest_runs,
        "factor_results": [],
        "passed_count": 0,
        "failed_count": 0,
        "best_factor": None,
        "best_portfolio": None,
        "data_source": "-",
        "dataset_rows": None,
        "alerts": [],
        "daemon_status": _quick_daemon_status(),
        "queue_counts": queue_counts,
        "latest_task": latest_task,
        "provider": provider,
        "provider_source": provider_source,
    }
    if not latest_run:
        payload["alerts"].append({"tone": "info", "message": "还没有完成的研究运行", "detail": "先运行一次 Workflow，结果会自动出现在这里。"})
        return payload

    run_dir = _overview_run_dir(latest_run)
    if run_dir is None:
        payload["alerts"].append({"tone": "warn", "message": "最新运行缺少本地产物", "detail": "可在运行记录中检查输出目录。"})
        return payload

    results = _read_json_safely(run_dir / "results.json", [])
    scores = _read_json_safely(run_dir / "factor_scores.json", [])
    portfolios = _read_json_safely(run_dir / "portfolio_results.json", [])
    timing = _read_json_safely(run_dir / "timing.json", {})
    if not isinstance(results, list):
        results = []
    if not isinstance(scores, list):
        scores = []
    if not isinstance(portfolios, list):
        portfolios = []
    score_by_name = {str(row.get("factor_name") or ""): row for row in scores if isinstance(row, dict)}
    factor_results = []
    for row in results:
        if not isinstance(row, dict):
            continue
        score = score_by_name.get(str(row.get("factor_name") or ""), {})
        factor_results.append({**row, "score": score.get("score") or score.get("composite_score")})
    factor_results.sort(key=lambda row: float(row.get("score") if row.get("score") is not None else row.get("rank_ic_mean") or -999), reverse=True)
    passed_count = len([row for row in factor_results if row.get("pass_gate") is True])
    failed_count = len(factor_results) - passed_count
    best_portfolio = max(portfolios, key=lambda row: float(row.get("sharpe") or -999), default=None)

    config_path = Path(str(latest_run.get("config_path") or ""))
    if not config_path.is_absolute():
        config_path = DB_PATH.parent.parent / config_path
    config = _read_json_safely(config_path, {})
    payload.update({
        "factor_results": factor_results[:5],
        "passed_count": passed_count,
        "failed_count": failed_count,
        "best_factor": factor_results[0] if factor_results else None,
        "best_portfolio": best_portfolio,
        "data_source": latest_run.get("data_source") or config.get("data_source") or "sample",
        "dataset_rows": timing.get("dataset_rows") if isinstance(timing, dict) else None,
    })
    if factor_results and passed_count == 0:
        payload["alerts"].append({"tone": "warn", "message": "本轮没有因子通过门槛", "detail": "运行已成功完成；这是研究结论，不是系统错误。"})
    return payload


def _research_os_read_model_cache_key() -> str:
    database_url = os.environ.get("FACTOR_LAB_DATABASE_URL", "postgresql-default")
    return hashlib.sha256(database_url.encode("utf-8")).hexdigest()


def get_research_os_read_model(max_age_seconds: float = 30.0) -> dict[str, Any]:
    """Return a cached, read-only projection of the authoritative catalog."""

    cache_key = _research_os_read_model_cache_key()
    now = time.monotonic()
    cached = _RESEARCH_OS_READ_MODEL_CACHE.get("payload")
    if (
        cached is not None
        and _RESEARCH_OS_READ_MODEL_CACHE.get("key") == cache_key
        and now - float(_RESEARCH_OS_READ_MODEL_CACHE.get("created_at") or 0.0)
        < max_age_seconds
    ):
        return deepcopy(cached)
    # Active WebUI pages are a PostgreSQL-only boundary.  SQLite remains
    # available to the read-model service only through an explicit direct test
    # call; no environment flag can make a live page treat legacy SQLite as
    # current Research OS state.
    payload = _load_research_os_read_model(allow_sqlite=False)
    _RESEARCH_OS_READ_MODEL_CACHE.update(
        {"key": cache_key, "created_at": now, "payload": deepcopy(payload)}
    )
    return payload


def _build_overview_snapshot() -> dict[str, Any]:
    """Build the homepage exclusively from the Research OS read model.

    Legacy SQLite and JSON artifacts remain importable as evidence, but active
    WebUI requests must never scan them or infer current system state from them.
    """

    return {"research_os": get_research_os_read_model()}


def get_overview_snapshot(max_age_seconds: float = 30.0) -> dict[str, Any]:
    cache_key = _research_os_read_model_cache_key()
    now = time.monotonic()
    cached = _OVERVIEW_CACHE.get("payload")
    if cached is not None and _OVERVIEW_CACHE.get("key") == cache_key and now - float(_OVERVIEW_CACHE.get("created_at") or 0.0) < max_age_seconds:
        return deepcopy(cached)
    payload = _build_overview_snapshot()
    _OVERVIEW_CACHE.update({"key": cache_key, "created_at": now, "payload": deepcopy(payload)})
    return payload


def _llm_usage_ledger_path() -> Path:
    return DB_PATH.parent / "llm_usage_ledger.jsonl"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ledger_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _load_llm_usage_rows(limit: int = 200, hours: int | None = 24) -> list[dict[str, Any]]:
    path = _llm_usage_ledger_path()
    if not path.exists():
        return []
    cutoff = _utcnow() - timedelta(hours=hours) if hours is not None else None
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    rows: list[dict[str, Any]] = []
    for line in reversed(lines):
        if len(rows) >= limit:
            break
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if cutoff is not None:
            created_at = _parse_ledger_time(row.get("created_at_utc"))
            if created_at is None or created_at < cutoff:
                continue
        rows.append(row)
    return rows


def _summarize_llm_usage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "rows": len(rows),
        "success": sum(1 for row in rows if row.get("success") is True),
        "failed": sum(1 for row in rows if row.get("success") is False),
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "cached_tokens": 0,
        "cached_tokens_missing_rows": 0,
        "cache_creation_tokens": 0,
        "cache_creation_tokens_missing_rows": 0,
        "uncached_prompt_tokens": 0,
        "uncached_prompt_tokens_missing_rows": 0,
        "estimated_cost_usd": 0.0,
        "estimated_user_prompt_tokens_4c": 0,
        "by_decision_type": {},
        "by_model": {},
        "by_provider": {},
    }
    for row in rows:
        usage = row.get("usage") or {}
        prompt_tokens = _safe_int(usage.get("prompt_tokens"))
        completion_tokens = _safe_int(usage.get("completion_tokens"))
        total_tokens = _safe_int(usage.get("total_tokens"))
        cached_tokens = _safe_int(usage.get("cached_tokens"))
        cache_creation_tokens = _safe_int(usage.get("cache_creation_tokens"))
        uncached_prompt_tokens = _safe_int(usage.get("uncached_prompt_tokens"))
        cost = estimate_llm_cost_usd(row.get("model"), usage)
        estimated_cost_usd = float(cost.get("estimated_cost_usd") or row.get("estimated_cost_usd") or 0.0)
        estimated = _safe_int(row.get("estimated_user_prompt_tokens_4c"))
        summary["prompt_tokens"] += prompt_tokens
        summary["completion_tokens"] += completion_tokens
        summary["total_tokens"] += total_tokens
        if usage.get("cached_tokens") is None:
            summary["cached_tokens_missing_rows"] += 1
        if usage.get("cache_creation_tokens") is None:
            summary["cache_creation_tokens_missing_rows"] += 1
        if usage.get("uncached_prompt_tokens") is None:
            summary["uncached_prompt_tokens_missing_rows"] += 1
        summary["cached_tokens"] += cached_tokens
        summary["cache_creation_tokens"] += cache_creation_tokens
        summary["uncached_prompt_tokens"] += uncached_prompt_tokens
        summary["estimated_cost_usd"] += estimated_cost_usd
        summary["estimated_user_prompt_tokens_4c"] += estimated
        decision_type = str(row.get("decision_type") or "unknown")
        model = str(row.get("model") or "unknown")
        provider = str(row.get("profile_name") or row.get("provider") or "unknown")
        decision_bucket = summary["by_decision_type"].setdefault(decision_type, {"rows": 0, "total_tokens": 0, "cached_tokens": 0, "estimated_cost_usd": 0.0})
        decision_bucket["rows"] += 1
        decision_bucket["total_tokens"] += total_tokens
        decision_bucket["cached_tokens"] += cached_tokens
        decision_bucket["estimated_cost_usd"] += estimated_cost_usd
        model_bucket = summary["by_model"].setdefault(model, {"rows": 0, "total_tokens": 0, "cached_tokens": 0, "estimated_cost_usd": 0.0})
        model_bucket["rows"] += 1
        model_bucket["total_tokens"] += total_tokens
        model_bucket["cached_tokens"] += cached_tokens
        model_bucket["estimated_cost_usd"] += estimated_cost_usd
        provider_bucket = summary["by_provider"].setdefault(provider, {"rows": 0, "total_tokens": 0, "cached_tokens": 0, "estimated_cost_usd": 0.0})
        provider_bucket["rows"] += 1
        provider_bucket["total_tokens"] += total_tokens
        provider_bucket["cached_tokens"] += cached_tokens
        provider_bucket["estimated_cost_usd"] += estimated_cost_usd
    summary["estimated_cost_usd"] = round(summary["estimated_cost_usd"], 6)
    for bucket in list(summary["by_decision_type"].values()) + list(summary["by_model"].values()) + list(summary["by_provider"].values()):
        bucket["estimated_cost_usd"] = round(bucket["estimated_cost_usd"], 6)
    summary["by_decision_type_rows"] = [
        {"name": name, **value} for name, value in sorted(summary["by_decision_type"].items())
    ]
    summary["by_provider_rows"] = [
        {"name": name, **value} for name, value in sorted(summary["by_provider"].items())
    ]
    summary["by_model_rows"] = [
        {"name": name, **value} for name, value in sorted(summary["by_model"].items())
    ]
    return summary


def _format_local_time(value: datetime | None = None) -> str:
    dt = value or _utcnow()
    return dt.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")


def _build_llm_usage_chart(rows: list[dict[str, Any]], hours: int = 24) -> list[dict[str, Any]]:
    now_local = _utcnow().astimezone(LOCAL_TZ).replace(minute=0, second=0, microsecond=0)
    buckets = []
    for offset in range(hours - 1, -1, -1):
        start = now_local - timedelta(hours=offset)
        buckets.append({
            "start": start,
            "label": start.strftime("%H:%M"),
            "full_label": start.strftime("%m-%d %H:%M"),
            "total_tokens": 0,
            "estimated_tokens": 0,
            "cached_tokens": 0,
            "estimated_cost_usd": 0.0,
            "rows": 0,
            "height_pct": 0,
        })
    by_start = {bucket["start"]: bucket for bucket in buckets}
    for row in rows:
        created_at = _parse_ledger_time(row.get("created_at_utc"))
        if created_at is None:
            continue
        bucket_start = created_at.astimezone(LOCAL_TZ).replace(minute=0, second=0, microsecond=0)
        bucket = by_start.get(bucket_start)
        if bucket is None:
            continue
        usage = row.get("usage") or {}
        bucket["total_tokens"] += _safe_int(usage.get("total_tokens"))
        bucket["cached_tokens"] += _safe_int(usage.get("cached_tokens"))
        cost = estimate_llm_cost_usd(row.get("model"), usage)
        bucket["estimated_cost_usd"] += float(cost.get("estimated_cost_usd") or row.get("estimated_cost_usd") or 0.0)
        bucket["estimated_tokens"] += _safe_int(row.get("estimated_user_prompt_tokens_4c"))
        bucket["rows"] += 1
    max_value = max([bucket["total_tokens"] for bucket in buckets] + [0])
    for bucket in buckets:
        bucket["height_pct"] = int((bucket["total_tokens"] / max_value) * 100) if max_value else 0
        bucket["estimated_cost_usd"] = round(bucket["estimated_cost_usd"], 6)
    return buckets


def _latest_llm_usage_rows(limit: int = 50) -> list[dict[str, Any]]:
    rows = _load_llm_usage_rows(limit=limit, hours=24)
    for row in rows:
        usage = row.get("usage") or {}
        row["usage_total_tokens"] = usage.get("total_tokens")
        row["usage_prompt_tokens"] = usage.get("prompt_tokens")
        row["usage_completion_tokens"] = usage.get("completion_tokens")
        row["usage_cached_tokens"] = usage.get("cached_tokens")
        row["usage_cache_creation_tokens"] = usage.get("cache_creation_tokens")
        row["usage_uncached_prompt_tokens"] = usage.get("uncached_prompt_tokens")
        row["usage_source"] = usage.get("usage_source")
        cost = estimate_llm_cost_usd(row.get("model"), usage)
        row["estimated_cost_usd"] = cost.get("estimated_cost_usd") or row.get("estimated_cost_usd")
        row["pricing_family"] = cost.get("pricing_family")
    return rows


def _systemd_service_snapshot(service: str) -> dict[str, Any]:
    snapshot = {
        "name": service,
        "active_state": "unknown",
        "main_pid": None,
        "working_directory": None,
        "exec_start": None,
        "fragment_path": None,
    }
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show", service, "--no-pager"],
            text=True,
            capture_output=True,
            timeout=0.8,
            check=False,
        )
    except Exception as exc:
        snapshot["active_state"] = "error"
        snapshot["exec_start"] = str(exc)
        return snapshot
    if proc.returncode != 0:
        snapshot["active_state"] = "error"
        snapshot["exec_start"] = (proc.stderr or proc.stdout).strip()
        return snapshot
    data: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            data[key] = value
    snapshot.update(
        {
            "active_state": data.get("ActiveState") or "unknown",
            "main_pid": data.get("MainPID") or None,
            "working_directory": data.get("WorkingDirectory") or None,
            "exec_start": data.get("ExecStart") or None,
            "fragment_path": data.get("FragmentPath") or None,
        }
    )
    return snapshot


def _quick_research_queue_snapshot() -> tuple[dict[str, int], dict[str, Any] | None]:
    empty_counts = {"pending": 0, "running": 0, "finished_24h": 0, "failed_24h": 0}
    if not DB_PATH.exists():
        return empty_counts, None
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=0.2)
        conn.row_factory = sqlite3.Row
        try:
            status_rows = conn.execute("SELECT status, COUNT(*) AS n FROM research_tasks GROUP BY status").fetchall()
            counts = dict(empty_counts)
            for row in status_rows:
                status = row["status"]
                if status in counts:
                    counts[status] = int(row["n"] or 0)
            counts["finished_24h"] = int(conn.execute("SELECT COUNT(*) FROM research_tasks WHERE status='finished' AND finished_at_utc >= datetime('now', '-1 day')").fetchone()[0])
            counts["failed_24h"] = int(conn.execute("SELECT COUNT(*) FROM research_tasks WHERE status='failed' AND finished_at_utc >= datetime('now', '-1 day')").fetchone()[0])
            latest = conn.execute("SELECT task_id, task_type, status, created_at_utc, worker_note FROM research_tasks ORDER BY created_at_utc DESC LIMIT 1").fetchone()
            return counts, (dict(latest) if latest else None)
        finally:
            conn.close()
    except Exception:
        return empty_counts, None


def _quick_heartbeat() -> dict[str, Any] | None:
    path = DB_PATH.parent / "research_daemon_heartbeat.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"timestamp": None, "error": str(exc)}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return render(
        "dashboard_quick.html",
        title="研究总览",
        overview=get_overview_snapshot(),
    )


@app.get("/control", response_class=HTMLResponse)
def control_page():
    provider, provider_source = _quick_provider_status()
    queue_counts, latest_task = _quick_research_queue_snapshot()
    return render(
        "control.html",
        title="控制",
        services=[
            _systemd_service_snapshot("factor-lab-web-ui.service"),
            _systemd_service_snapshot("factor-lab-research-daemon.service"),
        ],
        provider=provider,
        provider_source=provider_source,
        queue_counts=queue_counts,
        latest_task=latest_task,
        heartbeat=_quick_heartbeat(),
    )


@app.get("/dashboard-full", response_class=HTMLResponse)
def dashboard_full():
    return RedirectResponse(url="/", status_code=307)


def _legacy_dashboard_full_payload():
    """Retained temporarily as a compatibility reference for old dashboard queries."""
    health = get_cached_health_metrics()
    latest_runs = fetch_all(
        "SELECT run_id, created_at_utc, status, config_path FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 8"
    )
    stable_candidates = fetch_all(
        "SELECT factor_name, candidate_runs FROM v_stable_candidates ORDER BY candidate_runs DESC, factor_name ASC"
    )
    exposure_leaderboard = fetch_all(
        "SELECT factor_name, exposure_type, strength_score, raw_rank_ic_mean, split_fail_count, crowding_peers, recommended_max_weight, status FROM v_exposure_leaderboard WHERE run_id = (SELECT run_id FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1) ORDER BY COALESCE(strength_score, -999) DESC LIMIT 8"
    )
    top_factors = fetch_all(
        "SELECT factor_name, ROUND(avg_score, 6) AS avg_score, runs FROM v_factor_score_avg ORDER BY avg_score DESC LIMIT 8"
    )
    candidate_leaderboard = fetch_all(
        "SELECT id, name, family, status, ROUND(latest_final_score, 6) AS latest_final_score, ROUND(latest_recent_final_score, 6) AS latest_recent_final_score, ROUND(avg_final_score, 6) AS avg_final_score, evaluation_count, window_count FROM v_factor_candidate_leaderboard ORDER BY COALESCE(latest_recent_final_score, latest_final_score, -999) DESC, evaluation_count DESC LIMIT 8"
    )
    top_strategies = fetch_all(
        "SELECT strategy_name, ROUND(avg_sharpe, 6) AS avg_sharpe, ROUND(avg_return, 6) AS avg_return, runs FROM v_portfolio_strategy_avg ORDER BY avg_sharpe DESC LIMIT 8"
    )
    latest_run = fetch_one(
        "SELECT run_id, created_at_utc, config_path, status, output_dir FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1"
    )
    previous_finished = fetch_one(
        "SELECT run_id, created_at_utc, config_path FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1 OFFSET 1"
    )
    planner_tasks = ExperimentStore(DB_PATH).list_research_tasks(limit=20)
    planner_active = [
        t for t in planner_tasks
        if t['status'] in {'pending', 'running'} and 'planner_selected' in (t.get('worker_note') or '')
    ][:6]
    generated_candidate_active = [
        t for t in planner_tasks
        if t['status'] in {'pending', 'running'} and ((t.get('payload') or {}).get('source') == 'candidate_generation')
    ][:6]
    representative_active = [
        t for t in planner_tasks
        if t['status'] in {'pending', 'running'} and '代表因子专项验证' in (t.get('worker_note') or '')
    ][:6]
    planner_validated_path = DB_PATH.parent / 'research_planner_validated.json'
    planner_injected_path = DB_PATH.parent / 'research_planner_injected.json'
    research_flow_state_path = DB_PATH.parent / 'research_flow_state.json'
    research_learning_path = DB_PATH.parent / 'research_learning.json'
    planner_validated = json.loads(planner_validated_path.read_text(encoding='utf-8')) if planner_validated_path.exists() else {}
    planner_injected = json.loads(planner_injected_path.read_text(encoding='utf-8')) if planner_injected_path.exists() else {}
    research_flow_state = json.loads(research_flow_state_path.read_text(encoding='utf-8')) if research_flow_state_path.exists() else {}
    research_learning = json.loads(research_learning_path.read_text(encoding='utf-8')) if research_learning_path.exists() else {}
    promotion_scorecard = get_cached_promotion_scorecard(limit=6)
    approved_universe_path = DB_PATH.parent / 'approved_candidate_universe.json'
    approved_universe = json.loads(approved_universe_path.read_text(encoding='utf-8')) if approved_universe_path.exists() else {}
    novelty_judge = load_novelty_judgments(DB_PATH.parent)
    allocator_governance_audit = load_allocator_governance_audit(DB_PATH.parent)
    decision_ab = load_decision_ab_artifacts(DB_PATH.parent)
    diagnostician_enhancement = load_diagnostician_enhancement(DB_PATH.parent)
    stable_names = [row['factor_name'] for row in stable_candidates[:4]]
    latest_output_dir = Path(latest_run['output_dir']) if latest_run and latest_run.get('output_dir') else None
    candidate_status_snapshot = []
    rolling_summary_rows = []
    if latest_output_dir and (latest_output_dir / 'candidate_status_snapshot.json').exists():
        candidate_status_snapshot = json.loads((latest_output_dir / 'candidate_status_snapshot.json').read_text(encoding='utf-8'))
    if latest_output_dir and (latest_output_dir / 'rolling_summary.json').exists():
        rolling_summary_rows = json.loads((latest_output_dir / 'rolling_summary.json').read_text(encoding='utf-8'))
    stage_summary = {'explore': [], 'watchlist': [], 'candidate': [], 'graveyard': []}
    for row in candidate_status_snapshot:
        stage = row.get('research_stage')
        if stage in stage_summary:
            stage_summary[stage].append(row)
    family_rollup = {}
    for row in candidate_status_snapshot:
        factor_name = row.get('factor_name')
        family = 'other'
        for cand in candidate_leaderboard:
            if cand.get('name') == factor_name:
                family = cand.get('family') or 'other'
                break
        bucket = family_rollup.setdefault(family, {'family': family, 'count': 0, 'top_factor': factor_name, 'note': row.get('promotion_reason') or '-'})
        bucket['count'] += 1
        if bucket['count'] == 1:
            bucket['top_factor'] = factor_name
            bucket['note'] = row.get('promotion_reason') or '-'
    family_summary_rows = sorted(family_rollup.values(), key=lambda item: (-item['count'], item['family']))[:8]
    latest_summary_lines = []
    if latest_run:
        latest_summary_lines.append(f"最新一次任务：{latest_run['config_path']}（{format_bj_time(latest_run['created_at_utc'])}）。")
    if top_strategies:
        latest_summary_lines.append(
            f"当前长期平均表现最好的策略是 {top_strategies[0]['strategy_name']}，平均夏普 {top_strategies[0]['avg_sharpe']}。"
        )
    if stable_names:
        latest_summary_lines.append(f"目前最稳定的候选因子：{'、'.join(stable_names)}。")
    if planner_active:
        latest_summary_lines.append(
            f"当前队列里有 {len(planner_active)} 个由 planner 选出的任务正在等待或执行。"
        )
    if generated_candidate_active:
        latest_summary_lines.append(
            f"当前有 {len(generated_candidate_active)} 个 generated candidate 任务处于活跃状态。"
        )
    if representative_active:
        latest_summary_lines.append(
            f"当前有 {len(representative_active)} 个代表因子专项验证任务处于活跃状态。"
        )
    if candidate_leaderboard:
        latest_display_score = candidate_leaderboard[0].get('latest_recent_final_score')
        if latest_display_score is None:
            latest_display_score = candidate_leaderboard[0].get('latest_final_score')
        latest_summary_lines.append(
            f"当前候选榜单第一名是 {candidate_leaderboard[0]['name']}，状态 {candidate_leaderboard[0]['status']}，最近短窗分 {latest_display_score}。"
        )
    promotion_rows = (promotion_scorecard.get('summary') or {}).get('priority_rows') or []
    promotion_summary = promotion_scorecard.get('summary') or {}
    if promotion_rows:
        latest_summary_lines.append(
            "当前晋级赛优先处理：" + "；".join(
                f"{row['factor_name']}({row['decision_label']})" for row in promotion_rows[:3]
            ) + "。"
        )
        latest_summary_lines.append(
            "当前质量分层重点：" + "；".join(
                f"{row['factor_name']}({row.get('quality_classification_label') or row.get('quality_classification')})" for row in promotion_rows[:3]
            ) + "。"
        )
    latest_summary_lines.append(
        "质量分层统计："
        f"稳定 alpha 候选 {int(promotion_summary.get('stable_alpha_candidate_count') or 0)} 个，"
        f"继续验证 {int(promotion_summary.get('needs_validation_count') or 0)} 个，"
        f"Exposure Track {int(promotion_summary.get('exposure_track_count') or 0)} 个，"
        f"Regime-sensitive {int(promotion_summary.get('quality_regime_sensitive_count') or 0)} 个，"
        f"重复候选压制 {int(promotion_summary.get('duplicate_suppress_count') or 0)} 个。"
    )
    if approved_universe:
        au_summary = approved_universe.get('summary') or {}
        latest_summary_lines.append(
            f"Approved Universe：{int(au_summary.get('approved_count') or 0)} 个，当前入池={ '、'.join([row.get('factor_name') for row in (approved_universe.get('rows') or [])[:5] if row.get('factor_name')]) or '无' }。"
        )
        latest_summary_lines.append(
            f"AU 治理：state={au_summary.get('state_counts') or {}}, actions={au_summary.get('governance_action_counts') or {}}, bucket_budget={(approved_universe.get('budget_summary') or {}).get('bucket_allocations') or {}}。"
        )
    if diagnostician_enhancement and diagnostician_enhancement.get('summary'):
        latest_summary_lines.append(
            f"Failure Analyst+：reroute={diagnostician_enhancement['summary'].get('reroute_count') or 0}, stop={diagnostician_enhancement['summary'].get('stop_count') or 0}, question_cards_v2={diagnostician_enhancement['summary'].get('question_card_count') or 0}。"
        )
    latest_summary = '\n'.join(latest_summary_lines) if latest_summary_lines else '暂无摘要。'

    change_lines = []
    if latest_run and previous_finished:
        change_lines.append(f"最新完成运行：{latest_run['config_path']}（{format_bj_time(latest_run['created_at_utc'])}）。")
        change_lines.append(f"上一轮完成运行：{previous_finished['config_path']}（{format_bj_time(previous_finished['created_at_utc'])}）。")
    if planner_active:
        change_lines.append('当前活跃研究任务：')
        change_lines.extend([f"- {t.get('worker_note') or t['task_type']}" for t in planner_active])
    else:
        change_lines.append('当前没有活跃的 planner 任务。')
    change_report = '\n'.join(change_lines)
    return render(
        "dashboard.html",
        title="总览",
        health=health,
        latest_runs=latest_runs,
        stable_candidates=stable_candidates,
        exposure_leaderboard=exposure_leaderboard,
        top_factors=top_factors,
        candidate_leaderboard=candidate_leaderboard,
        top_strategies=top_strategies,
        latest_summary=latest_summary,
        change_report=change_report,
        planner_active=planner_active,
        generated_candidate_active=generated_candidate_active,
        representative_active=representative_active,
        planner_validated=planner_validated,
        planner_injected=planner_injected,
        research_flow_state=research_flow_state,
        research_learning=research_learning,
        promotion_scorecard=promotion_scorecard,
        approved_universe=approved_universe,
        novelty_judge=novelty_judge,
        allocator_governance_audit=allocator_governance_audit,
        decision_ab=decision_ab,
        diagnostician_enhancement=diagnostician_enhancement,
        stage_summary=stage_summary,
        rolling_summary_rows=rolling_summary_rows[:8],
        family_summary_rows=family_summary_rows,
    )


@app.get("/health", response_class=HTMLResponse)
def health_page():
    health = get_cached_health_metrics()
    weekly = compute_weekly_report(health)
    return render(
        "health.html",
        title="健康度",
        health=health,
        weekly=weekly,
        research_os=get_research_os_read_model(),
    )


@app.get("/harvest-agent/status")
def harvest_agent_status():
    """Small WebUI/API status hook for Harvest Agent artifacts; no scheduling side effects."""
    return build_harvest_report()


@app.get("/autonomous-strategy-lab/status")
def autonomous_strategy_lab_status():
    """Small WebUI/API status hook for Autonomous Strategy Lab artifacts; no scheduling side effects."""
    return build_autonomous_strategy_lab_report()


def _build_legacy_research_page_context() -> dict[str, Any]:
    """Compatibility-only projection for legacy import and regression tests."""

    tasks = ExperimentStore(DB_PATH).list_research_tasks(limit=100)
    candidate_pool_path = DB_PATH.parent / 'research_candidate_pool.json'
    branch_plan_path = DB_PATH.parent / 'research_branch_plan.json'
    family_summary_path = DB_PATH.parent / 'family_summary.json'
    opportunities_path = DB_PATH.parent / 'research_opportunities.json'
    opportunity_store_path = DB_PATH.parent / 'research_opportunity_store.json'
    opportunity_review_path = DB_PATH.parent / 'opportunity_review.json'
    opportunity_metrics_path = DB_PATH.parent / 'opportunity_metrics.json'
    candidate_pool = _read_json_safely(candidate_pool_path, {}) or {}
    branch_plan = _read_json_safely(branch_plan_path, {}) or {}
    family_summary = _read_json_safely(family_summary_path, []) or []
    opportunities = _read_json_safely(opportunities_path, {}) or {}
    opportunity_store = _read_json_safely(opportunity_store_path, {}) or {}
    opportunity_review = build_opportunity_review() if opportunity_store_path.exists() else {}
    opportunity_metrics = build_opportunity_metrics() if opportunity_store_path.exists() else {}
    opportunity_archive_diagnostics = build_opportunity_archive_diagnostics() if opportunity_store_path.exists() else {}
    summary = {
        "pending": len([t for t in tasks if t["status"] == "pending"]),
        "running": len([t for t in tasks if t["status"] == "running"]),
        "finished": len([t for t in tasks if t["status"] == "finished"]),
        "failed": len([t for t in tasks if t["status"] == "failed"]),
        "baseline": len([t for t in tasks if (t.get("worker_note") or "").startswith("baseline")]),
        "validation": len([t for t in tasks if (t.get("worker_note") or "").startswith("validation")]),
        "exploration": len([t for t in tasks if (t.get("worker_note") or "").startswith("exploration")]),
        "retry": len([t for t in tasks if (t.get("worker_note") or "").startswith("retry")]),
        "planner_candidates": len(candidate_pool.get('tasks', []) or []),
        "planner_suppressed": len(candidate_pool.get('suppressed_tasks', []) or []),
    }
    for task in tasks:
        payload = task.get("payload") or {}
        knowledge_gain = [item for item in (payload.get("knowledge_gain") or []) if item]
        task["knowledge_gain_text"] = "、".join(knowledge_gain) if knowledge_gain else "-"
        if task["task_type"] in {"workflow", "batch"}:
            task["payload_summary"] = payload.get("config_path", "-")
        elif task["task_type"] == "generated_batch":
            task["payload_summary"] = payload.get("batch_path", "-")
            if "knowledge_gain=" in (task.get("worker_note") or "") and task["knowledge_gain_text"] == "-":
                task["knowledge_gain_text"] = (task.get("worker_note") or "").split("knowledge_gain=", 1)[-1]
        elif task["task_type"] == "diagnostic":
            task["payload_summary"] = f"{payload.get('diagnostic_type', '-')}: {'; '.join(payload.get('reasons', []))}"
        else:
            task["payload_summary"] = pretty_json_text(payload)
    return {
        "tasks": tasks,
        "summary": summary,
        "candidate_pool": candidate_pool,
        "branch_plan": branch_plan,
        "family_summary": family_summary,
        "opportunities": opportunities,
        "opportunity_store": opportunity_store,
        "opportunity_review": opportunity_review,
        "opportunity_metrics": opportunity_metrics,
        "opportunity_archive_diagnostics": opportunity_archive_diagnostics,
    }


@app.get("/research", response_class=HTMLResponse)
def research_page():
    return render(
        "research.html",
        title="研究",
        research_os=get_research_os_read_model(),
    )


@app.get("/research-quality", response_class=HTMLResponse)
def research_quality_page():
    summary = build_research_quality_summary()
    return render("research_quality.html", title="研究质量", summary=summary)


@app.get("/weekly", response_class=HTMLResponse)
def weekly_page():
    health = get_cached_health_metrics()
    weekly = compute_weekly_report(health)
    return render("weekly.html", title="周报", health=health, weekly=weekly)


@app.get("/cockpit", response_class=HTMLResponse)
def cockpit_page():
    return RedirectResponse(url="/", status_code=307)


def _legacy_cockpit_payload():
    """Retained temporarily as a compatibility reference for old cockpit queries."""
    base = DB_PATH.parent
    latest_run = fetch_one(
        "SELECT run_id, created_at_utc, config_path, status, output_dir FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1"
    )
    stable_candidates = fetch_all(
        "SELECT factor_name, candidate_runs FROM v_stable_candidates ORDER BY candidate_runs DESC, factor_name ASC LIMIT 10"
    )
    llm_status_path = base / "llm_status.json"
    llm_status = json.loads(llm_status_path.read_text(encoding="utf-8")) if llm_status_path.exists() else {}
    snapshot_path = base / "llm_input_snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8")) if snapshot_path.exists() else {}
    change_report_path = base / "change_report.md"
    paper_current_path = base / "paper_portfolio" / "current_portfolio.json"
    recommendation_context = snapshot.get("recommendation_context", {}) or {}
    plan_validation = (llm_status.get("plan_validation", {})) or {}
    research_flow_state_path = base / "research_flow_state.json"
    research_learning_path = base / "research_learning.json"
    research_flow_state = json.loads(research_flow_state_path.read_text(encoding="utf-8")) if research_flow_state_path.exists() else {}
    research_learning = json.loads(research_learning_path.read_text(encoding="utf-8")) if research_learning_path.exists() else {}
    analyst_feedback_context = snapshot.get("analyst_feedback_context", {}) or {}
    paper_portfolio = json.loads(paper_current_path.read_text(encoding="utf-8")) if paper_current_path.exists() else {}
    return render(
        "cockpit.html",
        title="驾驶舱",
        latest_run=latest_run,
        stable_candidates=stable_candidates,
        llm_status=llm_status,
        paper_stability=snapshot.get("paper_portfolio_stability", {}),
        portfolio_policy=plan_validation.get("portfolio_policy", {}),
        conservative_policy=snapshot.get("conservative_policy", {}),
        recommendation_context=recommendation_context,
        recommendation_context_text=pretty_json_text(recommendation_context, "暂无模板上下文。"),
        plan_validation=plan_validation,
        research_flow_state=research_flow_state,
        research_learning=research_learning,
        analyst_feedback_context=analyst_feedback_context,
        plan_validation_text=pretty_json_text(plan_validation, "暂无计划校验摘要。"),
        paper_portfolio=paper_portfolio,
        paper_portfolio_positions=portfolio_positions(paper_portfolio),
        paper_portfolio_text=pretty_json_text(paper_portfolio, "暂无纸面组合。"),
        change_report=change_report_path.read_text(encoding="utf-8") if change_report_path.exists() else "暂无变化报告。",
    )


@app.get("/exposure", response_class=HTMLResponse)
def exposure_page():
    promotion_scorecard = build_promotion_scorecard(DB_PATH, limit=12)
    exposure_rows = fetch_all(
        """
        SELECT factor_name, exposure_type, exposure_label, effective_bucket_label,
               strength_score, total_score, raw_rank_ic_mean, raw_rank_ic_ir,
               neutralized_rank_ic_mean, retention_industry, split_fail_count,
               crowding_peers, recommended_max_weight, turnover_daily,
               net_metric, status, updated_at_utc
        FROM exposure_factors
        WHERE run_id = (SELECT run_id FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1)
        ORDER BY COALESCE(total_score, -999) DESC, COALESCE(strength_score, -999) DESC, COALESCE(raw_rank_ic_mean, -999) DESC
        LIMIT 200
        """
    )
    return render(
        "exposure.html",
        title="Exposure Track",
        exposure_rows=exposure_rows,
        promotion_scorecard=promotion_scorecard,
        promotion_rows=promotion_scorecard.get('rows') or [],
    )


def _load_legacy_runs_page_context() -> dict[str, Any]:
    """Load old workflow runs for explicit migration/regression consumers only."""

    return {"runs": fetch_all(
        """
        SELECT run_id, created_at_utc, config_path, data_source, start_date, end_date,
               factor_count, dataset_rows, status, rerun_of_run_id
        FROM workflow_runs
        ORDER BY created_at_utc DESC
        LIMIT 100
        """
    )}


@app.get("/runs", response_class=HTMLResponse)
def runs_page():
    return render(
        "runs.html",
        title="运行记录",
        research_os=get_research_os_read_model(),
    )


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(run_id: str):
    run = fetch_one(
        "SELECT * FROM workflow_runs WHERE run_id = ?",
        (run_id,),
    )
    if not run:
        raise HTTPException(status_code=404, detail="未找到该运行记录")

    factors = fetch_all(
        """
        SELECT factor_name, variant, expression, rank_ic_mean, rank_ic_ir,
               top_bottom_spread_mean, pass_gate, fail_reason, score, split_fail_count, high_corr_peers_json
        FROM factor_results
        WHERE run_id = ?
        ORDER BY variant, score DESC, factor_name ASC
        """,
        (run_id,),
    )
    portfolios = fetch_all(
        "SELECT * FROM portfolio_results WHERE run_id = ? ORDER BY sharpe DESC",
        (run_id,),
    )
    artifacts = fetch_all(
        "SELECT artifact_name, artifact_path FROM run_artifacts WHERE run_id = ? ORDER BY artifact_name ASC",
        (run_id,),
    )
    return render(
        "run_detail.html",
        title=f"运行详情 {run_id[:8]}",
        run=run,
        factors=factors,
        portfolios=portfolios,
        artifacts=artifacts,
    )


@app.get("/candidates", response_class=HTMLResponse)
def candidates_page():
    candidates = ExperimentStore(DB_PATH).list_factor_candidates(limit=200)
    summary = {
        'promising': len([row for row in candidates if row['status'] == 'promising']),
        'testing': len([row for row in candidates if row['status'] == 'testing']),
        'fragile': len([row for row in candidates if row['status'] == 'fragile']),
        'rejected_archived': len([row for row in candidates if row['status'] in {'rejected', 'archived'}]),
    }
    return render('candidates.html', title='候选榜单', candidates=candidates, summary=summary)


@app.get("/candidates/{candidate_id}", response_class=HTMLResponse)
def candidate_detail_page(candidate_id: str):
    store = ExperimentStore(DB_PATH)
    candidate = store.get_factor_candidate(candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail='未找到该候选因子')
    evaluations = store.list_factor_evaluations(candidate_id=candidate_id, limit=200)
    hypothesis = store.get_hypothesis_for_candidate(candidate_id)
    research_thesis = store.get_research_thesis_for_candidate(candidate_id)
    detail_context = build_candidate_detail_context(store, candidate_id)
    candidate_risk_profile = next((row for row in store.list_candidate_risk_profiles(limit=5000) if row.get('candidate_id') == candidate_id), None)
    acceptance_snapshot = summarize_candidate_status(evaluations)
    candidate_failure_dossier = build_candidate_failure_dossier(
        candidate,
        evaluations,
        store.list_candidate_relationships(limit=5000),
        {row.get('name'): row for row in store.list_factor_candidates(limit=1000) if row.get('name')},
    )
    contribution_report_path = DB_PATH.parent / 'paper_portfolio' / 'portfolio_contribution_report.json'
    contribution_report = json.loads(contribution_report_path.read_text(encoding='utf-8')) if contribution_report_path.exists() else {}
    candidate_portfolio_contribution = next(
        (row for row in (contribution_report.get('rows') or []) if row.get('factor_name') == candidate.get('name')),
        None,
    )
    promotion_scorecard = build_promotion_scorecard(DB_PATH, limit=200)
    promotion_row = next((row for row in (promotion_scorecard.get('rows') or []) if row.get('factor_name') == candidate.get('name')), None)
    novelty_payload = load_novelty_judgments(DB_PATH.parent)
    novelty_row = next((row for row in (novelty_payload.get('rows') or []) if row.get('candidate_name') == candidate.get('name')), None)
    return render(
        'candidate_detail.html',
        title=f"候选详情 {candidate['name']}",
        candidate=candidate,
        evaluations=evaluations,
        hypothesis=hypothesis,
        candidate_risk_profile=candidate_risk_profile,
        acceptance_snapshot=acceptance_snapshot,
        candidate_failure_dossier=candidate_failure_dossier,
        research_thesis=research_thesis,
        candidate_portfolio_contribution=candidate_portfolio_contribution,
        promotion_row=promotion_row,
        novelty_row=novelty_row,
        **detail_context,
    )


@app.get("/families", response_class=HTMLResponse)
def families_page():
    store = ExperimentStore(DB_PATH)
    candidates = store.list_factor_candidates(limit=1000)
    evaluations = store.list_factor_evaluations(limit=5000)
    family_profiles_path = DB_PATH.parent / 'family_risk_profiles.json'
    families = _read_json_safely(family_profiles_path, []) if family_profiles_path.exists() else family_rollup(candidates, evaluations)
    if not isinstance(families, list):
        families = []
    summary = {
        'family_count': len(families),
        'promising_count': sum(int(row.get('promising_count') or 0) for row in families),
        'evaluation_count': sum(int(row.get('evaluation_count') or 0) for row in families),
        'continue_count': len([row for row in families if row.get('recommended_action') == 'continue']),
        'refine_count': len([row for row in families if row.get('recommended_action') == 'refine']),
        'pause_count': len([row for row in families if row.get('recommended_action') == 'pause']),
        'explore_count': len([row for row in families if row.get('recommended_action') == 'explore_new_branch']),
    }
    return render('families.html', title='候选 Family', families=families, summary=summary)


@app.get("/candidate-clusters", response_class=HTMLResponse)
def candidate_clusters_page():
    store = ExperimentStore(DB_PATH)
    candidates = store.list_factor_candidates(limit=1000)
    relationships = store.list_candidate_relationships(limit=5000)
    clusters = candidate_clusters(candidates, relationships)
    summary = {
        'cluster_count': len(clusters),
        'connected_candidate_count': sum(int(row['cluster_size']) for row in clusters),
        'relationship_count': len(relationships),
        'suppressed_candidate_count': sum(int(row.get('suppressed_member_count') or 0) for row in clusters),
    }
    return render('candidate_clusters.html', title='候选簇', clusters=clusters, summary=summary)


@app.get("/robustness", response_class=HTMLResponse)
def robustness_page():
    store = ExperimentStore(DB_PATH)
    profiles = store.list_candidate_risk_profiles(limit=500)
    checks = store.list_candidate_robustness_checks(limit=200)
    family_risks = fetch_all(
        "SELECT family, candidate_count, avg_risk_score, high_risk_count, medium_risk_count, low_risk_count, avg_robustness_score FROM v_candidate_family_risk_summary ORDER BY COALESCE(avg_risk_score, -999) DESC, family ASC"
    )
    family_risk_map = {}
    family_risk_profiles_path = DB_PATH.parent / 'family_risk_profiles.json'
    if family_risk_profiles_path.exists():
        for row in json.loads(family_risk_profiles_path.read_text(encoding='utf-8')):
            family_risk_map[row.get('family')] = row
    for row in family_risks:
        extra = family_risk_map.get(row.get('family')) or {}
        row['trial_pressure'] = extra.get('trial_pressure')
        row['false_positive_pressure'] = extra.get('false_positive_pressure')
        row['recommended_action'] = extra.get('recommended_action')
    candidate_name_by_id = {row['id']: row['name'] for row in store.list_factor_candidates(limit=1000)}
    avg_risk_score = round(sum(float(row.get('risk_score') or 0.0) for row in profiles) / max(len(profiles), 1), 6) if profiles else None
    summary = {
        'profile_count': len(profiles),
        'high_risk_count': len([row for row in profiles if row.get('risk_level') == 'high']),
        'avg_risk_score': avg_risk_score,
    }
    return render('robustness.html', title='稳健性', profiles=profiles, family_risks=family_risks, checks=checks, candidate_name_by_id=candidate_name_by_id, summary=summary)


@app.get("/factors", response_class=HTMLResponse)
def factors_page():
    factors = fetch_all(
        """
        SELECT
            s.factor_name,
            ROUND(s.avg_score, 6) AS avg_score,
            s.runs,
            COALESCE(c.candidate_runs, 0) AS candidate_runs,
            COALESCE(g.graveyard_runs, 0) AS graveyard_runs
        FROM v_factor_score_avg s
        LEFT JOIN v_stable_candidates c ON s.factor_name = c.factor_name
        LEFT JOIN (
            SELECT factor_name, COUNT(*) AS graveyard_runs
            FROM factor_results
            WHERE variant = 'graveyard'
            GROUP BY factor_name
        ) g ON s.factor_name = g.factor_name
        ORDER BY s.avg_score DESC
        """
    )
    return render("factors.html", title="因子", factors=factors)


def _load_legacy_portfolios_page_context() -> dict[str, Any]:
    """Load old portfolio aggregates for explicit numerical regression only."""

    strategies = fetch_all(
        "SELECT strategy_name, ROUND(avg_sharpe, 6) AS avg_sharpe, ROUND(avg_return, 6) AS avg_return, runs FROM v_portfolio_strategy_avg ORDER BY avg_sharpe DESC"
    )
    recent = fetch_all(
        """
        SELECT p.run_id, p.strategy_name, p.annual_return, p.sharpe, p.max_drawdown, p.avg_turnover
        FROM portfolio_results p
        JOIN workflow_runs w ON p.run_id = w.run_id
        ORDER BY w.created_at_utc DESC, p.sharpe DESC
        LIMIT 30
        """
    )
    return {"strategies": strategies, "recent": recent}


@app.get("/portfolios", response_class=HTMLResponse)
def portfolios_page():
    return render(
        "portfolios.html",
        title="组合",
        research_os=get_research_os_read_model(),
    )


@app.get("/paper-portfolio", response_class=HTMLResponse)
def paper_portfolio_page():
    base = DB_PATH.parent / "paper_portfolio"
    allocator_governance_audit = load_allocator_governance_audit(DB_PATH.parent)
    decision_ab = load_decision_ab_artifacts(DB_PATH.parent)
    diagnostician_enhancement = load_diagnostician_enhancement(DB_PATH.parent)
    current_path = base / "current_portfolio.json"
    history_path = base / "portfolio_history.json"
    change_log_path = base / "portfolio_change_log.md"
    retro_path = base / "portfolio_retrospective.json"
    stability_path = base / "portfolio_stability_score.json"
    current = _read_json_safely(current_path, {}) or {}
    retrospective = _read_json_safely(retro_path, {}) or {}
    stability = _read_json_safely(stability_path, {}) or {}
    contribution_path = base / "portfolio_contribution_report.json"
    contribution = _read_json_safely(contribution_path, {}) or {}
    approved_universe_path = DB_PATH.parent / "approved_candidate_universe.json"
    approved_universe = _read_json_safely(approved_universe_path, {}) or {}
    return render(
        "paper_portfolio.html",
        title="纸面组合",
        current=current,
        current_positions=portfolio_positions(current),
        retrospective=retrospective,
        stability=stability,
        contribution=contribution,
        approved_universe=approved_universe,
        allocator_governance_audit=allocator_governance_audit,
        decision_ab=decision_ab,
        diagnostician_enhancement=diagnostician_enhancement,
        history_text=history_path.read_text(encoding="utf-8") if history_path.exists() else "暂无组合历史。",
        change_log_text=change_log_path.read_text(encoding="utf-8") if change_log_path.exists() else "暂无组合变更日志。",
        retrospective_text=pretty_json_text(retrospective, "暂无组合回溯。"),
        stability_text=pretty_json_text(stability, "暂无稳定性评分。"),
    )


@app.get("/approved-universe", response_class=HTMLResponse)
def approved_universe_page():
    universe_path = DB_PATH.parent / "approved_candidate_universe.json"
    debug_path = DB_PATH.parent / "approved_candidate_universe_debug.json"
    universe = _read_json_safely(universe_path, {}) or {}
    debug = _read_json_safely(debug_path, {}) or {}
    allocator_governance_audit = load_allocator_governance_audit(DB_PATH.parent)
    decision_ab = load_decision_ab_artifacts(DB_PATH.parent)
    diagnostician_enhancement = load_diagnostician_enhancement(DB_PATH.parent)
    return render(
        "approved_universe.html",
        title="Approved Universe",
        universe=universe,
        debug=debug,
        allocator_governance_audit=allocator_governance_audit,
        decision_ab=decision_ab,
        diagnostician_enhancement=diagnostician_enhancement,
    )


@app.get("/llm", response_class=HTMLResponse)
def llm_page():
    base = DB_PATH.parent
    review_path = base / "llm_review.md"
    plan_path = base / "llm_next_batch_proposal.json"
    snapshot_path = base / "llm_input_snapshot.json"
    status_path = base / "llm_status.json"
    review_text = review_path.read_text(encoding="utf-8") if review_path.exists() else "暂无 LLM 评审。"
    plan_text = plan_path.read_text(encoding="utf-8") if plan_path.exists() else "暂无 LLM 计划。"
    snapshot_text = snapshot_path.read_text(encoding="utf-8") if snapshot_path.exists() else "暂无 LLM 输入快照。"
    status_text = status_path.read_text(encoding="utf-8") if status_path.exists() else "暂无 LLM 状态。"
    llm_status = json.loads(status_text) if status_path.exists() else {}
    llm_plan = json.loads(plan_text) if plan_path.exists() else {}
    snapshot = json.loads(snapshot_text) if snapshot_path.exists() else {}
    generated_batch_path = DB_PATH.parent / "generated_batch_from_llm.json"
    generated_batch_workflow_path = DB_PATH.parent / "generated_workflow_from_llm.json"
    feedback_path = DB_PATH.parent / "llm_plan_feedback.json"
    retrospective_path = DB_PATH.parent / "llm_retrospective.json"
    retrospective_md_path = DB_PATH.parent / "llm_retrospective.md"
    recommendation_history_path = DB_PATH.parent / "llm_recommendation_history.json"
    recommendation_weights_path = DB_PATH.parent / "llm_recommendation_weights.json"
    llm_diagnostics_path = DB_PATH.parent / "llm_diagnostics.json"
    recommendation_context = snapshot.get("recommendation_context", {}) or {}
    recommendation_history_tail = snapshot.get("recommendation_history_tail", []) or []
    plan_validation = llm_status.get("plan_validation", {}) or {}
    generated_batch = json.loads(generated_batch_path.read_text(encoding="utf-8")) if generated_batch_path.exists() else {}
    generated_workflow = json.loads(generated_batch_workflow_path.read_text(encoding="utf-8")) if generated_batch_workflow_path.exists() else {}
    recommendation_weights = json.loads(recommendation_weights_path.read_text(encoding="utf-8")) if recommendation_weights_path.exists() else {}
    llm_diagnostics = json.loads(llm_diagnostics_path.read_text(encoding="utf-8")) if llm_diagnostics_path.exists() else {}
    return render(
        "llm.html",
        title="LLM",
        review_text=review_text,
        plan_text=plan_text,
        snapshot_text=snapshot_text,
        status_text=status_text,
        llm_status=llm_status,
        llm_plan=llm_plan,
        snapshot=snapshot,
        generated_batch=generated_batch,
        generated_batch_text=pretty_json_text(generated_batch, "暂无生成的 batch。"),
        generated_workflow_text=pretty_json_text(generated_workflow, "暂无生成的 workflow。"),
        generated_feedback_text=feedback_path.read_text(encoding="utf-8") if feedback_path.exists() else "暂无 batch 执行反馈。",
        retrospective_text=retrospective_md_path.read_text(encoding="utf-8") if retrospective_md_path.exists() else "暂无建议效果回溯。",
        retrospective_json_text=retrospective_path.read_text(encoding="utf-8") if retrospective_path.exists() else "暂无建议效果回溯 JSON。",
        recommendation_history_text=recommendation_history_path.read_text(encoding="utf-8") if recommendation_history_path.exists() else "暂无建议历史。",
        recommendation_weights=recommendation_weights,
        recommendation_weights_text=pretty_json_text(recommendation_weights, "暂无建议权重。"),
        recommendation_history_tail=recommendation_history_tail,
        recommendation_history_tail_text=pretty_json_text(recommendation_history_tail, "暂无已注入 planner 的历史尾部。"),
        recommendation_context=recommendation_context,
        recommendation_context_text=pretty_json_text(recommendation_context, "暂无模板优先级摘要与疲劳度。"),
        plan_validation=plan_validation,
        plan_validation_text=pretty_json_text(plan_validation, "暂无计划校验结果。"),
    )


@app.get("/llm-usage", response_class=HTMLResponse)
def llm_usage_page():
    recent_rows = _latest_llm_usage_rows(limit=50)
    summary_rows = _load_llm_usage_rows(limit=1000, hours=24)
    return render(
        "llm_usage.html",
        title="LLM Token 用量",
        usage_summary=_summarize_llm_usage(summary_rows),
        usage_chart=_build_llm_usage_chart(summary_rows, hours=24),
        recent_rows=recent_rows,
        ledger_path=str(_llm_usage_ledger_path()),
        usage_generated_at=_format_local_time(),
        usage_timezone="Asia/Shanghai",
    )


@app.get("/ops", response_class=HTMLResponse)
def ops_page():
    tasks = latest_task_states(limit=20)
    research_tasks = ExperimentStore(DB_PATH).list_research_tasks(limit=20)
    return render("ops.html", title="操作", tasks=tasks, research_tasks=research_tasks, result=None)


@app.post("/ops/run/{target}", response_class=HTMLResponse)
def ops_run(target: str):
    mapping = {
        "workflow": "scripts/run_tushare_workflow.py",
        "batch": "scripts/run_tushare_batch.py",
        "orchestrator": "scripts/run_research_orchestrator.py",
        "queue-seed": "scripts/seed_research_queue.py",
        "cycle": "scripts/run_scheduled_cycle.py",
        "llm": "scripts/run_llm_cycle.py",
        "llm-plan-generate": "scripts/generate_batch_from_llm_plan.py",
        "llm-plan-run": "scripts/run_generated_batch_from_llm.py",
        "llm-retrospective": "scripts/build_llm_retrospective.py",
        "llm-memory": "scripts/build_recommendation_memory.py",
        "robustness-batch-build": "scripts/build_robustness_batch.py",
        "robustness-batch-run": "scripts/run_robustness_batch.py",
    }
    if target not in mapping:
        raise HTTPException(status_code=404, detail="未知操作目标")
    # Compatibility endpoint only: the scripts above write the retired
    # SQLite/JSON research state and therefore cannot be launched by the
    # authoritative WebUI.  Research and retry operations belong in Dagster.
    raise HTTPException(
        status_code=410,
        detail="旧操作入口已停用；请在 Dagster 中运行 Research OS 任务",
    )
