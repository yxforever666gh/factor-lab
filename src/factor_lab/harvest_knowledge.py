from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default
    except Exception:
        return default


def _append_section(path: Path, title: str, bullets: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    prior = path.read_text(encoding='utf-8') if path.exists() else f"# {path.stem.replace('_', ' ').title()}\n\n"
    body = f"## {title}\n" + "".join(f"- {b}\n" for b in bullets) + "\n"
    path.write_text(prior.rstrip() + "\n\n" + body, encoding='utf-8')


def build_knowledge_update(*, verdict: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    cycle_id = verdict.get('cycle_id') or evidence.get('cycle_id') or 'unknown_cycle'
    rows = evidence.get('evidence') or []
    blockers: list[dict[str, Any]] = []
    waste: list[dict[str, Any]] = []
    lessons: list[str] = []
    for row in rows:
        failure = row.get('failure_class') or row.get('information_gain_class') or row.get('information_gain')
        item = {
            'cycle_id': cycle_id,
            'experiment_id': row.get('experiment_id'),
            'mechanism_id': row.get('mechanism_id'),
            'failure_class': failure,
            'noted_at_utc': _utc(),
        }
        if failure in {'missing_required_fields', 'unsupported_feature_requested', 'blocked_missing_data', 'coverage_too_low'}:
            blockers.append(item)
        if failure in {'duplicate_equivalent_experiment', 'duplicate_or_low_information', 'overfit_suspected'}:
            waste.append(item)
        if row.get('mechanism_id') and row.get('information_gain') in {'positive_progress', 'negative_but_informative'}:
            lessons.append(f"{row.get('mechanism_id')}: {row.get('information_gain')} in {cycle_id} ({row.get('experiment_id')}).")
    conclusion = verdict.get('next_action') or verdict.get('decision') or verdict.get('verdict') or 'no durable action recorded'
    return {
        'schema_version': 1,
        'cycle_id': cycle_id,
        'updated_at_utc': _utc(),
        'durable_conclusion': str(conclusion),
        'verdict_decision': verdict.get('decision') or verdict.get('verdict'),
        'mechanism_lessons': lessons,
        'data_blockers': blockers,
        'research_waste': waste,
        'raw_results_stored_in_artifacts_only': True,
    }


def update_harvest_knowledge(*, root: str | Path = ROOT, cycle_id: str | None = None) -> dict[str, Any]:
    root = Path(root)
    base = root / 'artifacts/harvest_agent'
    if cycle_id is None:
        latest = _load_json(base / 'latest_cycle.json', {})
        cycle_id = latest.get('cycle_id') or 'cycle_0001'
    cycle_id = str(cycle_id)
    cdir = base / cycle_id
    verdict = _load_json(cdir / 'verdict.json', {'cycle_id': cycle_id, 'decision': 'hold_route', 'next_action': 'insufficient evidence'})
    evidence = _load_json(cdir / 'evidence_ledger.json', {'cycle_id': cycle_id, 'evidence': []})
    update = build_knowledge_update(verdict=verdict, evidence=evidence)

    kdir = root / 'knowledge'
    kdir.mkdir(parents=True, exist_ok=True)
    _append_section(kdir / 'harvest_agent.md', f"Cycle {cycle_id}", [
        f"Latest durable conclusion: {update['durable_conclusion']}",
        f"Verdict: {update.get('verdict_decision')}",
        "Detailed run records remain under artifacts/harvest_agent/ and are not copied into Hermes memory.",
    ])
    if update['mechanism_lessons']:
        _append_section(kdir / 'mechanism_lessons.md', f"Harvest {cycle_id}", update['mechanism_lessons'])
    else:
        _append_section(kdir / 'mechanism_lessons.md', f"Harvest {cycle_id}", ["No new durable mechanism lesson."])
    if update['research_waste']:
        _append_section(kdir / 'research_waste.md', f"Harvest {cycle_id}", [f"{w['experiment_id']}: {w['failure_class']}" for w in update['research_waste']])
    else:
        _append_section(kdir / 'research_waste.md', f"Harvest {cycle_id}", ["No duplicate/low-information waste recorded."])

    blockers_path = kdir / 'data_blockers.json'
    existing = _load_json(blockers_path, {'schema_version': 1})
    if not isinstance(existing, dict):
        existing = {'schema_version': 1, 'legacy_value': existing}
    harvest_value = existing.get('harvest_blockers', existing.get('blockers', []))
    harvest_blockers: list[dict[str, Any]] = harvest_value if isinstance(harvest_value, list) else []
    existing_keys = {(b.get('cycle_id'), b.get('experiment_id'), b.get('failure_class')) for b in harvest_blockers if isinstance(b, dict)}
    for b in update['data_blockers']:
        key = (b.get('cycle_id'), b.get('experiment_id'), b.get('failure_class'))
        if key not in existing_keys:
            harvest_blockers.append(b)
    existing['schema_version'] = int(existing.get('schema_version') or 1)
    existing['updated_at_utc'] = _utc()
    existing['harvest_blockers'] = harvest_blockers
    if 'blockers' not in existing:
        existing['blockers'] = harvest_blockers
    blockers_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

    cdir.mkdir(parents=True, exist_ok=True)
    (cdir / 'knowledge_update.json').write_text(json.dumps(update, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (cdir / 'knowledge_update.md').write_text('# Knowledge Update\n\n' + f"Cycle: {cycle_id}\nConclusion: {update['durable_conclusion']}\n", encoding='utf-8')
    return update
