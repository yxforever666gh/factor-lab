# Factor Lab × Hermes Phase 4 Memory Sync Implementation Plan

> **For Hermes:** Use `subagent-driven-development` skill to implement this plan task-by-task. This plan is intentionally external-orchestration first: Factor Lab remains independent; Hermes owns the memory/knowledge synchronization layer.

**Goal:** Connect Factor Lab experiment artifacts to Hermes' memory and knowledge capabilities through a controlled sync layer, forming a repeatable “experiment → summarize → persist knowledge → inform next research” loop.

**Architecture:** Factor Lab continues to produce artifacts under `/home/admin/factor-lab/artifacts/`. A Hermes-side script reads new artifacts, extracts compact experiment facts, writes detailed records to `/home/admin/factor-lab/knowledge/`, and only optionally writes high-confidence durable conclusions into Hermes memory. The first implementation must avoid tight coupling and must not automatically spam Hermes memory.

**Tech Stack:** Python 3 stdlib, Factor Lab JSON artifacts, local JSONL/Markdown knowledge files, Hermes skill scripts, optional Hermes `memory` tool usage by the supervising agent rather than direct script API calls.

---

## 0. Scope and Non-Goals

### In Scope

- Create a local Factor Lab knowledge base under `/home/admin/factor-lab/knowledge/`.
- Create a Hermes skill script:
  - `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`
- Scan Factor Lab artifacts for completed experiment result files.
- Extract compact factor-level metrics.
- Deduplicate already-synced artifacts.
- Generate/update:
  - `/home/admin/factor-lab/knowledge/factor_experiments.jsonl`
  - `/home/admin/factor-lab/knowledge/factor_lessons.md`
  - `/home/admin/factor-lab/knowledge/factor_watchlist.json`
  - `/home/admin/factor-lab/knowledge/factor_blacklist.json`
  - `/home/admin/factor-lab/knowledge/memory_sync_state.json`
- Add dry-run mode and explicit `--write` mode.
- Update the `factor-lab` Hermes skill with the new workflow.

### Out of Scope for First Version

- Direct modification of Factor Lab daemon internals.
- Automatic unconditional writes into Hermes memory.
- Uploading experiment records to external services.
- Building a vector database.
- Replacing Factor Lab's existing planner/agent architecture.
- Full semantic clustering of factor families.

### Safety Rule

The first version must default to **read-only/dry-run** behavior unless `--write` is explicitly provided.

---

## 1. Target End State

After implementation, the normal workflow should be:

```text
Factor Lab workflow/daemon produces artifacts
  ↓
Hermes runs sync_memory.py --write
  ↓
New experiment facts are appended to factor_experiments.jsonl
  ↓
factor_lessons.md is regenerated/updated
  ↓
watchlist/blacklist files are updated
  ↓
Hermes uses these knowledge files before proposing the next factor batch
  ↓
Only stable, important conclusions are manually or explicitly written to Hermes memory
```

---

## 2. File Layout

Create or modify the following files:

```text
/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py
/home/admin/.hermes/skills/factor-lab/SKILL.md
/home/admin/factor-lab/knowledge/.gitkeep
/home/admin/factor-lab/knowledge/factor_experiments.jsonl
/home/admin/factor-lab/knowledge/factor_lessons.md
/home/admin/factor-lab/knowledge/factor_watchlist.json
/home/admin/factor-lab/knowledge/factor_blacklist.json
/home/admin/factor-lab/knowledge/memory_sync_state.json
/home/admin/factor-lab/tests/test_memory_sync.py
```

If the test suite layout differs, put tests under the closest existing tests directory and keep the test filename `test_memory_sync.py`.

---

## 3. Data Contract

### 3.1 Experiment Record Schema

Each line in `factor_experiments.jsonl` should be a JSON object:

```json
{
  "artifact_path": "/home/admin/factor-lab/artifacts/example_output/results.json",
  "artifact_sha256": "...",
  "synced_at": "2026-04-29T00:00:00Z",
  "run_id": "example_output",
  "factor_name": "turnover_shock_5_20",
  "expression": "turnover_shock_5_20",
  "rank_ic_mean": 0.000155,
  "rank_ic_ir": 0.000731,
  "top_bottom_spread_mean": -0.002741,
  "sharpe_net": -0.4945,
  "net_return_annual": -2.417539,
  "pass_gate": false,
  "fail_reason": "rank_ic_mean<0.03; top_bottom_spread<0.0; sharpe_net<1.0",
  "classification": "weak_signal_negative_spread",
  "lesson": "turnover_shock_5_20 had weak IC and negative spread; test reversed version and historical windows before discarding."
}
```

### 3.2 Sync State Schema

`memory_sync_state.json`:

```json
{
  "version": 1,
  "synced_files": {
    "/home/admin/factor-lab/artifacts/example_output/results.json": {
      "sha256": "...",
      "synced_at": "2026-04-29T00:00:00Z",
      "record_count": 1
    }
  }
}
```

### 3.3 Watchlist Schema

`factor_watchlist.json`:

```json
{
  "updated_at": "2026-04-29T00:00:00Z",
  "factors": [
    {
      "factor_name": "turnover_shock_5_20",
      "reason": "Best among failed factors by sharpe_net; worth reversed/historical retest.",
      "suggested_next_tests": [
        "reverse",
        "2020-2023 window",
        "larger universe 50-100 stocks"
      ]
    }
  ]
}
```

### 3.4 Blacklist Schema

`factor_blacklist.json`:

```json
{
  "updated_at": "2026-04-29T00:00:00Z",
  "factors": [
    {
      "factor_name": "earnings_yield",
      "reason": "Repeatedly very negative sharpe_net in current window; do not retest unchanged unless data window changes."
    }
  ]
}
```

---

## 4. Implementation Tasks

### Task 1: Create Knowledge Directory Skeleton

**Objective:** Establish a stable local knowledge base location for Factor Lab experiment memory.

**Files:**
- Create: `/home/admin/factor-lab/knowledge/.gitkeep`
- Create: `/home/admin/factor-lab/knowledge/factor_experiments.jsonl`
- Create: `/home/admin/factor-lab/knowledge/factor_lessons.md`
- Create: `/home/admin/factor-lab/knowledge/factor_watchlist.json`
- Create: `/home/admin/factor-lab/knowledge/factor_blacklist.json`
- Create: `/home/admin/factor-lab/knowledge/memory_sync_state.json`

**Step 1: Create directory and starter files**

Use Python or file tools to create:

```text
/home/admin/factor-lab/knowledge/
```

Initial `factor_lessons.md`:

```markdown
# Factor Lab Knowledge Lessons

This file is generated/updated by Hermes memory sync.

## Current High-Level Lessons

_No synced experiments yet._
```

Initial `factor_watchlist.json`:

```json
{
  "updated_at": null,
  "factors": []
}
```

Initial `factor_blacklist.json`:

```json
{
  "updated_at": null,
  "factors": []
}
```

Initial `memory_sync_state.json`:

```json
{
  "version": 1,
  "synced_files": {}
}
```

**Step 2: Verify**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
base = Path('/home/admin/factor-lab/knowledge')
required = [
    '.gitkeep',
    'factor_experiments.jsonl',
    'factor_lessons.md',
    'factor_watchlist.json',
    'factor_blacklist.json',
    'memory_sync_state.json',
]
missing = [p for p in required if not (base / p).exists()]
assert not missing, missing
print('knowledge skeleton ok')
PY
```

Expected:

```text
knowledge skeleton ok
```

---

### Task 2: Add Unit Tests for Result Parsing

**Objective:** Define expected behavior before implementing the sync script.

**Files:**
- Create: `/home/admin/factor-lab/tests/test_memory_sync.py`
- Create/Modify later: `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`

**Step 1: Write failing tests**

Add tests that import the script by path:

```python
import importlib.util
import json
from pathlib import Path

SCRIPT = Path('/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py')


def load_module():
    spec = importlib.util.spec_from_file_location('sync_memory', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_results_array_format(tmp_path):
    module = load_module()
    result_file = tmp_path / 'results.json'
    result_file.write_text(json.dumps([
        {
            'factor_name': 'turnover_shock_5_20',
            'expression': 'turnover_shock_5_20',
            'rank_ic_mean': 0.000155,
            'rank_ic_ir': 0.000731,
            'top_bottom_spread_mean': -0.002741,
            'sharpe_net': -0.4945,
            'net_return_annual': -2.417539,
            'pass_gate': False,
            'fail_reason': 'rank_ic_mean<0.03; top_bottom_spread<0.0; sharpe_net<1.0'
        }
    ]))

    records = module.parse_result_file(result_file)

    assert len(records) == 1
    assert records[0]['factor_name'] == 'turnover_shock_5_20'
    assert records[0]['classification'] == 'weak_signal_negative_spread'
    assert 'reverse' in records[0]['lesson'].lower() or 'reversed' in records[0]['lesson'].lower()


def test_parse_results_dict_factor_evaluations_format(tmp_path):
    module = load_module()
    result_file = tmp_path / 'results.json'
    result_file.write_text(json.dumps({
        'factor_evaluations': [
            {
                'factor_name': 'earnings_yield',
                'expression': 'earnings_yield',
                'rank_ic_mean': 0.004147,
                'rank_ic_ir': 0.019538,
                'top_bottom_spread_mean': -0.005905,
                'sharpe_net': -6.2586,
                'net_return_annual': -3.21497,
                'pass_gate': False,
                'fail_reason': 'rank_ic_mean<0.02; top_bottom_spread<0.0005; sharpe_net<1.0'
            }
        ]
    }))

    records = module.parse_result_file(result_file)

    assert len(records) == 1
    assert records[0]['factor_name'] == 'earnings_yield'
    assert records[0]['classification'] in {'weak_signal_negative_spread', 'reject_candidate'}
```

**Step 2: Run tests to verify failure**

Run:

```bash
cd /home/admin/factor-lab && python3 -m pytest tests/test_memory_sync.py -v
```

Expected before implementation:

```text
FAILED ... sync_memory.py not found
```

---

### Task 3: Implement Core Parser in sync_memory.py

**Objective:** Parse Factor Lab result files into normalized experiment records.

**Files:**
- Create: `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`

**Step 1: Implement minimal parser**

Create the script with these functions:

```python
#!/usr/bin/env python3
"""Sync Factor Lab experiment artifacts into a local Hermes-readable knowledge base."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path('/home/admin/factor-lab')
ARTIFACTS_DIR = PROJECT_ROOT / 'artifacts'
KNOWLEDGE_DIR = PROJECT_ROOT / 'knowledge'
EXPERIMENTS_FILE = KNOWLEDGE_DIR / 'factor_experiments.jsonl'
LESSONS_FILE = KNOWLEDGE_DIR / 'factor_lessons.md'
WATCHLIST_FILE = KNOWLEDGE_DIR / 'factor_watchlist.json'
BLACKLIST_FILE = KNOWLEDGE_DIR / 'factor_blacklist.json'
STATE_FILE = KNOWLEDGE_DIR / 'memory_sync_state.json'


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def classify_record(raw: dict[str, Any]) -> str:
    pass_gate = bool(raw.get('pass_gate'))
    rank_ic = raw.get('rank_ic_mean')
    spread = raw.get('top_bottom_spread_mean')
    sharpe = raw.get('sharpe_net')

    if pass_gate:
        return 'pass_candidate'
    if isinstance(spread, (int, float)) and spread < 0:
        return 'weak_signal_negative_spread'
    if isinstance(rank_ic, (int, float)) and abs(rank_ic) < 0.005 and isinstance(sharpe, (int, float)) and sharpe < 0:
        return 'weak_signal'
    if isinstance(sharpe, (int, float)) and sharpe < -3:
        return 'reject_candidate'
    return 'needs_review'


def build_lesson(raw: dict[str, Any], classification: str) -> str:
    name = raw.get('factor_name', 'unknown_factor')
    if classification == 'pass_candidate':
        return f'{name} passed current gates; add to watchlist and validate on other windows.'
    if classification == 'weak_signal_negative_spread':
        return f'{name} had weak signal and negative long-short spread; test reversed version and historical windows before discarding.'
    if classification == 'reject_candidate':
        return f'{name} performed very poorly in the current window; avoid unchanged retests unless data window or universe changes.'
    if classification == 'weak_signal':
        return f'{name} had near-zero IC with negative net Sharpe; deprioritize unless combined with other factors.'
    return f'{name} needs manual review before reuse.'


def _extract_evaluations(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ('factor_evaluations', 'results', 'factors'):
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        if 'factor_name' in data:
            return [data]
    return []


def parse_result_file(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text())
    evaluations = _extract_evaluations(data)
    records: list[dict[str, Any]] = []
    for raw in evaluations:
        classification = classify_record(raw)
        normalized = {
            'factor_name': raw.get('factor_name') or raw.get('name'),
            'expression': raw.get('expression'),
            'rank_ic_mean': raw.get('rank_ic_mean'),
            'rank_ic_ir': raw.get('rank_ic_ir') or raw.get('information_ratio'),
            'top_bottom_spread_mean': raw.get('top_bottom_spread_mean'),
            'sharpe_net': raw.get('sharpe_net'),
            'net_return_annual': raw.get('net_return_annual'),
            'pass_gate': raw.get('pass_gate'),
            'fail_reason': raw.get('fail_reason'),
            'classification': classification,
            'lesson': build_lesson(raw, classification),
        }
        if normalized['factor_name']:
            records.append(normalized)
    return records
```

**Step 2: Run parser tests**

Run:

```bash
cd /home/admin/factor-lab && python3 -m pytest tests/test_memory_sync.py -v
```

Expected:

```text
2 passed
```

---

### Task 4: Implement Artifact Discovery and Deduplication

**Objective:** Find unsynced result files without repeatedly importing the same artifact.

**Files:**
- Modify: `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`
- Modify tests: `/home/admin/factor-lab/tests/test_memory_sync.py`

**Step 1: Add tests for discovery/dedup**

Add:

```python
def test_discover_result_files_skips_synced(tmp_path):
    module = load_module()
    artifacts = tmp_path / 'artifacts'
    out = artifacts / 'run1'
    out.mkdir(parents=True)
    result_file = out / 'results.json'
    result_file.write_text('[]')
    digest = module.sha256_file(result_file)
    state = {'version': 1, 'synced_files': {str(result_file): {'sha256': digest}}}

    files = module.discover_result_files(artifacts, state)

    assert files == []
```

**Step 2: Implement discovery**

Add:

```python
def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {'version': 1, 'synced_files': {}}
    return json.loads(STATE_FILE.read_text())


def save_state(state: dict[str, Any]) -> None:
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False) + '\n')


def discover_result_files(artifacts_dir: Path, state: dict[str, Any]) -> list[Path]:
    candidates = sorted(set(artifacts_dir.glob('**/results.json')) | set(artifacts_dir.glob('**/factor_evaluations.json')))
    unsynced: list[Path] = []
    synced = state.get('synced_files', {})
    for path in candidates:
        if not path.is_file():
            continue
        digest = sha256_file(path)
        existing = synced.get(str(path))
        if existing and existing.get('sha256') == digest:
            continue
        unsynced.append(path)
    return unsynced
```

**Step 3: Run tests**

Run:

```bash
cd /home/admin/factor-lab && python3 -m pytest tests/test_memory_sync.py -v
```

Expected:

```text
3 passed
```

---

### Task 5: Implement JSONL Append and Lessons Generation

**Objective:** Persist detailed experiment records locally and generate a human-readable lesson file.

**Files:**
- Modify: `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`
- Modify tests: `/home/admin/factor-lab/tests/test_memory_sync.py`

**Step 1: Add tests**

Add:

```python
def test_build_lessons_markdown_contains_factor_name():
    module = load_module()
    records = [
        {
            'factor_name': 'turnover_shock_5_20',
            'classification': 'weak_signal_negative_spread',
            'sharpe_net': -0.4945,
            'rank_ic_mean': 0.000155,
            'lesson': 'test reversed version'
        }
    ]
    md = module.build_lessons_markdown(records)
    assert 'turnover_shock_5_20' in md
    assert 'test reversed version' in md
```

**Step 2: Implement persistence helpers**

Add:

```python
def append_experiment_records(records: list[dict[str, Any]]) -> None:
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)
    with EXPERIMENTS_FILE.open('a', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + '\n')


def load_all_experiment_records() -> list[dict[str, Any]]:
    if not EXPERIMENTS_FILE.exists():
        return []
    records = []
    for line in EXPERIMENTS_FILE.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def build_lessons_markdown(records: list[dict[str, Any]]) -> str:
    lines = [
        '# Factor Lab Knowledge Lessons',
        '',
        'This file is generated/updated by Hermes memory sync.',
        '',
        '## Current High-Level Lessons',
        '',
    ]
    if not records:
        lines.append('_No synced experiments yet._')
        return '\n'.join(lines) + '\n'

    sorted_records = sorted(
        records,
        key=lambda r: (r.get('pass_gate') is True, r.get('sharpe_net') if isinstance(r.get('sharpe_net'), (int, float)) else -999),
        reverse=True,
    )
    for record in sorted_records[:30]:
        lines.append(
            f"- `{record.get('factor_name')}`: "
            f"classification=`{record.get('classification')}`, "
            f"rank_ic={record.get('rank_ic_mean')}, "
            f"sharpe_net={record.get('sharpe_net')}. "
            f"Lesson: {record.get('lesson')}"
        )
    return '\n'.join(lines) + '\n'


def write_lessons(records: list[dict[str, Any]]) -> None:
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)
    LESSONS_FILE.write_text(build_lessons_markdown(records), encoding='utf-8')
```

**Step 3: Run tests**

Run:

```bash
cd /home/admin/factor-lab && python3 -m pytest tests/test_memory_sync.py -v
```

Expected:

```text
4 passed
```

---

### Task 6: Implement Watchlist and Blacklist Generation

**Objective:** Produce machine-readable recommendations for future Factor Lab research planning.

**Files:**
- Modify: `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`
- Modify tests: `/home/admin/factor-lab/tests/test_memory_sync.py`

**Step 1: Add tests**

Add:

```python
def test_watchlist_prefers_best_failed_factor():
    module = load_module()
    records = [
        {'factor_name': 'earnings_yield', 'classification': 'reject_candidate', 'sharpe_net': -6.25, 'pass_gate': False},
        {'factor_name': 'turnover_shock_5_20', 'classification': 'weak_signal_negative_spread', 'sharpe_net': -0.49, 'pass_gate': False},
    ]
    watch = module.build_watchlist(records, now='NOW')
    names = [x['factor_name'] for x in watch['factors']]
    assert 'turnover_shock_5_20' in names
```

**Step 2: Implement builders**

Add:

```python
def build_watchlist(records: list[dict[str, Any]], now: str | None = None) -> dict[str, Any]:
    now = now or utc_now()
    candidates = []
    for record in records:
        cls = record.get('classification')
        if record.get('pass_gate') is True or cls == 'weak_signal_negative_spread':
            candidates.append({
                'factor_name': record.get('factor_name'),
                'reason': record.get('lesson'),
                'sharpe_net': record.get('sharpe_net'),
                'rank_ic_mean': record.get('rank_ic_mean'),
                'suggested_next_tests': ['reverse', '2020-2023 window', 'larger universe 50-100 stocks'],
            })
    candidates = sorted(candidates, key=lambda x: x.get('sharpe_net') if isinstance(x.get('sharpe_net'), (int, float)) else -999, reverse=True)
    return {'updated_at': now, 'factors': candidates[:20]}


def build_blacklist(records: list[dict[str, Any]], now: str | None = None) -> dict[str, Any]:
    now = now or utc_now()
    rejected = []
    for record in records:
        if record.get('classification') == 'reject_candidate':
            rejected.append({
                'factor_name': record.get('factor_name'),
                'reason': record.get('lesson'),
                'sharpe_net': record.get('sharpe_net'),
                'rank_ic_mean': record.get('rank_ic_mean'),
            })
    return {'updated_at': now, 'factors': rejected[:50]}


def write_watchlist_and_blacklist(records: list[dict[str, Any]]) -> None:
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)
    WATCHLIST_FILE.write_text(json.dumps(build_watchlist(records), indent=2, ensure_ascii=False) + '\n')
    BLACKLIST_FILE.write_text(json.dumps(build_blacklist(records), indent=2, ensure_ascii=False) + '\n')
```

**Step 3: Run tests**

Run:

```bash
cd /home/admin/factor-lab && python3 -m pytest tests/test_memory_sync.py -v
```

Expected:

```text
5 passed
```

---

### Task 7: Implement CLI Dry-Run and Write Modes

**Objective:** Make sync script executable by Hermes with safe defaults.

**Files:**
- Modify: `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`

**Step 1: Add CLI**

Add:

```python
def enrich_records(path: Path, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    digest = sha256_file(path)
    now = utc_now()
    run_id = path.parent.name
    enriched = []
    for record in records:
        item = dict(record)
        item.update({
            'artifact_path': str(path),
            'artifact_sha256': digest,
            'synced_at': now,
            'run_id': run_id,
        })
        enriched.append(item)
    return enriched


def sync(write: bool = False, latest: bool = False) -> dict[str, Any]:
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state()
    files = discover_result_files(ARTIFACTS_DIR, state)
    if latest and files:
        files = [max(files, key=lambda p: p.stat().st_mtime)]

    new_records: list[dict[str, Any]] = []
    file_summaries = []
    for path in files:
        parsed = parse_result_file(path)
        enriched = enrich_records(path, parsed)
        new_records.extend(enriched)
        file_summaries.append({'path': str(path), 'records': len(enriched)})
        if write:
            state.setdefault('synced_files', {})[str(path)] = {
                'sha256': sha256_file(path),
                'synced_at': utc_now(),
                'record_count': len(enriched),
            }

    if write and new_records:
        append_experiment_records(new_records)
        all_records = load_all_experiment_records()
        write_lessons(all_records)
        write_watchlist_and_blacklist(all_records)
        save_state(state)

    return {
        'write': write,
        'latest': latest,
        'files_seen': len(files),
        'records_extracted': len(new_records),
        'files': file_summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Sync Factor Lab artifacts into local knowledge base.')
    parser.add_argument('--write', action='store_true', help='Actually write knowledge files and sync state. Default is dry-run.')
    parser.add_argument('--latest', action='store_true', help='Only process the newest unsynced result file.')
    args = parser.parse_args()

    result = sync(write=args.write, latest=args.latest)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
```

**Step 2: Verify dry-run**

Run:

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py
```

Expected:

```json
{
  "write": false,
  "latest": false,
  "files_seen": <number>,
  "records_extracted": <number>,
  "files": [...]
}
```

**Step 3: Verify write mode**

Run:

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py --write
```

Expected:

- `factor_experiments.jsonl` has appended records.
- `factor_lessons.md` contains factor names and lessons.
- `memory_sync_state.json` contains synced file entries.

---

### Task 8: Add Optional Hermes Memory Candidate Output

**Objective:** Produce a short candidate memory text without automatically writing it.

**Files:**
- Modify: `/home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py`

**Step 1: Add memory candidate builder**

Add:

```python
def build_hermes_memory_candidate(records: list[dict[str, Any]]) -> str | None:
    if not records:
        return None
    watch = build_watchlist(records)
    blacklist = build_blacklist(records)
    best = watch.get('factors', [])[:1]
    rejected_count = len(blacklist.get('factors', []))
    if not best and rejected_count == 0:
        return None
    parts = []
    if best:
        b = best[0]
        parts.append(f"Factor Lab latest synced experiments: `{b.get('factor_name')}` is the best watchlist candidate so far; suggested next tests include reverse, 2020-2023 window, and larger universe 50-100 stocks.")
    if rejected_count:
        parts.append(f"{rejected_count} factors are reject candidates in the current synced knowledge base; avoid unchanged retests unless data window/universe changes.")
    return ' '.join(parts)
```

**Step 2: Add CLI flag**

Add:

```python
parser.add_argument('--memory-candidate', action='store_true', help='Print a compact Hermes memory candidate text.')
```

If flag is set, include:

```python
if args.memory_candidate:
    all_records = load_all_experiment_records()
    candidate = build_hermes_memory_candidate(all_records)
    result['hermes_memory_candidate'] = candidate
```

**Step 3: Verify**

Run:

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py --memory-candidate
```

Expected:

- Prints JSON with `hermes_memory_candidate` field.
- Does **not** write to Hermes memory automatically.

---

### Task 9: Update Hermes factor-lab Skill Documentation

**Objective:** Teach future Hermes sessions to use the new knowledge sync layer.

**Files:**
- Modify: `/home/admin/.hermes/skills/factor-lab/SKILL.md`

**Step 1: Add a new section**

Add after “记忆积累”:

```markdown
## Phase 4: Memory / Knowledge Sync

Factor Lab experiment artifacts should not be dumped directly into Hermes memory. Use the local knowledge sync layer first.

### Sync latest experiment knowledge

Dry-run:
```bash
python3 ~/.hermes/skills/factor-lab/scripts/sync_memory.py --latest
```

Write local knowledge files:
```bash
python3 ~/.hermes/skills/factor-lab/scripts/sync_memory.py --write
```

Print compact Hermes memory candidate:
```bash
python3 ~/.hermes/skills/factor-lab/scripts/sync_memory.py --memory-candidate
```

### Knowledge files

- `/home/admin/factor-lab/knowledge/factor_experiments.jsonl` — detailed experiment records
- `/home/admin/factor-lab/knowledge/factor_lessons.md` — human-readable lessons
- `/home/admin/factor-lab/knowledge/factor_watchlist.json` — next candidates to test
- `/home/admin/factor-lab/knowledge/factor_blacklist.json` — factors to avoid unchanged
- `/home/admin/factor-lab/knowledge/memory_sync_state.json` — deduplication state

### Rule

Before proposing a new Factor Lab research batch, read:

```text
/home/admin/factor-lab/knowledge/factor_lessons.md
/home/admin/factor-lab/knowledge/factor_watchlist.json
/home/admin/factor-lab/knowledge/factor_blacklist.json
```

Only write Hermes memory when a conclusion is stable, compact, and useful across future sessions.
```

**Step 2: Verify skill loads**

Run a Hermes `skill_view('factor-lab')` check or inspect the file.

Expected:

- New Phase 4 section is present.

---

### Task 10: Run First Real Sync on Existing Artifacts

**Objective:** Validate the sync layer against current Factor Lab artifacts.

**Files:**
- Reads: `/home/admin/factor-lab/artifacts/**/results.json`
- Writes: `/home/admin/factor-lab/knowledge/*`

**Step 1: Dry-run**

Run:

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py
```

Expected:

- Reports discovered files and extracted records.
- Does not change sync state.

**Step 2: Write sync**

Run:

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py --write
```

Expected:

- Writes experiment records.
- Updates lessons, watchlist, blacklist, state.

**Step 3: Inspect outputs**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
base = Path('/home/admin/factor-lab/knowledge')
for name in ['factor_experiments.jsonl', 'factor_lessons.md', 'factor_watchlist.json', 'factor_blacklist.json', 'memory_sync_state.json']:
    p = base / name
    print(name, p.exists(), p.stat().st_size if p.exists() else 0)
PY
```

Expected:

- All files exist.
- Size > 0 for generated files.

---

### Task 11: Add Pre-Research Checklist to Future Workflow

**Objective:** Ensure the knowledge base influences the next factor research batch.

**Files:**
- Modify: `/home/admin/.hermes/skills/factor-lab/SKILL.md`

**Step 1: Add checklist**

Add to the skill:

```markdown
## Before Starting a New Factor Batch

1. Run or check memory sync:
   ```bash
   python3 ~/.hermes/skills/factor-lab/scripts/sync_memory.py --latest --write
   ```
2. Read:
   - `/home/admin/factor-lab/knowledge/factor_lessons.md`
   - `/home/admin/factor-lab/knowledge/factor_watchlist.json`
   - `/home/admin/factor-lab/knowledge/factor_blacklist.json`
3. Avoid unchanged retests of blacklist factors.
4. Prefer watchlist next tests, especially:
   - reversed versions when spread is negative
   - 2020-2023 window
   - larger universe 50-100 stocks
5. After completing the batch, run sync again.
```

**Step 2: Verification**

Ask Hermes to answer: “下一批 Factor Lab 因子怎么选？” and confirm it cites the local knowledge files before proposing factors.

---

## 5. Acceptance Criteria

Phase 4 is considered complete when all items below are true:

- [ ] `/home/admin/factor-lab/knowledge/` exists with the expected files.
- [ ] `sync_memory.py` exists and supports:
  - [ ] default dry-run
  - [ ] `--write`
  - [ ] `--latest`
  - [ ] `--memory-candidate`
- [ ] Unit tests pass:
  ```bash
  cd /home/admin/factor-lab && python3 -m pytest tests/test_memory_sync.py -v
  ```
- [ ] Running dry-run does not modify sync state.
- [ ] Running `--write` appends experiment records and updates state.
- [ ] Duplicate runs do not append duplicate records for unchanged artifact files.
- [ ] `factor_lessons.md` contains human-readable lessons from real artifacts.
- [ ] `factor_watchlist.json` recommends next tests.
- [ ] `factor_blacklist.json` lists reject candidates.
- [ ] `factor-lab` Hermes skill documents the new memory sync workflow.
- [ ] Future research planning explicitly reads the knowledge files before proposing new factors.

---

## 6. Verification Commands

Run all of these before declaring completion:

```bash
cd /home/admin/factor-lab && python3 -m pytest tests/test_memory_sync.py -v
```

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py
```

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py --write
```

```bash
python3 /home/admin/.hermes/skills/factor-lab/scripts/sync_memory.py --memory-candidate
```

```bash
python3 - <<'PY'
from pathlib import Path
base = Path('/home/admin/factor-lab/knowledge')
required = [
    'factor_experiments.jsonl',
    'factor_lessons.md',
    'factor_watchlist.json',
    'factor_blacklist.json',
    'memory_sync_state.json',
]
for name in required:
    p = base / name
    assert p.exists(), name
    print(name, p.stat().st_size)
print('phase4 memory sync verification ok')
PY
```

Expected final output includes:

```text
phase4 memory sync verification ok
```

---

## 7. Rollback Plan

If sync output is wrong:

1. Stop using `--write`.
2. Preserve bad files for inspection by moving them to:

```text
/home/admin/factor-lab/knowledge/bad-sync-archive-YYYYMMDD-HHMMSS/
```

3. Reset state file to:

```json
{
  "version": 1,
  "synced_files": {}
}
```

4. Fix parser/tests.
5. Re-run dry-run before write mode.

No Factor Lab runtime or daemon code should need rollback because this plan keeps sync external.

---

## 8. Future Enhancements After MVP

Only consider these after the acceptance criteria pass:

1. Add cron or heartbeat automation:
   - Periodically run `sync_memory.py --latest --write`.
2. Add stricter family-level aggregation:
   - momentum, value, liquidity, quality, reversal families.
3. Add configurable thresholds:
   - watchlist minimum Sharpe, blacklist minimum repeated failure count.
4. Add a WebUI knowledge page.
5. Add direct integration with Factor Lab daemon completion events.
6. Add automatic generation of next-batch configs from watchlist.

---

## 9. Recommended Execution Order

Implement in this order:

1. Task 1 — skeleton
2. Task 2 — parser tests
3. Task 3 — parser implementation
4. Task 4 — discovery/dedup
5. Task 5 — local persistence and lessons
6. Task 6 — watchlist/blacklist
7. Task 7 — CLI modes
8. Task 8 — memory candidate output
9. Task 9 — skill documentation
10. Task 10 — real sync
11. Task 11 — pre-research checklist

Commit after each successful task if working inside git:

```bash
git add <changed-files>
git commit -m "feat: add Factor Lab Hermes memory sync <task-name>"
```

---

## 10. Key Design Decision

Do **not** treat Hermes memory as the primary experiment database.

Use this split:

```text
Detailed experiment records → /home/admin/factor-lab/knowledge/factor_experiments.jsonl
Human-readable lessons      → /home/admin/factor-lab/knowledge/factor_lessons.md
Next-test candidates        → /home/admin/factor-lab/knowledge/factor_watchlist.json
Avoid unchanged retests     → /home/admin/factor-lab/knowledge/factor_blacklist.json
Durable global conclusions  → Hermes memory, only after explicit review or high confidence
```

This preserves Hermes memory quality while still giving Factor Lab a long-term learning loop.
