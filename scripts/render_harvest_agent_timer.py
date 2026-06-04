#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def render_timer_units(*, allow_controlled_execution: bool = False, max_experiments: int = 1, interval: str = '6h', root: str | Path = ROOT) -> dict[str, str]:
    root = Path(root)
    max_experiments = 1 if allow_controlled_execution else max(0, int(max_experiments))
    flags = ' --dry-run'
    if allow_controlled_execution:
        flags = f' --allow-controlled-execution --max-experiments {max_experiments}'
    service = (
        '[Unit]\nDescription=Factor Lab Harvest Agent one safe cycle (preview only)\n\n'
        '[Service]\nType=oneshot\n'
        f'WorkingDirectory={root}\n'
        'Environment=PYTHONPATH=src\n'
        f'ExecStart={root}/.venv/bin/python {root}/scripts/run_harvest_agent_once.py{flags}\n'
    )
    timer = (
        '[Unit]\nDescription=Preview timer for Factor Lab Harvest Agent (not enabled)\n\n'
        f'[Timer]\nOnUnitActiveSec={interval}\nPersistent=true\nUnit=factor-lab-harvest-agent.service\n\n'
        '[Install]\nWantedBy=timers.target\n'
    )
    return {'service': service, 'timer': timer}


def write_timer_preview(*, root: str | Path = ROOT, output_dir: str | Path = 'artifacts/harvest_agent/timer_preview', allow_controlled_execution: bool = False, max_experiments: int = 1, interval: str = '6h') -> dict[str, str | bool]:
    root = Path(root)
    units = render_timer_units(allow_controlled_execution=allow_controlled_execution, max_experiments=max_experiments, interval=interval, root=root)
    out = root / output_dir
    out.mkdir(parents=True, exist_ok=True)
    (out / 'factor-lab-harvest-agent.service').write_text(units['service'], encoding='utf-8')
    (out / 'factor-lab-harvest-agent.timer').write_text(units['timer'], encoding='utf-8')
    (out / 'README.md').write_text('# Harvest Agent timer preview\n\nRendered only. Not installed or enabled. Manual approval is required before scheduling.\n', encoding='utf-8')
    return {**units, 'service_path': str(out / 'factor-lab-harvest-agent.service'), 'timer_path': str(out / 'factor-lab-harvest-agent.timer'), 'enabled': False, 'installed': False}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--preview', action='store_true')
    ap.add_argument('--allow-controlled-execution', action='store_true')
    ap.add_argument('--max-experiments', type=int, default=1)
    ap.add_argument('--interval', default='6h')
    args = ap.parse_args()
    out = write_timer_preview(allow_controlled_execution=args.allow_controlled_execution, max_experiments=args.max_experiments, interval=args.interval)
    print(out['service'])
    print('---')
    print(out['timer'])
    print(f"Preview written to {out['service_path']} and {out['timer_path']}")
