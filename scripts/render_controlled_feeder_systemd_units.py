#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


SERVICE_NAME = "factor-lab-controlled-admission-feeder.service"
TIMER_NAME = "factor-lab-controlled-admission-feeder.timer"


def render_units(
    *,
    project_root: str = "/home/admin/factor-lab",
    python_bin: str = "/usr/bin/python3",
    config_path: str = "configs/controlled_admission_feeder.json",
    timer_interval: str = "30min",
    accuracy: str = "1min",
) -> dict[str, str]:
    service = f"""[Unit]
Description=Factor Lab controlled admission feeder
Documentation=file:{project_root}/.hermes/plans/2026-05-05_factor-lab-next-improvement-plan.md
After=factor-lab-research-daemon.service

[Service]
Type=oneshot
WorkingDirectory={project_root}
Environment=PYTHONUNBUFFERED=1
ExecStart={python_bin} {project_root}/scripts/run_controlled_admission_feeder.py --write --config {config_path}
"""
    timer = f"""[Unit]
Description=Run Factor Lab controlled admission feeder periodically
Documentation=file:{project_root}/.hermes/plans/2026-05-05_factor-lab-next-improvement-plan.md

[Timer]
OnBootSec=2min
OnUnitActiveSec={timer_interval}
AccuracySec={accuracy}
Persistent=true
Unit={SERVICE_NAME}

[Install]
WantedBy=timers.target
"""
    return {"service": service, "timer": timer}


def write_units(output_dir: str | Path, **kwargs) -> dict[str, str]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    units = render_units(**kwargs)
    service_path = out / SERVICE_NAME
    timer_path = out / TIMER_NAME
    service_path.write_text(units["service"], encoding="utf-8")
    timer_path.write_text(units["timer"], encoding="utf-8")
    return {"service_path": str(service_path), "timer_path": str(timer_path)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="artifacts/systemd_preview")
    parser.add_argument("--timer-interval", default="30min")
    parser.add_argument("--accuracy", default="1min")
    args = parser.parse_args()
    result = write_units(args.output_dir, timer_interval=args.timer_interval, accuracy=args.accuracy)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
