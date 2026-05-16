# Controlled Admission Feeder Systemd Rollout

This document describes the preview-only systemd units for the controlled admission feeder. Do not copy these into `~/.config/systemd/user/` or run `systemctl --user daemon-reload` until the user explicitly approves the operational profile.

## Conservative preview

Generate preview files only:

```bash
cd /home/admin/factor-lab
python3 scripts/render_controlled_feeder_systemd_units.py --output-dir artifacts/systemd_preview
```

Expected files:

```text
artifacts/systemd_preview/factor-lab-controlled-admission-feeder.service
artifacts/systemd_preview/factor-lab-controlled-admission-feeder.timer
```

Conservative profile:

- Timer interval: `30min`
- Config file: `configs/controlled_admission_feeder.json`
- Feeder config defaults: cooldown `60min`, daily budget `3`, limit `1`, no force-new.

## Stop/go gate

Before applying any unit change, verify:

```bash
cd /home/admin/factor-lab
python3 scripts/run_controlled_admission_feeder.py --config configs/controlled_admission_feeder.json
python3 scripts/dry_run_controlled_restart.py
python3 scripts/audit_runtime_takeover.py
```

Apply only after explicit user approval.
