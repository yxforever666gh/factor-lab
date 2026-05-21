# Autonomous Research Loop Ops

Preview only. Do not enable timer without manual approval.

Commands:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_research_loop_once.py --dry-run
PYTHONPATH=src .venv/bin/python scripts/run_autonomous_research_loop_once.py --allow-controlled-execution --max-experiments 1
PYTHONPATH=src .venv/bin/python scripts/render_autonomous_research_loop_timer.py --preview
```

Safety: no live trading, no broad daemon restoration, no order generation.
