#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from factor_lab.autonomous_research_executor import run_autonomous_research_cycle
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--plan',default='artifacts/autonomous_research_loop/cycle_0001/cycle_plan.json'); ap.add_argument('--gate',default='artifacts/autonomous_research_loop/cycle_0001/gate_decision.json'); ap.add_argument('--dry-run',action='store_true'); ap.add_argument('--allow-controlled-execution',action='store_true'); ap.add_argument('--max-experiments',type=int)
    a=ap.parse_args(); plan=json.loads(Path(a.plan).read_text()); gate=json.loads(Path(a.gate).read_text())
    print(json.dumps(run_autonomous_research_cycle(plan,gate,dry_run=a.dry_run,allow_controlled_execution=a.allow_controlled_execution,max_experiments=a.max_experiments),indent=2))
