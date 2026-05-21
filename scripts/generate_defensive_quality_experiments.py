#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from factor_lab.defensive_quality_experiments import generate_defensive_quality_experiments
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--plan',default='artifacts/autonomous_research_loop/cycle_0001/cycle_plan.json'); ap.add_argument('--dry-run',action='store_true')
    a=ap.parse_args(); plan=json.loads(Path(a.plan).read_text())
    print(json.dumps(generate_defensive_quality_experiments(plan),ensure_ascii=False,indent=2))
