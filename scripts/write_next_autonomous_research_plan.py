#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from factor_lab.autonomous_research_planner import write_next_autonomous_research_plan
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--verdict',default='artifacts/autonomous_research_loop/cycle_0001/verdict.json'); ap.add_argument('--previous-cycle-id',default='cycle_0001')
    a=ap.parse_args(); v=json.loads(Path(a.verdict).read_text())
    r=write_next_autonomous_research_plan(v,previous_cycle_id=a.previous_cycle_id); print(json.dumps({k:str(v) for k,v in r.items()},indent=2))
