#!/usr/bin/env python3
import argparse, json
from factor_lab.autonomous_research_planner import write_autonomous_research_cycle_plan
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--dry-run',action='store_true'); ap.add_argument('--dataset-path')
    a=ap.parse_args(); r=write_autonomous_research_cycle_plan(dry_run=a.dry_run,dataset_path=a.dataset_path)
    print(json.dumps({k:str(v) for k,v in r.items()},indent=2))
