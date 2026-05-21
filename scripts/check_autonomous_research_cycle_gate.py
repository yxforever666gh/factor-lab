#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from factor_lab.autonomous_research_gate import write_gate_decision
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('plan_path'); ap.add_argument('--allow-controlled-execution',action='store_true')
    a=ap.parse_args(); print(json.dumps(write_gate_decision(a.plan_path,allow_controlled_execution=a.allow_controlled_execution),indent=2))
