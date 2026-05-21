#!/usr/bin/env python3
import argparse, json
from factor_lab.autonomous_research_verdict import write_verdict
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--cycle-id',default='cycle_0001')
    print(json.dumps(write_verdict(cycle_id=ap.parse_args().cycle_id),indent=2))
