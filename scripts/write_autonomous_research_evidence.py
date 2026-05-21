#!/usr/bin/env python3
import argparse, json
from factor_lab.autonomous_research_evidence import write_evidence_ledger
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--cycle-id',default='cycle_0001')
    print(json.dumps(write_evidence_ledger(cycle_id=ap.parse_args().cycle_id),indent=2))
