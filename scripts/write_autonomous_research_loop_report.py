#!/usr/bin/env python3
import json
from factor_lab.autonomous_research_loop_report import write_autonomous_research_loop_report
if __name__=='__main__': print(json.dumps(write_autonomous_research_loop_report(),ensure_ascii=False,indent=2))
