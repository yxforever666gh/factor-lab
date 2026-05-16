#!/usr/bin/env python3
from factor_lab.value_sleeve_decision import write_value_sleeve_decision
import json
if __name__ == "__main__":
    payload=write_value_sleeve_decision()
    print(json.dumps({"written": True, "decision": payload.get("decision"), "primary": payload.get("primary_route")}, ensure_ascii=False))
