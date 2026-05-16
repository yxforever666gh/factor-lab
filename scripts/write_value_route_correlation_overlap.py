#!/usr/bin/env python3
from factor_lab.value_route_correlation_overlap import write_correlation_overlap
import json
if __name__ == "__main__":
    payload=write_correlation_overlap()
    print(json.dumps({"written": True, "pairs": len(payload.get("pairs", [])), "decision": payload.get("decision")}, ensure_ascii=False))
