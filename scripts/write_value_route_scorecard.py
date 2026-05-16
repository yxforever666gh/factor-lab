#!/usr/bin/env python3
from factor_lab.value_route_scorecard import write_route_scorecard
import json
if __name__ == "__main__":
    payload = write_route_scorecard()
    print(json.dumps({"written": True, "routes": len(payload.get("routes", []))}, ensure_ascii=False))
