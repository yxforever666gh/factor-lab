#!/usr/bin/env python3
from factor_lab.value_sleeve_portfolio_validation import write_sleeve_portfolio_validation
import json
if __name__ == "__main__":
    payload=write_sleeve_portfolio_validation()
    print(json.dumps({"written": True, "combinations": len(payload.get("combinations", [])), "best": payload.get("best_combination_id")}, ensure_ascii=False))
