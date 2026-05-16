from __future__ import annotations

from factor_lab.pit_non_cashflow_mechanism_preflight import run_non_cashflow_mechanism_preflight


if __name__ == "__main__":
    payload = run_non_cashflow_mechanism_preflight()
    decision = payload["decision"]
    print(decision["decision"])
    print(decision.get("recommended_mechanism"))
