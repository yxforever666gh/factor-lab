import json
import sys
from pathlib import Path

# Use factor_lab.paths for generic-safe path resolution
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paths import project_root
from factor_lab.hermes_decision_router import HermesDecisionRouter


if __name__ == "__main__":
    payload = HermesDecisionRouter().healthcheck()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
