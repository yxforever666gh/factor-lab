import json
import sys
from pathlib import Path

# Use factor_lab.paths for generic-safe path resolution
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paths import project_root
from factor_lab.llm_provider_router import DecisionProviderRouter


if __name__ == "__main__":
    payload = DecisionProviderRouter().healthcheck()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
