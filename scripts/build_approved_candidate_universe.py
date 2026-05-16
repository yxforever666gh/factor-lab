from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.approved_universe import write_approved_candidate_universe


if __name__ == "__main__":
    payload = write_approved_candidate_universe(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/approved_candidate_universe.json",
        debug_output_path="artifacts/approved_candidate_universe_debug.json",
        lifecycle_output_path="artifacts/approved_candidate_universe_lifecycle.json",
        governance_output_path="artifacts/approved_candidate_universe_governance.json",
    )
    print(f"approved universe built: approved={len(payload.get('rows') or [])}")
