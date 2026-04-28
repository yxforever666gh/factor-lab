from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.tushare_master_materialize import materialize_recent_generated_runs


if __name__ == "__main__":
    created = materialize_recent_generated_runs('artifacts/factor_lab.db', limit=30)
    print({'created': created})
