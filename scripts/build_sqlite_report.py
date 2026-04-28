from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.reporting import write_sqlite_report


if __name__ == "__main__":
    write_sqlite_report(
        db_path="artifacts/factor_lab.db",
        output_path="artifacts/sqlite_report.md",
    )
