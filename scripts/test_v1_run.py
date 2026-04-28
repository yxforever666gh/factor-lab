from pathlib import Path
import sys
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


if __name__ == "__main__":
    run(["python3", "scripts/run_scheduled_cycle.py"])
    run(["python3", "scripts/build_sqlite_report.py"])
    run(["python3", "scripts/build_html_report.py"])
    run(["python3", "scripts/build_index_page.py"])
    run(["python3", "scripts/build_run_summary.py"])
    run(["python3", "scripts/build_change_report.py"])

    required = [
        ROOT / "artifacts/factor_lab.db",
        ROOT / "artifacts/report.html",
        ROOT / "artifacts/index.html",
        ROOT / "artifacts/sqlite_report.md",
        ROOT / "artifacts/latest_summary.txt",
        ROOT / "artifacts/change_report.md",
        ROOT / "artifacts/tushare_workflow/summary.md",
        ROOT / "artifacts/tushare_batch/batch_summary.json",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Missing required artifacts:\n" + "\n".join(missing))

    print("v1.0 test run succeeded")
