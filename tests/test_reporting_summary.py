from pathlib import Path
import json

from factor_lab.summary import build_run_summary
from factor_lab.reporting import write_sqlite_report
from factor_lab.workflow import run_workflow


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_summary_and_reporting_include_new_outputs(tmp_path, monkeypatch):
    output_dir = tmp_path / 'first_workflow'
    config_path = REPO_ROOT / 'configs' / 'first_workflow.json'
    monkeypatch.chdir(tmp_path)
    run_workflow(str(config_path), str(output_dir))

    db_path = tmp_path / 'artifacts' / 'factor_lab.db'
    summary_path = tmp_path / 'artifacts' / 'latest_summary.txt'
    report_path = tmp_path / 'artifacts' / 'sqlite_report.md'

    build_run_summary(db_path, summary_path)
    write_sqlite_report(db_path, report_path)

    summary_text = summary_path.read_text(encoding='utf-8')
    report_text = report_path.read_text(encoding='utf-8')

    assert '四层研究状态' in summary_text
    assert 'rolling' in summary_text.lower()
    assert 'Candidate Status Snapshot' in report_text
    assert 'Rolling Stability Summary' in report_text
