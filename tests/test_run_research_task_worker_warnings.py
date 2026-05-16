from pathlib import Path


def test_run_research_task_worker_installs_warning_filters_before_factor_lab_imports():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_research_task_worker.py"
    text = path.read_text(encoding="utf-8")

    filter_pos = text.index('warnings.filterwarnings("ignore", category=Warning')
    first_factor_lab_import = text.index("from factor_lab.")

    assert filter_pos < first_factor_lab_import
