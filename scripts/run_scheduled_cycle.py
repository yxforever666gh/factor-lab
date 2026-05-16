from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.batch import run_batch
from factor_lab.change_detection import build_change_report
from factor_lab.reporting import write_sqlite_report
from factor_lab.html_report import build_html_report
from factor_lab.index_page import build_index_page
from factor_lab.summary import build_run_summary
from factor_lab.approved_universe import write_approved_candidate_universe, resolve_paper_portfolio_inputs
from factor_lab.paper_portfolio import build_paper_portfolio, append_portfolio_history, build_portfolio_change_log
from factor_lab.heartbeat import append_heartbeat


CANDIDATE_POOL_PATH = Path("artifacts/tushare_workflow/candidate_pool.json")
DATASET_PATH = Path("artifacts/tushare_workflow/dataset.csv")
PAPER_PORTFOLIO_DIR = Path("artifacts/paper_portfolio")


def load_candidate_factor_definitions(candidate_pool_path: str | Path) -> list[dict[str, str]]:
    path = Path(candidate_pool_path)
    if not path.exists():
        return []

    candidates = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(candidates, list):
        raise ValueError(f"candidate pool must be a list: {path}")

    definitions = []
    for row in candidates:
        factor_name = row.get("factor_name")
        expression = row.get("expression")
        if not factor_name or not expression:
            continue
        definitions.append({"name": factor_name, "expression": expression})
    return definitions


def update_paper_portfolio(
    candidate_pool_path: str | Path = CANDIDATE_POOL_PATH,
    dataset_path: str | Path = DATASET_PATH,
    output_dir: str | Path = PAPER_PORTFOLIO_DIR,
) -> dict:
    candidate_pool_path = Path(candidate_pool_path)
    dataset_path = Path(dataset_path)
    use_explicit_inputs = candidate_pool_path != CANDIDATE_POOL_PATH or dataset_path != DATASET_PATH
    if use_explicit_inputs:
        factor_definitions = load_candidate_factor_definitions(candidate_pool_path)
        current = build_paper_portfolio(
            dataset_path=dataset_path,
            factor_definitions=factor_definitions,
            output_dir=output_dir,
            strategy_name="paper_candidates_only",
            source_metadata={"source": "explicit_candidate_pool", "candidate_pool_path": str(candidate_pool_path), "dataset_path": str(dataset_path)},
        )
    else:
        inputs = resolve_paper_portfolio_inputs(
            db_path="artifacts/factor_lab.db",
            approved_universe_path="artifacts/approved_candidate_universe.json",
            fallback_candidate_pool_path=candidate_pool_path,
            fallback_dataset_path=dataset_path,
        )
        resolved_dataset_path = Path(inputs["dataset_path"]) if inputs.get("dataset_path") else Path(dataset_path)
        current = build_paper_portfolio(
            dataset_path=resolved_dataset_path,
            factor_definitions=inputs.get("factor_definitions") or [],
            output_dir=output_dir,
            strategy_name="paper_candidates_only",
            source_metadata={"source": inputs.get("source"), **(inputs.get("metadata") or {})},
        )
    append_portfolio_history(
        current_path=Path(output_dir) / "current_portfolio.json",
        history_path=Path(output_dir) / "portfolio_history.json",
    )
    build_portfolio_change_log(
        current_path=Path(output_dir) / "current_portfolio.json",
        history_path=Path(output_dir) / "portfolio_history.json",
        output_path=Path(output_dir) / "portfolio_change_log.md",
    )
    return current


def run_scheduled_cycle() -> None:
    try:
        append_heartbeat("scheduled_cycle", "started", message="完整周期开始执行。")
        run_batch(
            config_path="configs/tushare_batch.json",
            output_dir="artifacts/tushare_batch",
        )
        write_sqlite_report(
            db_path="artifacts/factor_lab.db",
            output_path="artifacts/sqlite_report.md",
        )
        build_html_report(
            db_path="artifacts/factor_lab.db",
            output_path="artifacts/report.html",
        )
        build_index_page(
            db_path="artifacts/factor_lab.db",
            output_path="artifacts/index.html",
        )
        build_run_summary(
            db_path="artifacts/factor_lab.db",
            output_path="artifacts/latest_summary.txt",
        )
        build_change_report(
            db_path="artifacts/factor_lab.db",
            output_path="artifacts/change_report.md",
        )

        write_approved_candidate_universe(
            db_path="artifacts/factor_lab.db",
            output_path="artifacts/approved_candidate_universe.json",
            debug_output_path="artifacts/approved_candidate_universe_debug.json",
        )
        update_paper_portfolio()

        from factor_lab.paper_portfolio_retrospective import build_portfolio_retrospective, build_portfolio_stability_score

        build_portfolio_retrospective(
            history_path="artifacts/paper_portfolio/portfolio_history.json",
            output_path="artifacts/paper_portfolio/portfolio_retrospective.json",
        )
        build_portfolio_stability_score(
            retro_path="artifacts/paper_portfolio/portfolio_retrospective.json",
            output_path="artifacts/paper_portfolio/portfolio_stability_score.json",
        )
        append_heartbeat("scheduled_cycle", "finished", summary="完整周期执行完成，并已更新报告、组合与变化检测。")
    except Exception as exc:
        append_heartbeat("scheduled_cycle", "failed", message=str(exc))
        raise


if __name__ == "__main__":
    run_scheduled_cycle()
