import json
from pathlib import Path

import factor_lab.cli as cli
from factor_lab.cli import build_parser


def test_cli_exposes_only_lightweight_mainline_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["data", "status"]).data_command == "status"
    research = parser.parse_args(["research", "run", "--suite", "next", "--canary"])
    assert research.research_command == "run"
    assert research.canary is True
    assert parser.parse_args(["report", "--run", "latest"]).command == "report"
    enrich = parser.parse_args(
        ["data", "enrich", "--from", "2017-01-01", "--to", "2026-08-13"]
    )
    assert enrich.data_command == "enrich"
    assert enrich.resume is True
    suspensions = parser.parse_args(
        ["data", "suspensions", "--from", "2017-01-01", "--to", "2026-08-21"]
    )
    assert suspensions.data_command == "suspensions"
    assert suspensions.resume is True


def test_adaptive_is_the_default_research_suite() -> None:
    research = build_parser().parse_args(["research", "run", "--canary"])

    assert research.suite == "adaptive"


def test_default_root_finds_checkout_above_noneditable_wheel(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "factor-lab"
    (project / ".git").mkdir(parents=True)
    (project / "protocols").mkdir()
    (project / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (project / "protocols" / "5.0.json").write_text("{}\n", encoding="utf-8")
    installed = (
        project
        / "runtime/environments/5.5/Lib/site-packages/factor_lab/cli.py"
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.setattr(cli, "__file__", str(installed))
    monkeypatch.chdir(elsewhere)

    assert cli._root() == project.resolve()


def test_explicit_root_does_not_require_implicit_discovery(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    def fail_discovery() -> Path:
        raise AssertionError("explicit --root must bypass discovery")

    monkeypatch.setattr(cli, "_root", fail_discovery)
    monkeypatch.setattr(
        cli,
        "ledger_status",
        lambda root: {"ledger_root": str(Path(root).resolve())},
    )

    assert cli.main(["--root", str(tmp_path), "prospective", "status"]) == 0
    rendered = json.loads(capsys.readouterr().out)
    assert Path(rendered["ledger_root"]) == (
        tmp_path / "runtime/prospective/5.0"
    ).resolve()

