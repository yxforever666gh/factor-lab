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


def test_recovery_is_the_default_research_suite() -> None:
    research = build_parser().parse_args(["research", "run", "--canary"])

    assert research.suite == "recovery"

