from factor_lab.cli import build_parser


def test_cli_exposes_only_lightweight_mainline_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["data", "status"]).data_command == "status"
    research = parser.parse_args(["research", "run", "--suite", "next", "--canary"])
    assert research.research_command == "run"
    assert research.canary is True
    assert parser.parse_args(["report", "--run", "latest"]).command == "report"

