import json
import subprocess
import sys
from pathlib import Path


def test_summarize_llm_usage_reports_totals(tmp_path):
    ledger = tmp_path / "llm_usage_ledger.jsonl"
    ledger.write_text(
        "\n".join(
            [
                json.dumps({
                    "created_at_utc": "2026-04-28T00:00:00+00:00",
                    "success": True,
                    "decision_type": "planner",
                    "model": "gpt-5.5",
                    "estimated_user_prompt_tokens_4c": 100,
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "total_tokens": 15,
                        "cached_tokens": 7,
                        "cache_creation_tokens": 2,
                        "uncached_prompt_tokens": 3,
                    },
                }),
                json.dumps({
                    "created_at_utc": "2026-04-28T00:01:00+00:00",
                    "success": False,
                    "decision_type": "failure_analyst",
                    "model": "gpt-5.5",
                    "estimated_user_prompt_tokens_4c": 200,
                    "usage": {"prompt_tokens": None, "completion_tokens": None, "total_tokens": None},
                }),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    script = Path(__file__).resolve().parents[1] / "scripts" / "summarize_llm_usage.py"

    result = subprocess.run(
        [sys.executable, str(script), "--ledger", str(ledger)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "rows=2 success=1 failed=1" in result.stdout
    assert "prompt_tokens=10 completion_tokens=5 total_tokens=15" in result.stdout
    assert "cached_tokens=7 cache_creation_tokens=2 uncached_prompt_tokens=3" in result.stdout
    assert "cached_tokens_missing_rows=1" in result.stdout
    assert "cache_creation_tokens_missing_rows=1" in result.stdout
    assert "uncached_prompt_tokens_missing_rows=1" in result.stdout
    assert "estimated_cost_usd=0.000066" in result.stdout
    assert "estimated_user_prompt_tokens_4c=300" in result.stdout
    assert "planner total_tokens=15 cached_tokens=7 cost_usd=0.000066 rows=1" in result.stdout
    assert "gpt-5.5 total_tokens=15 cached_tokens=7 cost_usd=0.000066 rows=2" in result.stdout
