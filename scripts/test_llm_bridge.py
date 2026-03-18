from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


if __name__ == "__main__":
    run(["python3", "scripts/run_llm_bridge_prepare.py"])
    response = ROOT / "artifacts/agent_response.json"
    if not response.exists():
        raise SystemExit("缺少 artifacts/agent_response.json，请先让 OpenClaw agent 处理 request。")
    run(["python3", "scripts/import_llm_bridge_response.py"])
    for path in [
        ROOT / "artifacts/agent_request.json",
        ROOT / "artifacts/agent_response.json",
        ROOT / "artifacts/llm_status.json",
        ROOT / "artifacts/llm_review.md",
        ROOT / "artifacts/llm_next_batch_proposal.json",
    ]:
        if not path.exists():
            raise SystemExit(f"缺少必需文件: {path}")
    print("llm bridge test succeeded")
