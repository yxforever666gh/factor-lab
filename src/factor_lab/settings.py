from __future__ import annotations

import os
from pathlib import Path


_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]


def load_env_file(path: str | Path | None = None) -> None:
    env_path = Path(path) if path else _WORKSPACE_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def get_required_env(key: str) -> str:
    load_env_file()
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value
