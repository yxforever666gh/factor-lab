from __future__ import annotations

import os
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _path_from_env(name: str) -> Path | None:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def project_root() -> Path:
    return _path_from_env("FACTOR_LAB_ROOT") or _repo_root()


def artifacts_dir() -> Path:
    return _path_from_env("FACTOR_LAB_ARTIFACTS_DIR") or (project_root() / "artifacts")


def config_dir() -> Path:
    return _path_from_env("FACTOR_LAB_CONFIG_DIR") or (project_root() / "configs")


def env_file() -> Path:
    return _path_from_env("FACTOR_LAB_ENV_FILE") or (project_root() / ".env")


def db_path() -> Path:
    return _path_from_env("FACTOR_LAB_DB_PATH") or (artifacts_dir() / "factor_lab.db")
