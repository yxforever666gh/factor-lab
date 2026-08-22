from __future__ import annotations

"""Fail-closed startup checks for the isolated production WebUI container."""

import os
from pathlib import Path
import re
import stat
from typing import Mapping
from urllib.parse import urlparse


WEBUI_SECRET_EDITOR = Path("/opt/factor-lab/runtime/secrets-editor")
WEBUI_SETTINGS_DIRECTORY = Path("/opt/factor-lab/runtime/artifacts/settings")
WEBUI_DATABASE_SECRET = Path("/run/webui-db-secret/password")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_ALLOWED_EDITOR_SECRET = re.compile(
    r"(?:tushare_token|diemeng_api_key|llm_api_key|llm-[A-Za-z0-9_.-]+|"
    r"source-(?:tushare|diemeng|akshare|local_file|custom)-[A-Za-z0-9_.-]+)"
)
_FORBIDDEN_WORKER_ENV = {
    "FACTOR_LAB_OBJECT_STORE_ACCESS_KEY_FILE",
    "FACTOR_LAB_OBJECT_STORE_SECRET_KEY_FILE",
    "FACTOR_LAB_ICEBERG_CATALOG_URI",
    "FACTOR_LAB_ICEBERG_WAREHOUSE",
    "FACTOR_LAB_SECRETS_ROOT",
    "TUSHARE_TOKEN_FILE",
    "DIEMENG_API_KEY_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "PGPASSWORD",
    "RESEARCH_OS_DATABASE_URL",
    "DAGSTER_POSTGRES_URL",
}


class WebUIRuntimeIsolationError(RuntimeError):
    pass


def _link_or_reparse(path: Path) -> bool:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(info.st_mode) or bool(
        int(getattr(info, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _require_regular_file(path: Path, label: str) -> None:
    if _link_or_reparse(path) or not path.is_file():
        raise WebUIRuntimeIsolationError(f"{label} is missing or is not a regular file")


def _validate_editor_root(root: Path) -> None:
    if _link_or_reparse(root) or not root.is_dir():
        raise WebUIRuntimeIsolationError(
            "WebUI secret editor root is missing or is a link/reparse point"
        )
    for item in root.iterdir():
        if _link_or_reparse(item) or not item.is_file():
            raise WebUIRuntimeIsolationError(
                "WebUI secret editor root may contain regular secret files only"
            )
        if not _ALLOWED_EDITOR_SECRET.fullmatch(item.name):
            raise WebUIRuntimeIsolationError(
                f"WebUI secret editor root contains forbidden entry {item.name!r}"
            )
        if item.stat().st_size > 64 * 1024:
            raise WebUIRuntimeIsolationError("WebUI editor secret exceeds 64 KiB")
        lines = item.read_text(encoding="utf-8-sig").splitlines()
        if len(lines) != 1 or not lines[0] or "\x00" in lines[0]:
            raise WebUIRuntimeIsolationError(
                "WebUI editor secrets must contain exactly one non-empty line"
            )


def _mount_table() -> dict[Path, frozenset[str]]:
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.is_file():
        raise WebUIRuntimeIsolationError("Linux mount metadata is unavailable")

    def decode(value: str) -> str:
        return (
            value.replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\")
        )

    mounts: dict[Path, frozenset[str]] = {}
    for line in mountinfo.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) < 6:
            continue
        mounts[Path(decode(fields[4]))] = frozenset(fields[5].split(","))
    return mounts


def validate_webui_runtime(
    env: Mapping[str, str] | None = None, *, require_mounts: bool = True
) -> None:
    values = os.environ if env is None else env
    if str(values.get("FACTOR_LAB_PRODUCTION_ROLE") or "").strip() != "webui":
        raise WebUIRuntimeIsolationError("production WebUI role marker is missing")
    leaked = sorted(key for key in _FORBIDDEN_WORKER_ENV if values.get(key))
    if leaked:
        raise WebUIRuntimeIsolationError(
            "production WebUI inherited forbidden worker environment: "
            + ", ".join(leaked)
        )
    if Path(str(values.get("FACTOR_LAB_SECRETS_DIR") or "")) != WEBUI_SECRET_EDITOR:
        raise WebUIRuntimeIsolationError("WebUI secret editor path is not isolated")
    if Path(str(values.get("FACTOR_LAB_ENV_FILE") or "")).parent != WEBUI_SETTINGS_DIRECTORY:
        raise WebUIRuntimeIsolationError("WebUI settings file is outside its narrow mount")
    if Path(str(values.get("FACTOR_LAB_POSTGRES_PASSWORD_FILE") or "")) != WEBUI_DATABASE_SECRET:
        raise WebUIRuntimeIsolationError("WebUI database secret path is not isolated")

    database_url = str(values.get("FACTOR_LAB_DATABASE_URL") or "")
    parsed = urlparse(database_url)
    expected_role = str(values.get("RESEARCH_OS_WEBUI_POSTGRES_USER") or "").strip()
    if (
        not database_url.startswith("postgresql+")
        or not expected_role
        or parsed.username != expected_role
        or parsed.password is not None
    ):
        raise WebUIRuntimeIsolationError(
            "WebUI database URL must use the dedicated passwordless read-only login"
        )
    effective_role = str(values.get("RESEARCH_OS_POSTGRES_USER") or "").strip()
    if effective_role != expected_role:
        raise WebUIRuntimeIsolationError(
            "WebUI process PostgreSQL role must be the dedicated read-only role"
        )

    _require_regular_file(WEBUI_DATABASE_SECRET, "WebUI database secret")
    _validate_editor_root(WEBUI_SECRET_EDITOR)
    if _link_or_reparse(WEBUI_SETTINGS_DIRECTORY) or not WEBUI_SETTINGS_DIRECTORY.is_dir():
        raise WebUIRuntimeIsolationError(
            "WebUI settings directory is missing or is a link/reparse point"
        )
    if require_mounts:
        mounts = _mount_table()
        expected_modes = {
            Path("/opt/factor-lab/runtime/artifacts"): "ro",
            WEBUI_SETTINGS_DIRECTORY: "rw",
            WEBUI_SECRET_EDITOR: "rw",
            WEBUI_DATABASE_SECRET: "ro",
        }
        for target, mode in expected_modes.items():
            if target not in mounts or mode not in mounts[target]:
                raise WebUIRuntimeIsolationError(
                    f"WebUI mount {target} is missing required {mode} mode"
                )
        if Path("/run/secrets") in mounts:
            raise WebUIRuntimeIsolationError(
                "WebUI must not mount the worker /run/secrets"
            )


def main() -> int:
    validate_webui_runtime()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
