"""Fail-closed credential-file resolution for production Research OS processes.

Secrets are passed as file *locations*, never as their values in ordinary
container environment variables.  ``secret://name`` resolves through the
corresponding ``NAME_FILE`` environment variable (or a named file below an
explicit secrets root).  ``env://NAME`` is retained for test and legacy
imports, while production callers must set ``allow_plain_env=False``.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Mapping


_SECRET_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")


class CredentialResolutionError(RuntimeError):
    """Raised when a credential cannot be resolved without weakening policy."""


def _normalise_env_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", name).upper()


def read_secret_file(path: str | Path, *, label: str = "credential") -> str:
    """Read one regular, non-symlink secret file and strip one trailing newline."""

    supplied = Path(path)
    if supplied.is_symlink() or not supplied.is_file():
        raise CredentialResolutionError(
            f"{label} file is missing, not regular, or is a symlink: {supplied}"
        )
    try:
        raw = supplied.read_bytes()
    except OSError as exc:
        raise CredentialResolutionError(f"{label} file is unreadable: {supplied}") from exc
    if b"\0" in raw:
        raise CredentialResolutionError(f"{label} file contains a NUL byte")
    try:
        value = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CredentialResolutionError(f"{label} file must be UTF-8") from exc
    value = value.rstrip("\r\n")
    if not value or "\n" in value or "\r" in value:
        raise CredentialResolutionError(
            f"{label} file must contain exactly one non-empty line"
        )
    return value


def resolve_credential_ref(
    reference: str,
    *,
    env: Mapping[str, str] | None = None,
    secrets_root: str | Path | None = None,
    allow_plain_env: bool = False,
) -> str:
    """Resolve ``secret://NAME`` or explicitly permitted ``env://NAME``.

    ``secret://tushare_token`` first checks ``TUSHARE_TOKEN_FILE`` and then
    ``<secrets_root>/tushare_token``.  The latter root is explicit so a caller
    cannot make this function search arbitrary host paths.
    """

    values = os.environ if env is None else env
    scheme, separator, name = str(reference or "").partition("://")
    if not separator or scheme not in {"secret", "env"}:
        raise CredentialResolutionError(
            "credential_ref must use secret://NAME or env://NAME"
        )
    if not _SECRET_NAME.fullmatch(name):
        raise CredentialResolutionError("credential_ref contains an unsafe name")
    env_name = _normalise_env_name(name)
    if scheme == "env":
        if not allow_plain_env:
            raise CredentialResolutionError(
                "plain environment credentials are forbidden in production"
            )
        value = str(values.get(env_name) or "")
        if not value:
            raise CredentialResolutionError(
                f"credential environment variable is missing: {env_name}"
            )
        return value

    file_value = str(values.get(f"{env_name}_FILE") or "").strip()
    if file_value:
        return read_secret_file(file_value, label=f"secret://{name}")
    if secrets_root is not None:
        root = Path(secrets_root).resolve()
        candidate = (root / name).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:  # Defensive; the name grammar already blocks traversal.
            raise CredentialResolutionError("credential_ref escapes secrets root") from exc
        return read_secret_file(candidate, label=f"secret://{name}")
    raise CredentialResolutionError(
        f"secret://{name} requires {env_name}_FILE or an explicit secrets root"
    )


def resolve_env_secret(
    name: str,
    *,
    env: Mapping[str, str] | None = None,
    default: str = "",
    allow_plain_env: bool = True,
) -> str:
    """Resolve ``NAME_FILE`` before an optional legacy/plain ``NAME`` value."""

    values = os.environ if env is None else env
    file_path = str(values.get(f"{name}_FILE") or "").strip()
    plain = str(values.get(name) or "")
    if file_path and plain:
        raise CredentialResolutionError(
            f"set only {name}_FILE; {name} must not also contain a credential"
        )
    if file_path:
        return read_secret_file(file_path, label=name)
    if plain:
        if not allow_plain_env:
            raise CredentialResolutionError(
                f"{name} is forbidden in production; use {name}_FILE"
            )
        return plain
    return default


__all__ = [
    "CredentialResolutionError",
    "read_secret_file",
    "resolve_credential_ref",
    "resolve_env_secret",
]
