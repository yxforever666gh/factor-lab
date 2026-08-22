from pathlib import Path

import pytest

from factor_lab.research_os.credentials import (
    CredentialResolutionError,
    read_secret_file,
    resolve_credential_ref,
    resolve_env_secret,
)


def test_secret_ref_resolves_only_through_file_pointer(tmp_path: Path) -> None:
    secret = tmp_path / "tushare"
    secret.write_text("real-token\n", encoding="utf-8")

    assert resolve_credential_ref(
        "secret://tushare_token",
        env={"TUSHARE_TOKEN_FILE": str(secret)},
    ) == "real-token"
    with pytest.raises(CredentialResolutionError, match="requires"):
        resolve_credential_ref(
            "secret://tushare_token",
            env={"TUSHARE_TOKEN": "raw-token"},
        )


def test_plain_env_ref_is_explicitly_test_only() -> None:
    with pytest.raises(CredentialResolutionError, match="forbidden in production"):
        resolve_credential_ref("env://TOKEN", env={"TOKEN": "value"})
    assert resolve_credential_ref(
        "env://TOKEN", env={"TOKEN": "value"}, allow_plain_env=True
    ) == "value"


def test_secret_files_are_one_line_regular_files(tmp_path: Path) -> None:
    multiline = tmp_path / "multi"
    multiline.write_text("one\ntwo\n", encoding="utf-8")
    with pytest.raises(CredentialResolutionError, match="exactly one"):
        read_secret_file(multiline)

    missing = tmp_path / "missing"
    with pytest.raises(CredentialResolutionError, match="missing"):
        read_secret_file(missing)


def test_file_and_plain_env_cannot_be_set_together(tmp_path: Path) -> None:
    secret = tmp_path / "secret"
    secret.write_text("file-value", encoding="utf-8")
    with pytest.raises(CredentialResolutionError, match="set only TOKEN_FILE"):
        resolve_env_secret(
            "TOKEN",
            env={"TOKEN": "raw", "TOKEN_FILE": str(secret)},
        )
