from pathlib import Path

import pytest

from factor_lab.research_os.webui_readonly_role import (
    WebUIDatabaseRoleConfig,
    WebUIRoleBootstrapError,
    _read_one_line_secret,
    _validate_webui_password,
)


def _environment(tmp_path: Path) -> dict[str, str]:
    owner = tmp_path / "owner-password"
    webui = tmp_path / "webui-password"
    owner.write_text("owner-secret-value\n", encoding="utf-8")
    webui.write_text("webui-secret-value\n", encoding="utf-8")
    return {
        "RESEARCH_OS_POSTGRES_HOST": "postgres",
        "RESEARCH_OS_POSTGRES_PORT_INTERNAL": "5432",
        "RESEARCH_OS_POSTGRES_DB": "factor_lab",
        "RESEARCH_OS_POSTGRES_USER": "factor_lab_owner",
        "RESEARCH_OS_WEBUI_POSTGRES_USER": "factor_lab_webui",
        "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(owner),
        "FACTOR_LAB_WEBUI_POSTGRES_PASSWORD_FILE": str(webui),
    }


def test_webui_role_config_requires_distinct_safe_role(tmp_path: Path) -> None:
    env = _environment(tmp_path)
    parsed = WebUIDatabaseRoleConfig.from_env(env)
    assert parsed.owner_role == "factor_lab_owner"
    assert parsed.webui_role == "factor_lab_webui"

    with pytest.raises(WebUIRoleBootstrapError, match="must differ"):
        WebUIDatabaseRoleConfig.from_env(
            {**env, "RESEARCH_OS_WEBUI_POSTGRES_USER": "factor_lab_owner"}
        )
    with pytest.raises(WebUIRoleBootstrapError, match="lowercase"):
        WebUIDatabaseRoleConfig.from_env(
            {**env, "RESEARCH_OS_WEBUI_POSTGRES_USER": "owner;DROP ROLE owner"}
        )


def test_webui_role_secret_reader_rejects_symlink_and_multiple_lines(
    tmp_path: Path,
) -> None:
    multi = tmp_path / "multi"
    multi.write_text("one\ntwo\n", encoding="utf-8")
    with pytest.raises(WebUIRoleBootstrapError, match="exactly one"):
        _read_one_line_secret(multi, "test")

    target = tmp_path / "target"
    target.write_text("safe-secret\n", encoding="utf-8")
    link = tmp_path / "link"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")
    with pytest.raises(WebUIRoleBootstrapError, match="link/reparse"):
        _read_one_line_secret(link, "test")


@pytest.mark.parametrize(
    "candidate",
    ("too-short", "replace-me-webui-password", "PASSWORD-is-not-a-secret"),
)
def test_webui_role_rejects_weak_or_placeholder_password(candidate: str) -> None:
    with pytest.raises(WebUIRoleBootstrapError, match="too short|forbidden"):
        _validate_webui_password("owner-secret-value-123", candidate)


def test_webui_role_requires_password_distinct_from_owner() -> None:
    with pytest.raises(WebUIRoleBootstrapError, match="must differ"):
        _validate_webui_password(
            "same-strong-secret-123",
            "same-strong-secret-123",
        )
