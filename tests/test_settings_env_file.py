from pathlib import Path

from factor_lab.settings import load_env_file


def test_load_env_file_loads_missing_values_without_overriding(monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "# comment\n"
        "TUSHARE_TOKEN=from_file\n"
        "export DIEMENG_API_KEY='diemeng_key'\n"
        "EXISTING=from_file\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("DIEMENG_API_KEY", raising=False)
    monkeypatch.setenv("EXISTING", "from_env")

    load_env_file(env_path)

    assert __import__("os").environ["TUSHARE_TOKEN"] == "from_file"
    assert __import__("os").environ["DIEMENG_API_KEY"] == "diemeng_key"
    assert __import__("os").environ["EXISTING"] == "from_env"


def test_load_env_file_uses_factor_lab_env_file(monkeypatch, tmp_path):
    env_path = tmp_path / "custom.env"
    env_path.write_text('CUSTOM_ENV_VALUE="loaded"\n', encoding="utf-8")
    monkeypatch.setenv("FACTOR_LAB_ENV_FILE", str(env_path))
    monkeypatch.delenv("CUSTOM_ENV_VALUE", raising=False)

    load_env_file()

    assert __import__("os").environ["CUSTOM_ENV_VALUE"] == "loaded"
