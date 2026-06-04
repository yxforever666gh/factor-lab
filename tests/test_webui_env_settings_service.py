from pathlib import Path

from factor_lab.webui.services import env_settings


def test_read_env_values_uses_injected_env_file_func(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\nA=1\nB = two\n", encoding="utf-8")

    values = env_settings.read_env_values(lambda: env_file)

    assert values == {"A": "1", "B": "two"}


def test_mask_secret_redacts_without_revealing_full_value():
    assert env_settings.mask_secret("") == "未配置"
    assert env_settings.mask_secret("short") == "***"
    assert env_settings.mask_secret("tushare-secret") == "tush...cret"


def test_split_csv_handles_strings_and_lists():
    assert env_settings.split_csv("primary, backup,,third") == ["primary", "backup", "third"]
    assert env_settings.split_csv([" primary ", "", "backup"]) == ["primary", "backup"]


def test_coerce_boolish_accepts_common_form_values():
    assert env_settings.coerce_boolish("on") is True
    assert env_settings.coerce_boolish("false") is False
    assert env_settings.coerce_boolish(None, default=False) is False


def test_normalize_api_and_data_source_types():
    assert env_settings.normalize_llm_api_format("responses", "gpt-5.5") == "openai_responses"
    assert env_settings.normalize_llm_api_format("", "opus4.7") == "anthropic"
    assert env_settings.normalize_data_source_type("TUSHARE") == "tushare"
    assert env_settings.normalize_data_source_type("unknown") == "custom"
