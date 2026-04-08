import json

from factor_lab.opportunity_policy import _read_json


def test_read_json_tolerates_extra_trailing_brace(tmp_path):
    path = tmp_path / "research_opportunity_store.json"
    path.write_text('{"opportunities": {}}\n}', encoding="utf-8")

    payload = _read_json(path, {"opportunities": {}})

    assert payload == {"opportunities": {}}


def test_read_json_raises_on_real_invalid_json(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text('{"opportunities": { invalid', encoding="utf-8")

    try:
        _read_json(path, {})
    except json.JSONDecodeError:
        pass
    else:
        raise AssertionError("expected JSONDecodeError")
