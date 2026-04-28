import importlib

import pytest


pytest.importorskip("fastapi")
pytest.importorskip("jinja2")


def test_webui_module_imports():
    module = importlib.import_module("factor_lab.webui_app")
    assert module.app.title == "Factor Lab 中文控制台"
