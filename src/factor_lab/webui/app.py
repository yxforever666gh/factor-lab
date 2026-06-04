from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from jinja2 import Environment


def create_app(title: str = "Factor Lab 中文控制台") -> FastAPI:
    """Create the Factor Lab WebUI FastAPI app.

    This small app factory is the first app-wiring step out of the legacy
    ``webui_app.py`` module.  Route modules are still registered by the legacy
    compatibility module until the remaining large route groups are split.
    """

    return FastAPI(title=title)


def register_startup_cache(app: FastAPI, warm_dashboard_cache: Callable[[], None]) -> None:
    """Attach startup cache warming without keeping the decorator in webui_app."""

    @app.on_event("startup")
    def _warm_dashboard_cache() -> None:
        warm_dashboard_cache()


def render_template(
    template_env: Environment,
    localize_times: Callable[[Any], Any],
    template_name: str,
    **context: Any,
) -> HTMLResponse:
    template = template_env.get_template(template_name)
    return HTMLResponse(template.render(**localize_times(context)))
