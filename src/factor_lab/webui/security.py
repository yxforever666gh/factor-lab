"""Small, dependency-free protections for the localhost settings UI."""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from urllib.parse import urlsplit

from fastapi import HTTPException, Request


_CSRF_TOKEN = secrets.token_urlsafe(32)


def csrf_token() -> str:
    """Return the process-local form token rendered into settings pages."""

    return _CSRF_TOKEN


def _origin_matches_request(request: Request, origin: str) -> bool:
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return False
    return (
        parsed.scheme.lower() == request.url.scheme.lower()
        and (parsed.hostname or "").lower() == (request.url.hostname or "").lower()
        and parsed.port == request.url.port
    )


def enforce_settings_post(request: Request, form: Mapping[str, str]) -> None:
    """Require an unforgeable form token or a verified same-origin POST.

    Browsers send ``Origin`` for cross-site form POSTs, while the hidden token
    covers clients and browser versions that omit it.  A supplied cross-origin
    ``Origin`` is always rejected, even when a token is present, so accidental
    token disclosure cannot turn the localhost console into a cross-site
    request primitive.
    """

    origin = (request.headers.get("origin") or "").strip()
    if origin:
        if _origin_matches_request(request, origin):
            return
        raise HTTPException(status_code=403, detail="cross-origin settings POST rejected")

    submitted = str(form.get("csrf_token") or "")
    if submitted and secrets.compare_digest(submitted, _CSRF_TOKEN):
        return
    raise HTTPException(status_code=403, detail="missing or invalid settings CSRF token")


__all__ = ["csrf_token", "enforce_settings_post"]
