from __future__ import annotations

import os
import subprocess
import time
from typing import Any


def _run_command(command: list[str]) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=3, check=False)
    except Exception:
        return ""
    if result.returncode != 0:
        return ""
    return (result.stdout or "").strip()


def _parse_key_value_lines(text: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    for line in (text or "").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        payload[key.strip()] = value.strip()
    return payload


def current_session_id() -> str | None:
    preferred = (os.getenv("XDG_SESSION_ID") or "").strip()
    if preferred:
        return preferred
    text = _run_command(["loginctl", "list-sessions", "--no-legend"])
    for line in text.splitlines():
        parts = [part for part in line.split() if part]
        if not parts:
            continue
        # loginctl columns: SESSION UID USER SEAT TTY
        if len(parts) >= 3 and parts[2] == os.getenv("USER", ""):
            return parts[0]
        return parts[0]
    return None


def session_properties(session_id: str | None = None) -> dict[str, Any]:
    session_id = session_id or current_session_id()
    if not session_id:
        return {"available": False}
    text = _run_command(
        [
            "loginctl",
            "show-session",
            str(session_id),
            "-p",
            "Active",
            "-p",
            "IdleHint",
            "-p",
            "IdleSinceHint",
            "-p",
            "IdleSinceHintMonotonic",
            "-p",
            "LockedHint",
            "-p",
            "Remote",
            "-p",
            "Type",
            "-p",
            "State",
            "-p",
            "Name",
        ]
    )
    raw = _parse_key_value_lines(text)
    if not raw:
        return {"available": False, "session_id": session_id}
    return {
        "available": True,
        "session_id": session_id,
        "active": raw.get("Active", "").lower() == "yes",
        "idle_hint": raw.get("IdleHint", "").lower() == "yes",
        "locked_hint": raw.get("LockedHint", "").lower() == "yes",
        "remote": raw.get("Remote", "").lower() == "yes",
        "type": raw.get("Type") or None,
        "state": raw.get("State") or None,
        "name": raw.get("Name") or None,
        "idle_since_hint": raw.get("IdleSinceHint") or None,
        "idle_since_hint_monotonic": raw.get("IdleSinceHintMonotonic") or None,
    }


def get_user_idle_seconds() -> float | None:
    props = session_properties()
    if not props.get("available"):
        return None
    idle_hint = bool(props.get("idle_hint"))
    idle_monotonic = props.get("idle_since_hint_monotonic")
    if not idle_hint:
        return 0.0
    try:
        monotonic_us = int(str(idle_monotonic))
    except Exception:
        return None
    now_us = time.monotonic_ns() // 1000
    if monotonic_us <= 0 or now_us <= monotonic_us:
        return None
    return max(0.0, (now_us - monotonic_us) / 1_000_000.0)


def user_idle_snapshot() -> dict[str, Any]:
    props = session_properties()
    idle_seconds = get_user_idle_seconds() if props.get("available") else None
    interactive_confidence = 0.0
    idle_confidence = 0.0

    if props.get("available"):
        if props.get("active") and not props.get("remote"):
            interactive_confidence += 0.4
        if props.get("type") in {"x11", "wayland"}:
            interactive_confidence += 0.2
        if props.get("locked_hint"):
            idle_confidence += 0.5
        if idle_seconds is not None:
            if idle_seconds < 60:
                interactive_confidence += 0.5
            elif idle_seconds >= 300:
                idle_confidence += 0.5
            elif idle_seconds >= 120:
                idle_confidence += 0.25
        if props.get("idle_hint"):
            idle_confidence += 0.2

    mode = "unknown"
    if idle_confidence >= 0.6 and (idle_confidence - interactive_confidence >= 0.2 or bool(props.get("locked_hint"))):
        mode = "background_idle"
    elif interactive_confidence >= 0.6 and (interactive_confidence - idle_confidence >= 0.2 or (idle_seconds is not None and idle_seconds < 60 and not props.get("locked_hint"))):
        mode = "interactive"

    return {
        "available": bool(props.get("available")),
        "session_id": props.get("session_id"),
        "active": bool(props.get("active")),
        "locked": bool(props.get("locked_hint")),
        "remote": bool(props.get("remote")),
        "session_type": props.get("type"),
        "state": props.get("state"),
        "idle_seconds": idle_seconds,
        "interactive_confidence": round(interactive_confidence, 3),
        "idle_confidence": round(idle_confidence, 3),
        "mode": mode,
    }
