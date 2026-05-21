from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.hermes_contracts import validate_hermes_response


def write_hermes_response_artifact(path: str | Path, payload: dict[str, Any], *, request_id: str, profile_key: str) -> dict[str, Any]:
    errors = validate_hermes_response(payload, request_id=request_id, profile_key=profile_key)
    if errors:
        raise ValueError(f"invalid Hermes response artifact: {','.join(errors)}")
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def read_hermes_response_artifact(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
