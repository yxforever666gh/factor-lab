from __future__ import annotations

import json
import hashlib
from typing import Any


def config_fingerprint(config: dict[str, Any]) -> str:
    payload = json.dumps(config, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
