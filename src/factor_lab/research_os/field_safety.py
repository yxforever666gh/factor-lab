"""Shared forward-field classification for research and shadow boundaries."""

from __future__ import annotations

import re


_FORWARD_SEGMENT = re.compile(
    r"(^|_)(forward|future|label|lead|next|fwd)(_|$)", re.IGNORECASE
)
_TARGET_SEGMENT = re.compile(r"(^|_)target(_|$)", re.IGNORECASE)
_HORIZON_RETURN = re.compile(
    r"(^|_)return_[0-9]+(?:d|w|m|y)?(?:_|$)", re.IGNORECASE
)
_EXACT_FORWARD_ALIASES = frozenset({"label", "target", "y"})


def is_forward_derived_field(
    value: object,
    *,
    strict_target_segments: bool = False,
) -> bool:
    """Return whether a field name can carry a future outcome.

    ``target_weights`` is legitimate portfolio state, so general shadow-event
    inspection only treats the exact name ``target`` as an outcome.  The
    factor DSL enables ``strict_target_segments`` because any target-named raw
    feature would bypass preregistered label separation.
    """

    normalized = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    if not normalized:
        return False
    return bool(
        normalized in _EXACT_FORWARD_ALIASES
        or _FORWARD_SEGMENT.search(normalized)
        or _HORIZON_RETURN.search(normalized)
        or "target_return" in normalized
        or "return_label" in normalized
        or (strict_target_segments and _TARGET_SEGMENT.search(normalized))
    )


__all__ = ["is_forward_derived_field"]
