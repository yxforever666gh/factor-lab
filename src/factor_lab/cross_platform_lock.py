"""Small non-blocking file-lock adapter for Windows and POSIX."""

from __future__ import annotations

import os
from typing import IO


if os.name == "nt":  # pragma: no cover - selected by platform
    import msvcrt
else:  # pragma: no cover - selected by platform
    import fcntl


def try_lock_file(handle: IO[str]) -> bool:
    """Try to acquire a one-byte exclusive lock without blocking."""

    if os.name == "nt":
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write("\0")
            handle.flush()
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def unlock_file(handle: IO[str]) -> None:
    if os.name == "nt":
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
