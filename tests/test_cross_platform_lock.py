import subprocess
import sys

from factor_lab.cross_platform_lock import try_lock_file, unlock_file


_CHILD_LOCK_PROBE = """
from pathlib import Path
import sys
from factor_lab.cross_platform_lock import try_lock_file, unlock_file

with Path(sys.argv[1]).open("a+", encoding="utf-8") as handle:
    acquired = try_lock_file(handle)
    print("locked" if acquired else "busy")
    if acquired:
        unlock_file(handle)
"""


def _probe_from_child(path) -> str:
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD_LOCK_PROBE, str(path)],
        text=True,
        capture_output=True,
        timeout=15,
        check=True,
    )
    return completed.stdout.strip()


def test_nonblocking_file_lock_round_trip(tmp_path):
    path = tmp_path / "runtime.lock"
    with path.open("a+", encoding="utf-8") as first:
        assert try_lock_file(first) is True
        with path.open("a+", encoding="utf-8") as second:
            assert try_lock_file(second) is False
        unlock_file(first)
    with path.open("a+", encoding="utf-8") as third:
        assert try_lock_file(third) is True
        unlock_file(third)


def test_nonblocking_file_lock_is_visible_across_processes(tmp_path):
    path = tmp_path / "cross-process.lock"
    with path.open("a+", encoding="utf-8") as parent:
        assert try_lock_file(parent) is True
        assert _probe_from_child(path) == "busy"
        unlock_file(parent)

    assert _probe_from_child(path) == "locked"
