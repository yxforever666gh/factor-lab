"""Refresh a prospective implementation manifest's exact runtime closure.

Run this only after the release source tree and dependency pins are final.  The
result is deterministic for a given checkout and installed release runtime.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re
import sys
import sysconfig
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "protocols/5.2-target-generator.json"
# ``None`` freezes the complete project-local release environment, including
# transitive native/runtime dependencies.  Tests may replace this with an
# explicit tuple to keep their synthetic environment small.
DISTRIBUTIONS: tuple[str, ...] | None = None
_IMPLEMENTATION_RELEASE_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _git_lf_text_bytes(path: Path) -> bytes:
    """Return the bytes Git stores for the closure's declared text files.

    Every closure path is UTF-8 Python, TOML, JSON, lock text, or the
    prospective watchdog PowerShell runner and is declared
    ``text eol=lf`` in ``.gitattributes``.  A Windows working tree may still
    contain CRLF or mixed newlines before the next checkout.  Hashing those
    raw bytes would bind a representation that Git never publishes, so mirror
    Git's text clean conversion before computing the release digest.
    """

    raw = path.read_bytes()
    try:
        raw.decode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise SystemExit(f"runtime closure text file is not UTF-8: {path}") from exc
    return raw.replace(b"\r\n", b"\n")


def _sha256(path: Path) -> str:
    return hashlib.sha256(_git_lf_text_bytes(path)).hexdigest()


def _closure_files() -> list[Path]:
    runtime_lock = ROOT / "protocols/5.2-runtime-lock.txt"
    fixed = [
        ROOT / "pyproject.toml",
        ROOT / "configs/data.json",
        runtime_lock,
        ROOT / "scripts/invoke-prospective-watchdog.ps1",
    ]
    package = list((ROOT / "src/factor_lab").rglob("*.py"))
    files = sorted(
        [*fixed, *package], key=lambda path: path.relative_to(ROOT).as_posix()
    )
    missing = [path for path in files if not path.is_file()]
    if missing:
        raise SystemExit(f"runtime closure file is missing: {missing[0]}")
    return files


def _distribution_versions() -> dict[str, str]:
    if DISTRIBUTIONS is not None:
        return {
            name: importlib.metadata.version(name)
            for name in sorted(DISTRIBUTIONS, key=str.casefold)
        }
    result: dict[str, str] = {}
    for distribution in importlib.metadata.distributions():
        raw_name = distribution.metadata.get("Name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise SystemExit("installed distribution is missing canonical Name metadata")
        name = re.sub(r"[-_.]+", "-", raw_name.strip().casefold())
        version = distribution.version
        if name in result:
            raise SystemExit(f"duplicate installed distribution metadata: {name}")
        result[name] = version
    if not result:
        raise SystemExit("release environment contains no installed distributions")
    return dict(sorted(result.items()))


def _locked_distribution_versions() -> dict[str, str]:
    runtime_lock = ROOT / "protocols/5.2-runtime-lock.txt"
    result: dict[str, str] = {}
    line_pattern = re.compile(
        r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s]+) "
        r"--hash=sha256:([0-9a-f]{64})$"
    )
    try:
        lines = runtime_lock.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise SystemExit(f"runtime lock is unreadable: {runtime_lock}") from exc
    for line_number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = line_pattern.fullmatch(line)
        if match is None:
            raise SystemExit(f"runtime lock line {line_number} is not exact")
        name = re.sub(r"[-_.]+", "-", match.group(1).casefold())
        if name in result:
            raise SystemExit(f"duplicate runtime lock distribution: {name}")
        result[name] = match.group(2)
    if not result:
        raise SystemExit("runtime lock contains no distributions")
    if list(result) != sorted(result):
        raise SystemExit("runtime lock distributions are not canonically sorted")
    return result


def _verify_environment_matches_lock(distributions: dict[str, str]) -> None:
    locked = _locked_distribution_versions()
    if distributions != locked:
        missing = sorted(set(locked) - set(distributions))
        extra = sorted(set(distributions) - set(locked))
        changed = sorted(
            name
            for name in set(locked) & set(distributions)
            if locked[name] != distributions[name]
        )
        raise SystemExit(
            "release environment differs from runtime lock "
            f"(missing={missing}, extra={extra}, changed={changed})"
        )


def _verify_implementation_release(
    manifest: dict[str, Any], distributions: dict[str, str]
) -> None:
    release = manifest.get("implementation_release")
    if not isinstance(release, str) or not _IMPLEMENTATION_RELEASE_RE.fullmatch(
        release
    ):
        raise SystemExit("manifest implementation_release is not canonical major.minor")
    expected = f"{release}.0"
    installed = distributions.get("factor-research-mvp")
    if installed != expected:
        raise SystemExit(
            "installed factor-research-mvp differs from implementation_release "
            f"(expected={expected}, installed={installed})"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    arguments = parser.parse_args()
    manifest_path = arguments.manifest
    if not manifest_path.is_absolute():
        manifest_path = ROOT / manifest_path
    manifest_path = manifest_path.resolve()
    try:
        manifest_path.relative_to(ROOT)
    except ValueError as exc:
        raise SystemExit("manifest must be inside the project root") from exc
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise SystemExit("manifest must be a JSON object")

    distributions = _distribution_versions()
    if DISTRIBUTIONS is None:
        _verify_environment_matches_lock(distributions)
        _verify_implementation_release(manifest, distributions)
    payload = {
        "schema_version": 1,
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_runtime": sys.version,
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "platform_tag": sysconfig.get_platform(),
        "distributions": distributions,
        "files": [
            {
                "path": path.relative_to(ROOT).as_posix(),
                "sha256": _sha256(path),
            }
            for path in _closure_files()
        ],
    }
    manifest["runtime_closure"] = {
        **payload,
        "payload_sha256": hashlib.sha256(_canonical_json_bytes(payload)).hexdigest(),
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
