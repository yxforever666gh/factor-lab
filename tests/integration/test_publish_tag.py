from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
PUBLISH_SCRIPT = ROOT / "scripts" / "publish-tag.ps1"
PWSH = shutil.which("pwsh")
GIT = shutil.which("git")

pytestmark = pytest.mark.skipif(
    PWSH is None or GIT is None,
    reason="publish-tag black-box tests require pwsh and git",
)


def _run(
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if check and completed.returncode != 0:
        raise AssertionError(
            f"command failed ({completed.returncode}): {args!r}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def _git(repo: Path | None, *args: str) -> str:
    return _run([str(GIT), *args], cwd=repo).stdout.strip()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _install_fake_gh(bin_dir: Path) -> Path:
    bin_dir.mkdir(parents=True, exist_ok=True)
    log_path = bin_dir.parent / "gh-invocations.jsonl"
    driver = bin_dir / "fake_gh.py"
    _write(
        driver,
        """\
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with Path(os.environ["FAKE_GH_LOG"]).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(args) + "\\n")

if args[:2] == ["repo", "view"]:
    print(json.dumps({"nameWithOwner": "example/factor-lab"}))
elif args[:2] == ["run", "list"]:
    print(json.dumps([{
        "status": os.environ.get("FAKE_GH_STATUS", "completed"),
        "conclusion": os.environ.get("FAKE_GH_CONCLUSION", "success"),
        "headSha": os.environ["FAKE_GH_HEAD"],
        "headBranch": os.environ.get("FAKE_GH_BRANCH", "main"),
        "event": os.environ.get("FAKE_GH_EVENT", "push"),
        "databaseId": 123,
        "url": "https://example.invalid/actions/runs/123",
    }]))
else:
    print(f"unexpected fake gh arguments: {args!r}", file=sys.stderr)
    raise SystemExit(2)
""",
    )

    unix_launcher = bin_dir / "gh"
    _write(
        unix_launcher,
        "#!/bin/sh\nexec "
        f"{shlex.quote(sys.executable)} {shlex.quote(str(driver))} \"$@\"\n",
    )
    unix_launcher.chmod(unix_launcher.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP)

    _write(
        bin_dir / "gh.cmd",
        f'@echo off\r\n"{sys.executable}" "{driver}" %*\r\n',
    )
    return log_path


def _make_release_repo(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    repo = tmp_path / "repo"
    remote = tmp_path / "remote.git"
    _git(None, "init", "--bare", str(remote))
    _git(None, "init", "-b", "main", str(repo))
    _git(repo, "config", "user.name", "Release Test")
    _git(repo, "config", "user.email", "release-test@example.invalid")

    script = repo / "scripts" / "publish-tag.ps1"
    script.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PUBLISH_SCRIPT, script)
    _write(
        repo / "pyproject.toml",
        '[project]\nname = "release-test"\nversion = "4.1.0"\n',
    )
    _write(repo / "src" / "factor_lab" / "__init__.py", '__version__ = "4.1.0"\n')
    _write(
        repo / "CHANGELOG.md",
        "# Changelog\n\n"
        "## [Unreleased]\n\n"
        "## [4.1] - 2026-08-28\n\n"
        "- Release evidence.\n",
    )
    _write(repo / ".github" / "workflows" / "ci.yml", "name: CI\n")

    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "release 4.1")
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-u", "origin", "main")
    log_path = _install_fake_gh(tmp_path / "bin")
    return repo, remote, script, log_path


def _commit_and_push(repo: Path, message: str) -> None:
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", message)
    _git(repo, "push", "origin", "main")


def _publish(
    repo: Path,
    script: Path,
    log_path: Path,
    *,
    tag: str = "4.1",
    gh_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PATH"] = str(log_path.parent / "bin") + os.pathsep + environment["PATH"]
    environment["FAKE_GH_LOG"] = str(log_path)
    environment["FAKE_GH_HEAD"] = _git(repo, "rev-parse", "HEAD")
    environment.update(gh_overrides or {})
    return _run(
        [str(PWSH), "-NoLogo", "-NoProfile", "-File", str(script), "-Tag", tag],
        cwd=repo,
        env=environment,
        check=False,
    )


def _combined(completed: subprocess.CompletedProcess[str]) -> str:
    return f"{completed.stdout}\n{completed.stderr}"


def _remote_sha(remote: Path, ref: str) -> str:
    line = _git(None, "ls-remote", str(remote), ref)
    return line.split()[0] if line else ""


def test_publish_tag_creates_annotated_tag_and_uses_exact_ci_filters(tmp_path: Path) -> None:
    repo, remote, script, log_path = _make_release_repo(tmp_path)

    completed = _publish(repo, script, log_path)

    assert completed.returncode == 0, _combined(completed)
    assert _git(repo, "cat-file", "-t", "refs/tags/4.1") == "tag"
    local_object = _git(repo, "rev-parse", "refs/tags/4.1")
    local_commit = _git(repo, "rev-parse", "refs/tags/4.1^{}")
    assert _remote_sha(remote, "refs/tags/4.1") == local_object
    assert _remote_sha(remote, "refs/tags/4.1^{}") == local_commit

    invocations = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    run_args = next(args for args in invocations if args[:2] == ["run", "list"])
    assert run_args[run_args.index("--workflow") + 1] == "ci.yml"
    assert run_args[run_args.index("--branch") + 1] == "main"
    assert run_args[run_args.index("--event") + 1] == "push"
    assert run_args[run_args.index("--commit") + 1] == local_commit


def test_publish_tag_reverifies_an_already_synchronized_tag(tmp_path: Path) -> None:
    repo, remote, script, log_path = _make_release_repo(tmp_path)
    first = _publish(repo, script, log_path)
    assert first.returncode == 0, _combined(first)
    expected_object = _remote_sha(remote, "refs/tags/4.1")
    expected_commit = _remote_sha(remote, "refs/tags/4.1^{}")

    second = _publish(repo, script, log_path)

    assert second.returncode == 0, _combined(second)
    assert "already synchronized" in second.stdout
    assert _remote_sha(remote, "refs/tags/4.1") == expected_object
    assert _remote_sha(remote, "refs/tags/4.1^{}") == expected_commit


@pytest.mark.parametrize("tag", ["04.1", "4.01", "v4.1", "research-os-final-20260828"])
def test_publish_tag_rejects_noncanonical_tags(tmp_path: Path, tag: str) -> None:
    repo, remote, script, log_path = _make_release_repo(tmp_path)

    completed = _publish(repo, script, log_path, tag=tag)

    assert completed.returncode != 0
    assert "canonical major.minor" in _combined(completed)
    assert _remote_sha(remote, f"refs/tags/{tag}") == ""


@pytest.mark.parametrize("target", ["pyproject", "package"])
def test_publish_tag_rejects_version_mismatch(tmp_path: Path, target: str) -> None:
    repo, remote, script, log_path = _make_release_repo(tmp_path)
    path = repo / ("pyproject.toml" if target == "pyproject" else "src/factor_lab/__init__.py")
    path.write_text(path.read_text(encoding="utf-8").replace("4.1.0", "4.1.1"), encoding="utf-8")
    _commit_and_push(repo, f"break {target} version")

    completed = _publish(repo, script, log_path)

    assert completed.returncode != 0
    assert "4.1.0" in _combined(completed)
    assert _remote_sha(remote, "refs/tags/4.1") == ""


@pytest.mark.parametrize(
    "changelog",
    [
        "# Changelog\n\n## [Unreleased]\n\n- Still pending.\n\n"
        "## [4.1] - 2026-08-28\n\n- Release.\n",
        "# Changelog\n\n## [Unreleased]\n\n"
        "## [4.1] - 2026-02-30\n\n- Release.\n",
    ],
)
def test_publish_tag_rejects_invalid_changelog(tmp_path: Path, changelog: str) -> None:
    repo, remote, script, log_path = _make_release_repo(tmp_path)
    (repo / "CHANGELOG.md").write_text(changelog, encoding="utf-8")
    _commit_and_push(repo, "break changelog")

    completed = _publish(repo, script, log_path)

    assert completed.returncode != 0
    assert "CHANGELOG.md" in _combined(completed)
    assert _remote_sha(remote, "refs/tags/4.1") == ""


@pytest.mark.parametrize(
    "overrides",
    [
        {"FAKE_GH_STATUS": "completed", "FAKE_GH_CONCLUSION": "failure"},
        {"FAKE_GH_BRANCH": "feature"},
        {"FAKE_GH_EVENT": "workflow_dispatch"},
        {"FAKE_GH_HEAD": "0" * 40},
    ],
)
def test_publish_tag_rejects_nonmatching_or_failed_ci(
    tmp_path: Path,
    overrides: dict[str, str],
) -> None:
    repo, remote, script, log_path = _make_release_repo(tmp_path)

    completed = _publish(repo, script, log_path, gh_overrides=overrides)

    assert completed.returncode != 0
    assert "CI" in _combined(completed) or "run" in _combined(completed)
    assert _remote_sha(remote, "refs/tags/4.1") == ""
