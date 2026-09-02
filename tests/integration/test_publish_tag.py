from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

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


def _make_release_repo(tmp_path: Path) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    remote = tmp_path / "remote.git"
    _git(None, "init", "--bare", str(remote))
    _git(None, "init", "-b", "main", str(repo))
    _git(repo, "config", "user.name", "Release Test")
    _git(repo, "config", "user.email", "release-test@example.invalid")

    script = repo / "scripts" / "publish-tag.ps1"
    script.parent.mkdir(parents=True, exist_ok=True)
    script_source = PUBLISH_SCRIPT.read_text(encoding="utf-8")
    assert "Assert-GitHubTransport\n$head" in script_source
    script.write_text(
        script_source.replace(
            "Assert-GitHubTransport\n$head",
            "# Test copy uses an isolated local bare remote.\n$head",
            1,
        ),
        encoding="utf-8",
    )
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
        "- Local verification: 12 passed, 0 skipped; compileall; wheel "
        f"SHA-256 {'0' * 64}; pip check; CLI verification.\n",
    )

    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "release 4.1")
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-u", "origin", "main")
    return repo, remote, script


def _commit_and_push(repo: Path, message: str) -> None:
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", message)
    _git(repo, "push", "origin", "main")


def _publish(
    repo: Path,
    script: Path,
    *,
    tag: str = "4.1",
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
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


def test_publish_tag_creates_and_verifies_annotated_remote_tag(tmp_path: Path) -> None:
    repo, remote, script = _make_release_repo(tmp_path)

    completed = _publish(repo, script)

    assert completed.returncode == 0, _combined(completed)
    assert _git(repo, "cat-file", "-t", "refs/tags/4.1") == "tag"
    local_object = _git(repo, "rev-parse", "refs/tags/4.1")
    local_commit = _git(repo, "rev-parse", "refs/tags/4.1^{}")
    assert _remote_sha(remote, "refs/tags/4.1") == local_object
    assert _remote_sha(remote, "refs/tags/4.1^{}") == local_commit


def test_publish_tag_reverifies_an_already_synchronized_tag(tmp_path: Path) -> None:
    repo, remote, script = _make_release_repo(tmp_path)
    first = _publish(repo, script)
    assert first.returncode == 0, _combined(first)
    expected_object = _remote_sha(remote, "refs/tags/4.1")
    expected_commit = _remote_sha(remote, "refs/tags/4.1^{}")

    second = _publish(repo, script)

    assert second.returncode == 0, _combined(second)
    assert "already synchronized" in second.stdout
    assert _remote_sha(remote, "refs/tags/4.1") == expected_object
    assert _remote_sha(remote, "refs/tags/4.1^{}") == expected_commit


@pytest.mark.parametrize("tag", ["04.1", "4.01", "v4.1", "research-os-final-20260828"])
def test_publish_tag_rejects_noncanonical_tags(tmp_path: Path, tag: str) -> None:
    repo, remote, script = _make_release_repo(tmp_path)

    completed = _publish(repo, script, tag=tag)

    assert completed.returncode != 0
    assert "canonical major.minor" in _combined(completed)
    assert _remote_sha(remote, f"refs/tags/{tag}") == ""


@pytest.mark.parametrize("target", ["pyproject", "package"])
def test_publish_tag_rejects_version_mismatch(tmp_path: Path, target: str) -> None:
    repo, remote, script = _make_release_repo(tmp_path)
    path = repo / ("pyproject.toml" if target == "pyproject" else "src/factor_lab/__init__.py")
    path.write_text(path.read_text(encoding="utf-8").replace("4.1.0", "4.1.1"), encoding="utf-8")
    _commit_and_push(repo, f"break {target} version")

    completed = _publish(repo, script)

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
    repo, remote, script = _make_release_repo(tmp_path)
    (repo / "CHANGELOG.md").write_text(changelog, encoding="utf-8")
    _commit_and_push(repo, "break changelog")

    completed = _publish(repo, script)

    assert completed.returncode != 0
    assert "CHANGELOG.md" in _combined(completed)
    assert _remote_sha(remote, "refs/tags/4.1") == ""


@pytest.mark.parametrize("token", ["passed", "compileall", "SHA-256", "pip check", "CLI"])
def test_publish_tag_rejects_missing_local_verification_record(
    tmp_path: Path, token: str
) -> None:
    repo, remote, script = _make_release_repo(tmp_path)
    path = repo / "CHANGELOG.md"
    path.write_text(
        path.read_text(encoding="utf-8").replace(token, "missing"),
        encoding="utf-8",
    )
    _commit_and_push(repo, f"remove local verification token {token}")

    completed = _publish(repo, script)

    assert completed.returncode != 0
    assert "local tests" in _combined(completed)
    assert _remote_sha(remote, "refs/tags/4.1") == ""


def test_production_publisher_forbids_ci_and_pins_github_transport() -> None:
    source = PUBLISH_SCRIPT.read_text(encoding="utf-8")
    assert "Invoke-Gh" not in source
    assert "gh run" not in source
    assert "ci.yml" not in source
    assert "ssh://git@ssh.github.com:443/" in source
    assert "codex_github_ed25519" in source
    assert "github_proxy.py" in source
    assert "127\\.0\\.0\\.1" in source
    assert "7890" in source
    workflows = ROOT / ".github" / "workflows"
    assert not workflows.exists() or not any(workflows.iterdir())
