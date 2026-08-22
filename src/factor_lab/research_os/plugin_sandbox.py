"""Docker-isolated escape hatch for mechanisms not expressible in the DSL.

Plugins receive one Arrow IPC file and must produce one Arrow IPC file.  They
never receive a repository mount, network namespace, host credentials or a
writable root filesystem.  The host validates the result before it can enter a
research snapshot; plugins therefore cannot mutate Factor Lab itself.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Callable, Sequence
from uuid import uuid4


_DIGEST_IMAGE = re.compile(r"^[^\s]+@sha256:[0-9a-f]{64}$")
_ARROW_SUFFIXES = {".arrow", ".feather", ".ipc"}
_RESULT_NAME = "result.arrow"
_MAX_HOST_DIAGNOSTIC_CHARS = 4096


class PluginSandboxError(RuntimeError):
    pass


@dataclass(frozen=True)
class PluginSandboxPolicy:
    cpu_limit: float = 1.0
    memory_limit: str = "1g"
    timeout_seconds: int = 300
    pids_limit: int = 128
    maximum_output_bytes: int = 512 * 1024 * 1024
    allowed_dependencies: tuple[str, ...] = (
        "numpy",
        "pyarrow",
        "polars",
        "scipy",
        "scikit-learn",
    )

    def __post_init__(self) -> None:
        if self.cpu_limit <= 0 or self.timeout_seconds <= 0 or self.pids_limit <= 0:
            raise ValueError("sandbox resource limits must be positive")
        if self.maximum_output_bytes <= 0:
            raise ValueError("maximum_output_bytes must be positive")


@dataclass(frozen=True)
class PluginManifest:
    plugin_id: str
    image: str
    command: tuple[str, ...]
    declared_dependencies: tuple[str, ...] = ()

    def validate(self, policy: PluginSandboxPolicy) -> None:
        if not self.plugin_id.strip():
            raise ValueError("plugin_id is required")
        if not _DIGEST_IMAGE.fullmatch(self.image):
            raise ValueError("plugin image must be pinned by sha256 digest")
        if not self.command or any(not str(item) for item in self.command):
            raise ValueError("plugin command must be a non-empty argv tuple")
        forbidden = sorted(set(self.declared_dependencies) - set(policy.allowed_dependencies))
        if forbidden:
            raise ValueError(f"plugin dependencies are not allowed: {forbidden}")


@dataclass(frozen=True)
class PluginSandboxResult:
    plugin_id: str
    image: str
    output_ipc: bytes
    output_sha256: str
    stdout: str
    stderr: str


RunCommand = Callable[..., subprocess.CompletedProcess[str]]


def _resolved_file(path: str | Path, *, name: str) -> Path:
    result = Path(path).resolve()
    if not result.is_file():
        raise FileNotFoundError(result)
    if result.is_symlink():
        raise PluginSandboxError(f"{name} cannot be a symlink")
    if result.suffix.lower() not in _ARROW_SUFFIXES:
        raise PluginSandboxError(f"{name} must be an Arrow IPC/Feather file")
    return result


def build_docker_create_command(
    manifest: PluginManifest,
    *,
    input_arrow: Path,
    output_dir: Path,
    container_name: str,
    policy: PluginSandboxPolicy,
) -> tuple[str, ...]:
    """Build a shell-free, least-privilege Docker create invocation."""

    manifest.validate(policy)
    # ``output_dir`` remains part of the public builder signature for callers
    # that allocate host scratch space, but it is intentionally not mounted.
    # Plugin output lives in a size-capped container tmpfs and is copied out
    # only after the container stops.
    del output_dir
    return (
        "docker",
        "create",
        "--name",
        container_name,
        "--network",
        "none",
        "--log-driver",
        "none",
        "--read-only",
        "--cpus",
        str(policy.cpu_limit),
        "--memory",
        policy.memory_limit,
        "--pids-limit",
        str(policy.pids_limit),
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--user",
        "65532:65532",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,nodev,size=64m",
        "--mount",
        f"type=bind,src={input_arrow},dst=/input/features.arrow,readonly",
        "--mount",
        (
            "type=tmpfs,destination=/output,"
            f"tmpfs-size={policy.maximum_output_bytes},tmpfs-mode=1777"
        ),
        "--env",
        "FACTOR_LAB_PLUGIN_INPUT=/input/features.arrow",
        "--env",
        "FACTOR_LAB_PLUGIN_OUTPUT=/output/result.arrow",
        manifest.image,
        *manifest.command,
    )


def run_plugin(
    manifest: PluginManifest,
    input_arrow: str | Path,
    *,
    repository_root: str | Path,
    policy: PluginSandboxPolicy = PluginSandboxPolicy(),
    runner: RunCommand = subprocess.run,
) -> PluginSandboxResult:
    """Run a pinned plugin container and return its validated Arrow bytes."""

    manifest.validate(policy)
    input_path = _resolved_file(input_arrow, name="input_arrow")
    repository = Path(repository_root).resolve()
    try:
        input_path.relative_to(repository)
    except ValueError as exc:
        raise PluginSandboxError(
            "input_arrow must be an explicitly exported snapshot inside repository_root"
        ) from exc
    # Reading a frozen Arrow export from the repository is safe because Docker
    # receives that single file read-only.  No directory or Git metadata is
    # mounted, and the output is created in the system temporary directory.
    container_name = f"factor-lab-plugin-{uuid4().hex}"
    created = False
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    with tempfile.TemporaryDirectory(prefix="factor-lab-plugin-") as temporary:
        output_dir = Path(temporary).resolve()
        try:
            output_dir.relative_to(repository)
        except ValueError:
            pass
        else:
            raise PluginSandboxError("plugin scratch directory must be outside the repository")
        command = build_docker_create_command(
            manifest,
            input_arrow=input_path,
            output_dir=output_dir,
            container_name=container_name,
            policy=policy,
        )
        try:
            create = runner(
                list(command),
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )
            created = True
            stdout_parts.append(create.stdout or "")
            stderr_parts.append(create.stderr or "")
            start = runner(
                ["docker", "start", container_name],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )
            stdout_parts.append(start.stdout or "")
            stderr_parts.append(start.stderr or "")
            wait = runner(
                ["docker", "wait", container_name],
                capture_output=True,
                text=True,
                check=True,
                timeout=policy.timeout_seconds,
            )
            stdout_parts.append(wait.stdout or "")
            stderr_parts.append(wait.stderr or "")
            try:
                container_exit_code = int(str(wait.stdout or "").strip().splitlines()[-1])
            except (IndexError, ValueError) as exc:
                raise PluginSandboxError("Docker returned an invalid plugin exit status") from exc
            if container_exit_code != 0:
                raise PluginSandboxError(
                    f"plugin container exited with status {container_exit_code}; logs are disabled"
                )
            copied = runner(
                ["docker", "cp", f"{container_name}:/output/.", str(output_dir)],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )
            stdout_parts.append(copied.stdout or "")
            stderr_parts.append(copied.stderr or "")
        except subprocess.TimeoutExpired as exc:
            if created:
                runner(
                    ["docker", "kill", container_name],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )
            raise PluginSandboxError(
                f"plugin exceeded {policy.timeout_seconds}s timeout"
            ) from exc
        except (OSError, subprocess.CalledProcessError) as exc:
            raise PluginSandboxError(f"plugin container failed: {exc}") from exc
        finally:
            if created:
                runner(
                    ["docker", "rm", "--force", container_name],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )

        entries = sorted(output_dir.rglob("*"))
        relative_entries = [entry.relative_to(output_dir).as_posix() for entry in entries]
        if relative_entries != [_RESULT_NAME]:
            raise PluginSandboxError(
                "plugin output directory may contain only result.arrow; found "
                + repr(relative_entries)
            )
        output = output_dir / _RESULT_NAME
        if not output.is_file() or output.is_symlink():
            raise PluginSandboxError("plugin did not produce a regular result.arrow")
        size = output.stat().st_size
        if size <= 0 or size > policy.maximum_output_bytes:
            raise PluginSandboxError(
                f"plugin output size {size} is outside the accepted range"
            )
        encoded = output.read_bytes()
        _validate_arrow_ipc(encoded)
        return PluginSandboxResult(
            plugin_id=manifest.plugin_id,
            image=manifest.image,
            output_ipc=encoded,
            output_sha256=hashlib.sha256(encoded).hexdigest(),
            stdout="".join(stdout_parts)[:_MAX_HOST_DIAGNOSTIC_CHARS],
            stderr="".join(stderr_parts)[:_MAX_HOST_DIAGNOSTIC_CHARS],
        )


def _validate_arrow_ipc(encoded: bytes) -> None:
    try:
        import pyarrow as pa
        import pyarrow.feather as feather
    except ImportError as exc:  # pragma: no cover - pyarrow is a core dependency.
        raise PluginSandboxError("pyarrow is required to validate plugin output") from exc
    source = pa.BufferReader(encoded)
    try:
        table = feather.read_table(source)
    except Exception:
        source.seek(0)
        try:
            with pa.ipc.open_file(source) as reader:
                table = reader.read_all()
        except Exception as exc:
            raise PluginSandboxError("plugin output is not valid Arrow IPC") from exc
    if table.num_columns == 0:
        raise PluginSandboxError("plugin output has no columns")


__all__ = [
    "PluginManifest",
    "PluginSandboxError",
    "PluginSandboxPolicy",
    "PluginSandboxResult",
    "build_docker_create_command",
    "run_plugin",
]
