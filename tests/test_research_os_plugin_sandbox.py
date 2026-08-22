from pathlib import Path
import subprocess

import pytest

from factor_lab.research_os.plugin_sandbox import (
    PluginManifest,
    PluginSandboxError,
    PluginSandboxPolicy,
    build_docker_create_command,
    run_plugin,
)


IMAGE = "example/factor-plugin@sha256:" + "a" * 64


def test_sandbox_command_has_no_network_and_no_repository_mount(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    source = repository / "frozen.arrow"
    source.write_bytes(b"arrow-placeholder")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    command = build_docker_create_command(
        PluginManifest(
            plugin_id="quality-v1",
            image=IMAGE,
            command=("python", "/app/plugin.py"),
            declared_dependencies=("numpy", "pyarrow"),
        ),
        input_arrow=source,
        output_dir=scratch,
        container_name="factor-lab-plugin-test",
        policy=PluginSandboxPolicy(),
    )
    joined = " ".join(command)
    assert "--network none" in joined
    assert "--read-only" in command
    assert "--cap-drop ALL" in joined
    assert "no-new-privileges:true" in joined
    assert f"src={repository}," not in joined
    assert f"src={source},dst=/input/features.arrow,readonly" in joined
    assert "--log-driver none" in joined
    assert "type=tmpfs,destination=/output" in joined
    assert str(scratch) not in joined
    assert "--attach" not in command


def test_plugin_image_and_dependencies_are_fail_closed() -> None:
    policy = PluginSandboxPolicy()
    with pytest.raises(ValueError, match="sha256"):
        PluginManifest("bad", "example/plugin:latest", ("run",)).validate(policy)
    with pytest.raises(ValueError, match="not allowed"):
        PluginManifest(
            "bad-dependency", IMAGE, ("run",), declared_dependencies=("requests",)
        ).validate(policy)


def test_plugin_cannot_mount_arrow_file_outside_authorized_snapshot_root(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    outside = tmp_path / "outside.arrow"
    outside.write_bytes(b"not-authorized")

    with pytest.raises(PluginSandboxError, match="inside repository_root"):
        run_plugin(
            PluginManifest("blocked", IMAGE, ("run",)),
            outside,
            repository_root=repository,
            runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("Docker must not be invoked")
            ),
        )


def _fake_runner(output_payload: bytes, *, extra_file: bool = False):
    calls = []

    def runner(command, **_kwargs):
        argv = list(command)
        calls.append(argv)
        stdout = ""
        if argv[1] == "wait":
            stdout = "0\n"
        elif argv[1] == "cp":
            output_dir = Path(argv[-1])
            (output_dir / "result.arrow").write_bytes(output_payload)
            if extra_file:
                (output_dir / "unexpected.txt").write_text("blocked", encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    return runner, calls


def _arrow_payload() -> bytes:
    import io
    import pyarrow as pa
    import pyarrow.feather as feather

    sink = io.BytesIO()
    feather.write_feather(pa.table({"factor": [1.0]}), sink)
    return sink.getvalue()


def test_plugin_runner_disables_logs_and_copies_only_capped_arrow_result(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    source = repository / "frozen.arrow"
    source.write_bytes(b"authorized-input")
    runner, calls = _fake_runner(_arrow_payload())

    result = run_plugin(
        PluginManifest("quality-v1", IMAGE, ("python", "/app/plugin.py")),
        source,
        repository_root=repository,
        runner=runner,
        policy=PluginSandboxPolicy(maximum_output_bytes=1024 * 1024),
    )

    assert result.output_ipc
    flattened = [item for command in calls for item in command]
    assert "--attach" not in flattened
    assert "logs" not in flattened
    create = calls[0]
    assert "none" in create[create.index("--log-driver") + 1 :]
    assert any("tmpfs-size=1048576" in item for item in create)


def test_plugin_runner_rejects_any_extra_output_file(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    source = repository / "frozen.arrow"
    source.write_bytes(b"authorized-input")
    runner, _ = _fake_runner(_arrow_payload(), extra_file=True)

    with pytest.raises(PluginSandboxError, match="only result.arrow"):
        run_plugin(
            PluginManifest("quality-v1", IMAGE, ("python", "/app/plugin.py")),
            source,
            repository_root=repository,
            runner=runner,
        )
