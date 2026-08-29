from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import pytest


pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows watchdog integration")

ROOT = Path(__file__).resolve().parents[2]
INVOKE = ROOT / "scripts" / "invoke-prospective-watchdog.ps1"
REGISTER = ROOT / "scripts" / "register-prospective-watchdog.ps1"
CONTRACT = "factor-lab/prospective-readiness/5.9"


def _pwsh() -> str:
    executable = shutil.which("pwsh")
    if executable is None:
        pytest.skip("PowerShell 7 is not installed")
    return executable


@pytest.fixture(scope="session")
def fake_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("watchdog-fake-python")
    source = directory / "Program.cs"
    project = directory / "fake-python.csproj"
    executable = directory / "bin/Release/net8.0/fake-python.exe"
    source.write_text(
        r'''
using System;
using System.IO;
using System.Text;
using System.Threading;

public static class FakePython {
    public static int Main(string[] args) {
        string root = null;
        for (int i = 0; i + 1 < args.Length; i++) {
            if (args[i] == "--root") { root = args[i + 1]; break; }
        }
        if (root == null) { return 97; }
        string counterPath = Path.Combine(root, "fake-counter.txt");
        string responsesPath = Path.Combine(root, "fake-responses.txt");
        string argvPath = Path.Combine(root, "fake-argv.txt");
        int index;
        string response;
        using (var counter = new FileStream(counterPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None)) {
            using (var reader = new StreamReader(counter, Encoding.UTF8, false, 1024, true)) {
                string old = reader.ReadToEnd();
                index = String.IsNullOrWhiteSpace(old) ? 0 : Int32.Parse(old);
            }
            string[] lines = File.ReadAllLines(responsesPath, Encoding.UTF8);
            if (index >= lines.Length) { return 98; }
            response = lines[index];
            counter.Position = 0;
            counter.SetLength(0);
            byte[] next = Encoding.UTF8.GetBytes((index + 1).ToString());
            counter.Write(next, 0, next.Length);
            counter.Flush(true);
            var encoded = new string[args.Length];
            for (int i = 0; i < args.Length; i++) {
                encoded[i] = Convert.ToBase64String(Encoding.UTF8.GetBytes(args[i]));
            }
            File.AppendAllText(argvPath, String.Join(",", encoded) + Environment.NewLine, new UTF8Encoding(false));
        }
        string[] fields = response.Split(new char[] {'|'}, 4);
        int exitCode = Int32.Parse(fields[0]);
        int sleepMilliseconds = Int32.Parse(fields[1]);
        if (sleepMilliseconds > 0) { Thread.Sleep(sleepMilliseconds); }
        Console.OutputEncoding = new UTF8Encoding(false);
        Console.Error.Write(Encoding.UTF8.GetString(Convert.FromBase64String(fields[3])));
        Console.Write(Encoding.UTF8.GetString(Convert.FromBase64String(fields[2])));
        return exitCode;
    }
}
''',
        encoding="utf-8",
    )
    project.write_text(
        """<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <AssemblyName>fake-python</AssemblyName>
    <ImplicitUsings>disable</ImplicitUsings>
    <Nullable>disable</Nullable>
  </PropertyGroup>
</Project>
""",
        encoding="utf-8",
    )
    completed = subprocess.run(
        ["dotnet", "build", str(project), "-c", "Release", "--nologo"],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    assert executable.is_file()
    return executable


def _report(status: str, *, action: list[str] | None = None, complete: bool = False) -> str:
    exit_phase = "awaiting_execution" if complete else "decision_generation"
    return json.dumps(
        {
            "schema_version": 2,
            "kind": "prospective_readiness",
            "contract_id": CONTRACT,
            "status": status,
            "reason": f"test_{status}",
            "action": None if action is None else {"command": "test action", "argv": action},
            "ledger": {
                "decision_count": 1 if complete else 0,
                "last_decision_signal_date": "2026-08-31" if complete else None,
                "phase": exit_phase,
            },
        },
        separators=(",", ":"),
    )


def _later_cycle_report(status: str) -> str:
    return json.dumps(
        {
            "schema_version": 2,
            "kind": "prospective_readiness",
            "contract_id": CONTRACT,
            "status": status,
            "reason": f"later_cycle_{status}",
            "action": None,
            "ledger": {
                "decision_count": 2,
                "last_decision_signal_date": "2026-09-01",
                "phase": "awaiting_receipt",
            },
        },
        separators=(",", ":"),
    )


def _response(exit_code: int, stdout: str, *, stderr: str = "", sleep_ms: int = 0) -> str:
    encode = lambda value: base64.b64encode(value.encode()).decode()
    return f"{exit_code}|{sleep_ms}|{encode(stdout)}|{encode(stderr)}"


def _shadow_report(status: str, *, action: str | None = None) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "status": status,
            "reason": f"test_shadow_{status}",
            "action": action,
        },
        separators=(",", ":"),
    )


def _write_responses(root: Path, responses: list[str]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "fake-responses.txt").write_text("\n".join(responses) + "\n", encoding="utf-8")


def _window_arguments(*, soft_passed: bool = False) -> list[str]:
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    soft = now - timedelta(minutes=1) if soft_passed else now + timedelta(hours=1)
    return [
        "-NotBeforeUtc", (now - timedelta(hours=1)).isoformat(),
        "-SoftDeadlineUtc", soft.isoformat(),
        "-NotAfterUtc", (now + timedelta(hours=2)).isoformat(),
    ]


def _invoke(root: Path, fake_python: Path, *extra: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            _pwsh(), "-NoLogo", "-NoProfile", "-NonInteractive",
            "-File", str(INVOKE),
            "-ProjectRoot", str(root),
            "-RuntimePython", str(fake_python),
            "-Origin", "heartbeat",
            *_window_arguments(),
            *extra,
        ],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def _argv_calls(root: Path) -> list[list[str]]:
    calls = []
    for line in (root / "fake-argv.txt").read_text(encoding="utf-8").splitlines():
        calls.append([base64.b64decode(item).decode() for item in line.split(",")])
    return calls


def _run_records(root: Path) -> list[dict[str, object]]:
    files = list((root / "runtime/operations/prospective-watchdog-5.9/runs").glob("*.jsonl"))
    assert len(files) == 1
    return [json.loads(line) for line in files[0].read_text(encoding="utf-8").splitlines()]


def test_executes_action_argv_element_for_element_and_redacts_output(
    tmp_path: Path, fake_python: Path
) -> None:
    action = ["prospective", "attest", "value with spaces", 'x"y', "--literal=$()&;"]
    _write_responses(
        tmp_path,
        [
            _response(0, _report("ready", action=action), stderr="readiness-secret-token"),
            _response(0, "action-secret-token", stderr="action-stderr-secret-token"),
            _response(2, _report("waiting")),
        ],
    )

    completed = _invoke(tmp_path, fake_python)

    assert completed.returncode == 2, completed.stderr
    calls = _argv_calls(tmp_path)
    prefix = ["-I", "-m", "factor_lab.cli", "--root", str(tmp_path)]
    assert calls == [
        [*prefix, "prospective", "readiness"],
        [*prefix, *action],
        [*prefix, "prospective", "readiness"],
    ]
    log_text = json.dumps(_run_records(tmp_path))
    assert "secret-token" not in log_text
    assert all(record["origin"] == "heartbeat" for record in _run_records(tmp_path))
    process_records = [row for row in _run_records(tmp_path) if row["event"] == "process"]
    assert all("stdout_sha256" in row and "stdout_length_bytes" in row for row in process_records)
    assert all("stderr_sha256" in row and "stderr_length_bytes" in row for row in process_records)
    dispatch = next(row for row in _run_records(tmp_path) if row["event"] == "action_dispatch")
    assert dispatch["action_argv"] == action


def test_waiting_formal_controller_advances_shadow_until_it_waits(
    tmp_path: Path,
    fake_python: Path,
) -> None:
    (tmp_path / "runtime/adaptive-shadow/1").mkdir(parents=True)
    _write_responses(
        tmp_path,
        [
            _response(2, _report("waiting", complete=True)),
            _response(0, _shadow_report("planned", action="plan")),
            _response(0, _shadow_report("advanced", action="outcome")),
            _response(2, _shadow_report("waiting")),
        ],
    )

    completed = _invoke(tmp_path, fake_python)

    assert completed.returncode == 2, completed.stderr
    prefix = ["-I", "-m", "factor_lab.cli", "--root", str(tmp_path)]
    assert _argv_calls(tmp_path) == [
        [*prefix, "prospective", "readiness"],
        [*prefix, "adaptive-shadow", "sync"],
        [*prefix, "adaptive-shadow", "sync"],
        [*prefix, "adaptive-shadow", "sync"],
    ]
    observations = [
        row for row in _run_records(tmp_path) if row["event"] == "shadow_observation"
    ]
    assert [row["status"] for row in observations] == [
        "planned",
        "advanced",
        "waiting",
    ]
    assert _run_records(tmp_path)[-1]["reason"] == "readiness_waiting_shadow_waiting"


def test_shadow_blocked_or_exit_contract_mismatch_fails_closed(
    tmp_path: Path,
    fake_python: Path,
) -> None:
    (tmp_path / "runtime/adaptive-shadow/1").mkdir(parents=True)
    _write_responses(
        tmp_path,
        [
            _response(2, _report("waiting", complete=True)),
            _response(3, _shadow_report("blocked")),
        ],
    )

    completed = _invoke(tmp_path, fake_python)

    assert completed.returncode == 3, completed.stderr
    alert = next(
        (tmp_path / "runtime/operations/prospective-watchdog-5.9/alerts").glob("*.json")
    )
    assert json.loads(alert.read_text(encoding="utf-8"))["reason"] == "shadow_sync_blocked"


def test_shadow_action_limit_is_shared_with_formal_actions(
    tmp_path: Path,
    fake_python: Path,
) -> None:
    (tmp_path / "runtime/adaptive-shadow/1").mkdir(parents=True)
    _write_responses(
        tmp_path,
        [
            _response(0, _report("ready", action=["formal", "action"])),
            _response(0, "ok"),
            _response(2, _report("waiting", complete=True)),
        ],
    )

    completed = _invoke(tmp_path, fake_python, "-MaxActions", "1")

    assert completed.returncode == 3, completed.stderr
    assert len(_argv_calls(tmp_path)) == 3
    assert _run_records(tmp_path)[-1]["reason"] == "max_actions_exhausted_while_shadow_ready"


@pytest.mark.parametrize("exit_code,status", [(2, "waiting"), (3, "blocked"), (4, "terminal")])
def test_propagates_readiness_exit_codes_and_only_alerts_for_three_or_four(
    tmp_path: Path, fake_python: Path, exit_code: int, status: str
) -> None:
    _write_responses(tmp_path, [_response(exit_code, _report(status))])

    completed = _invoke(tmp_path, fake_python)

    assert completed.returncode == exit_code
    alerts = list((tmp_path / "runtime/operations/prospective-watchdog-5.9/alerts").glob("*.json"))
    assert len(alerts) == (1 if exit_code in {3, 4} else 0)
    if alerts:
        assert json.loads(alerts[0].read_text(encoding="utf-8"))["exit_code"] == exit_code


def test_twelve_action_limit_has_a_thirteenth_observation_only(
    tmp_path: Path, fake_python: Path
) -> None:
    responses: list[str] = []
    for index in range(12):
        responses.append(_response(0, _report("ready", action=["action", str(index)])))
        responses.append(_response(0, "ok"))
    responses.append(_response(0, _report("ready", action=["must", "not", "run"])))
    _write_responses(tmp_path, responses)

    completed = _invoke(tmp_path, fake_python)

    assert completed.returncode == 3, completed.stderr
    calls = _argv_calls(tmp_path)
    assert len(calls) == 25
    assert sum(call[-2:-1] == ["action"] for call in calls) == 12
    assert all("must" not in call for call in calls)
    assert json.loads((tmp_path / "fake-counter.txt").read_text()) == 25
    alert = next((tmp_path / "runtime/operations/prospective-watchdog-5.9/alerts").glob("*.json"))
    assert json.loads(alert.read_text(encoding="utf-8"))["reason"] == "max_actions_exhausted_while_ready"


@pytest.mark.parametrize(
    "stdout",
    ["not-json", json.dumps({"kind": "prospective_readiness", "contract_id": "wrong"})],
)
def test_bad_json_or_contract_fails_closed_with_create_only_alert(
    tmp_path: Path, fake_python: Path, stdout: str
) -> None:
    _write_responses(tmp_path, [_response(0, stdout)])

    completed = _invoke(tmp_path, fake_python)

    assert completed.returncode == 3
    alerts = list((tmp_path / "runtime/operations/prospective-watchdog-5.9/alerts").glob("*.json"))
    assert len(alerts) == 1
    assert json.loads(alerts[0].read_text(encoding="utf-8"))["reason"] == "invalid_readiness_json_or_contract"


def test_controller_lock_makes_second_runner_wait(
    tmp_path: Path, fake_python: Path
) -> None:
    _write_responses(tmp_path, [_response(2, _report("waiting"), sleep_ms=3000)])
    command = [
        _pwsh(), "-NoProfile", "-File", str(INVOKE),
        "-ProjectRoot", str(tmp_path), "-RuntimePython", str(fake_python),
        *_window_arguments(),
    ]
    first = subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
    )
    lock = tmp_path / "runtime/operations/prospective-watchdog-5.9/controller.lock"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not lock.exists():
        time.sleep(0.05)
    assert lock.exists()

    second = subprocess.run(
        command,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
    )
    first_stdout, first_stderr = first.communicate(timeout=10)

    assert second.returncode == 2, second.stderr
    assert first.returncode == 2, first_stderr or first_stdout
    assert json.loads((tmp_path / "fake-counter.txt").read_text()) == 1


def test_outside_window_is_noop_even_when_runtime_does_not_exist(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            _pwsh(), "-NoProfile", "-File", str(INVOKE),
            "-ProjectRoot", str(tmp_path / "missing-root"),
            "-RuntimePython", str(tmp_path / "missing-python.exe"),
            "-NotBeforeUtc", "2020-01-01T00:00:00Z",
            "-SoftDeadlineUtc", "2020-01-01T00:30:00Z",
            "-NotAfterUtc", "2020-01-01T01:00:00Z",
        ],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr
    assert not (tmp_path / "missing-root").exists()


def test_soft_deadline_is_checked_after_waiting_and_requires_receipt_completion(
    tmp_path: Path, fake_python: Path
) -> None:
    _write_responses(tmp_path, [_response(2, _report("waiting"))])
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    completed = subprocess.run(
        [
            _pwsh(), "-NoProfile", "-File", str(INVOKE),
            "-ProjectRoot", str(tmp_path), "-RuntimePython", str(fake_python),
            "-NotBeforeUtc", (now - timedelta(hours=2)).isoformat(),
            "-SoftDeadlineUtc", (now - timedelta(minutes=1)).isoformat(),
            "-NotAfterUtc", (now + timedelta(hours=1)).isoformat(),
        ],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )

    assert completed.returncode == 3
    alert = next((tmp_path / "runtime/operations/prospective-watchdog-5.9/alerts").glob("*.json"))
    assert json.loads(alert.read_text(encoding="utf-8"))["reason"] == "first_decision_soft_deadline_unmet"


@pytest.mark.parametrize("exit_code,status", [(2, "waiting"), (4, "terminal")])
def test_continuous_mode_ignores_expired_first_cycle_window_and_uses_readiness_status(
    tmp_path: Path, fake_python: Path, exit_code: int, status: str
) -> None:
    _write_responses(tmp_path, [_response(exit_code, _later_cycle_report(status))])

    completed = subprocess.run(
        [
            _pwsh(), "-NoProfile", "-File", str(INVOKE),
            "-ProjectRoot", str(tmp_path), "-RuntimePython", str(fake_python),
            "-ControllerMode", "continuous",
            "-NotBeforeUtc", "2020-01-01T00:00:00Z",
            "-SoftDeadlineUtc", "2020-01-01T00:30:00Z",
            "-NotAfterUtc", "2020-01-01T01:00:00Z",
        ],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )

    assert completed.returncode == exit_code, completed.stderr
    records = _run_records(tmp_path)
    assert all(record["controller_mode"] == "continuous" for record in records)
    start = next(record for record in records if record["event"] == "start")
    assert start["first_cycle_window_enforced"] is False
    assert start["local_time_zone_id"] == "China Standard Time"
    finish = next(record for record in records if record["event"] == "finish")
    assert finish["reason"] == f"readiness_{status}"
    alerts = list((tmp_path / "runtime/operations/prospective-watchdog-5.9/alerts").glob("*.json"))
    assert len(alerts) == (1 if exit_code == 4 else 0)
    if alerts:
        assert json.loads(alerts[0].read_text(encoding="utf-8"))["controller_mode"] == "continuous"


def test_process_timeout_kills_tree_and_fails_closed(tmp_path: Path, fake_python: Path) -> None:
    _write_responses(tmp_path, [_response(2, _report("waiting"), sleep_ms=5000)])
    started = time.monotonic()

    completed = _invoke(tmp_path, fake_python, "-CommandTimeoutSeconds", "1", timeout=15)

    assert completed.returncode == 3
    assert time.monotonic() - started < 5
    process_record = next(row for row in _run_records(tmp_path) if row["event"] == "process")
    assert process_record["timed_out"] is True
    assert list((tmp_path / "runtime/operations/prospective-watchdog-5.9/alerts").glob("*.json"))


def test_register_fails_when_annotated_release_capsule_is_missing(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-b", "main", str(tmp_path)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.email", "test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.name", "Test"], check=True)
    marker = tmp_path / "marker.txt"
    marker.write_text("release\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(tmp_path), "add", "marker.txt"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-m", "release"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(tmp_path), "tag", "-a", "5.9", "-m", "5.9"], check=True)
    runtime = tmp_path / "runtime-python.exe"
    runtime.write_bytes(b"placeholder")

    completed = subprocess.run(
        [
            _pwsh(), "-NoProfile", "-File", str(REGISTER),
            "-ProjectRoot", str(tmp_path), "-RuntimePython", str(runtime),
        ],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )

    assert completed.returncode != 0
    assert "release capsule" in completed.stderr


def test_register_continuous_plan_has_weekday_deadlines_and_weekend_recovery(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-b", "main", str(tmp_path)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.email", "test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.name", "Test"], check=True)
    marker = tmp_path / "marker.txt"
    marker.write_text("release\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(tmp_path), "add", "marker.txt"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-m", "release"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(tmp_path), "tag", "-a", "5.9", "-m", "5.9"], check=True)
    release_commit = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "5.9^{commit}"],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    capsule_script = (
        tmp_path
        / "runtime/prospective/5.0/release-runners"
        / release_commit
        / "scripts/invoke-prospective-watchdog.ps1"
    )
    capsule_script.parent.mkdir(parents=True)
    shutil.copyfile(INVOKE, capsule_script)
    runtime = tmp_path / "runtime-python.exe"
    runtime.write_bytes(b"placeholder")

    completed = subprocess.run(
        [
            _pwsh(), "-NoProfile", "-File", str(REGISTER),
            "-ProjectRoot", str(tmp_path), "-RuntimePython", str(runtime),
            "-ControllerMode", "continuous", "-PlanOnly",
        ],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )

    assert completed.returncode == 0, completed.stderr
    plan = json.loads(completed.stdout)
    assert plan["registered"] is False
    assert plan["task_name"] == "Factor Lab Prospective Continuous Watchdog 5.9"
    assert plan["release_tag"] == "5.9"
    assert plan["controller_mode"] == "continuous"
    assert plan["schedule_kind"] == "continuous_with_weekend_recovery"
    assert plan["multiple_instances"] == "Parallel"
    assert plan["trigger_count"] == 35
    assert plan["local_time_zone_id"] == "China Standard Time"
    assert plan["logon_trigger_enabled"] is True
    assert plan["scheduled_days"] == [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
    ]
    assert "-ControllerMode continuous" in plan["action_arguments"]
    assert plan["scheduled_times_local"] == [
        "00:30",
        *[f"{hour:02d}:{minute:02d}" for hour, minute in (
            divmod(value, 60) for value in range(475, 556, 5)
        )],
        "15:00", "16:30",
        "17:30", "18:00", "18:30", "19:00", "19:30", "20:00", "20:30",
        "23:30",
    ]
    assert plan["weekend_scheduled_days"] == ["Saturday", "Sunday"]
    assert plan["weekend_scheduled_times_local"] == [
        "00:30", "08:30", "16:30", "18:00", "20:30", "23:30",
    ]


def test_register_rejects_continuous_mode_for_first_cycle_capsule(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            _pwsh(), "-NoProfile", "-File", str(REGISTER),
            "-ProjectRoot", str(tmp_path / "missing"),
            "-ReleaseTag", "5.7", "-ControllerMode", "continuous", "-PlanOnly",
        ],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
    )

    assert completed.returncode != 0
    assert "requires release tag 5.8 or newer" in completed.stderr
