from datetime import datetime, timezone
import subprocess

import pytest

from factor_lab.research_os.contracts import (
    DataSnapshotRef,
    EnvironmentRef,
    EvaluationInputBindings,
    ExperimentSpec,
    FactorSpec,
    PortfolioPolicy,
    Preregistration,
)
from factor_lab.research_os.fingerprint import (
    canonical_json,
    capture_environment,
    content_fingerprint,
    experiment_fingerprint,
    file_tree_fingerprint,
)


NOW = datetime(2026, 8, 22, tzinfo=timezone.utc)


def experiment(**updates: object) -> ExperimentSpec:
    values = {
        "snapshot": DataSnapshotRef(
            snapshot_id="snapshot-1",
            tier="gold",
            uri="s3://factor-lab/gold/1",
            content_hash="a" * 64,
            as_of=NOW,
        ),
        "factor": FactorSpec(
            factor_id="value-1",
            family="value",
            name="Value",
            mechanism="Overreaction mean reversion.",
            expression={"op": "rank", "input": "book_yield"},
            direction="higher_is_better",
            falsification_criteria=("negative outer OOS excess",),
        ),
        "evaluator_version": "portfolio-v1",
        "environment": EnvironmentRef(
            code_hash="b" * 64,
            dependency_lock_hash="c" * 64,
            configuration_hash="d" * 64,
            python_version="3.10",
            platform="Windows-AMD64",
            evaluator_build="portfolio-v1",
        ),
        "preregistration": Preregistration(
            hypothesis_id="hyp-1",
            economic_mechanism="Overreaction mean reversion.",
            direction="positive",
            falsification_criteria=("negative outer OOS excess",),
            stop_rules=("two diagnostics",),
        ),
    }
    values.update(updates)
    return ExperimentSpec(**values)


def test_canonical_fingerprint_ignores_mapping_and_set_iteration_order() -> None:
    left = {"中文": {"b", "a"}, "nested": {"z": 1, "a": 2}}
    right = {"nested": {"a": 2, "z": 1}, "中文": {"a", "b"}}

    assert canonical_json(left) == canonical_json(right)
    assert content_fingerprint(left) == content_fingerprint(right)


def test_experiment_fingerprint_changes_for_result_affecting_inputs() -> None:
    base = experiment()
    changed_policy = experiment(portfolio=PortfolioPolicy(capital=60_000_000))
    changed_evaluator = experiment(evaluator_version="portfolio-v2")
    changed_risk_input = experiment(
        evaluation_inputs=EvaluationInputBindings(exposure_frame_hash="e" * 64)
    )

    assert experiment_fingerprint(base) == experiment_fingerprint(base.model_dump())
    assert experiment_fingerprint(base) != experiment_fingerprint(changed_policy)
    assert experiment_fingerprint(base) != experiment_fingerprint(changed_evaluator)
    assert experiment_fingerprint(base) != experiment_fingerprint(changed_risk_input)


def test_canonical_fingerprint_rejects_nonportable_numbers_and_naive_time() -> None:
    with pytest.raises(ValueError, match="non-finite"):
        content_fingerprint({"value": float("nan")})
    with pytest.raises(ValueError, match="naive"):
        content_fingerprint({"time": datetime(2026, 8, 22)})
    with pytest.raises(ValueError, match="collide"):
        content_fingerprint({1: "numeric", "1": "string"})


def test_file_tree_fingerprint_is_order_independent(tmp_path) -> None:
    first = tmp_path / "a.txt"
    second = tmp_path / "b.txt"
    first.write_text("alpha", encoding="utf-8")
    second.write_text("beta", encoding="utf-8")

    assert file_tree_fingerprint([first, second], base_dir=tmp_path) == (
        file_tree_fingerprint([second, first], base_dir=tmp_path)
    )


def test_environment_capture_hashes_untracked_code_but_not_gitignored_secrets(
    tmp_path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()

    def git(*args: str) -> None:
        subprocess.run(
            ["git", *args],
            cwd=repository,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    git("init")
    git("config", "user.email", "research-os@example.invalid")
    git("config", "user.name", "Research OS Test")
    (repository / ".gitignore").write_text(".env\n", encoding="utf-8")
    (repository / "tracked.py").write_text("VERSION = 1\n", encoding="utf-8")
    lock = repository / "uv.lock"
    lock.write_text("lock-version = 1\n", encoding="utf-8")
    git("add", ".gitignore", "tracked.py", "uv.lock")
    git("commit", "-m", "initial")

    untracked = repository / "new_research_code.py"
    untracked.write_text("VALUE = 1\n", encoding="utf-8")
    secret = repository / ".env"
    secret.write_text("TOKEN=first-secret\n", encoding="utf-8")

    first = capture_environment(
        repository,
        dependency_lock=lock,
        configuration={"mode": "test"},
        evaluator_build="test-v1",
    )
    secret.write_text("TOKEN=second-secret\n", encoding="utf-8")
    ignored_secret_changed = capture_environment(
        repository,
        dependency_lock=lock,
        configuration={"mode": "test"},
        evaluator_build="test-v1",
    )
    untracked.write_text("VALUE = 2\n", encoding="utf-8")
    code_changed = capture_environment(
        repository,
        dependency_lock=lock,
        configuration={"mode": "test"},
        evaluator_build="test-v1",
    )

    assert first.dirty_patch_hash is not None
    assert first.dirty_patch_hash == ignored_secret_changed.dirty_patch_hash
    assert first.dirty_patch_hash != code_changed.dirty_patch_hash
