from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import os
from pathlib import Path
from threading import Event, Lock
import time

import pytest

import factor_lab.research_os.object_store as object_store_module
from factor_lab.research_os.object_store import (
    ObjectStoreIntegrityError,
    S3ImmutableArchive,
)


class _MemoryFileSystem:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.objects

    def open(self, path: str, mode: str = "rb"):
        if mode == "rb":
            return io.BytesIO(self.objects[path])
        if mode != "wb":
            raise ValueError(mode)
        filesystem = self

        class _Writer(io.BytesIO):
            def close(self) -> None:
                filesystem.objects[path] = self.getvalue()
                super().close()

        return _Writer()


def test_archive_is_content_addressed_verified_and_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "accepted.parquet"
    source.write_bytes(b"immutable-parquet")
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)

    first = archive.archive_file(source, logical_path="silver/2026-08-22")
    second = archive.archive_file(source, logical_path="silver/2026-08-22")

    assert first.uri.startswith("s3://factor-lab/research-os/silver/2026-08-22/")
    assert f"sha256={first.sha256}" in first.key
    assert first.reused is False
    assert second.reused is True


def test_archive_fails_closed_on_existing_corrupt_object(tmp_path: Path) -> None:
    source = tmp_path / "daily.parquet"
    source.write_bytes(b"expected")
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    first = archive.archive_file(source, logical_path="bronze/vendor/daily")
    filesystem.objects[f"factor-lab/{first.key}"] = b"corrupt"

    with pytest.raises(ObjectStoreIntegrityError, match="immutable object differs"):
        archive.archive_file(source, logical_path="bronze/vendor/daily")


@pytest.mark.parametrize("logical_path", ["../escape", "/absolute", "a/../../b"])
def test_archive_rejects_unsafe_keys(tmp_path: Path, logical_path: str) -> None:
    source = tmp_path / "file.parquet"
    source.write_bytes(b"x")
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=_MemoryFileSystem())
    with pytest.raises(ValueError):
        archive.archive_file(source, logical_path=logical_path)


def test_restore_recovers_deleted_cache_and_is_idempotent_for_object_and_uri(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "lake" / "accepted.parquet"
    cache.parent.mkdir(parents=True)
    payload = b"immutable-parquet-from-minio"
    cache.write_bytes(payload)
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = archive.archive_file(cache, logical_path="silver/2026-08-22")

    cache.unlink()
    restored = archive.restore_file(archived, cache)
    reused = archive.restore_file(archived, cache)
    uri_target = tmp_path / "second-cache" / "accepted.parquet"
    restored_from_uri = archive.restore_file(
        archived.uri,
        uri_target,
        expected_sha256=archived.sha256,
        expected_size_bytes=archived.size_bytes,
    )

    assert cache.read_bytes() == uri_target.read_bytes() == payload
    assert hashlib.sha256(cache.read_bytes()).hexdigest() == archived.sha256
    assert restored.reused is False
    assert reused.reused is True
    assert restored_from_uri.reused is False
    assert restored.path == cache.resolve()


def test_restore_fails_closed_on_corrupt_remote_without_destination_or_temp(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"expected")
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = archive.archive_file(source, logical_path="bronze/tushare/daily")
    filesystem.objects[f"factor-lab/{archived.key}"] = b"corrupt"
    destination = tmp_path / "cache" / "daily.parquet"

    with pytest.raises(ObjectStoreIntegrityError, match="immutable object differs"):
        archive.restore_file(archived, destination)

    assert not destination.exists()
    assert list(destination.parent.glob(".daily.parquet.restore-*.tmp")) == []


def test_restore_preserves_existing_mismatch_and_leaves_no_temp(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"expected")
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = archive.archive_file(source, logical_path="silver/2026-08-22")
    destination = tmp_path / "cache" / "daily.parquet"
    destination.parent.mkdir()
    destination.write_bytes(b"local-conflict")

    with pytest.raises(ObjectStoreIntegrityError, match="existing restore destination differs"):
        archive.restore_file(archived, destination)

    assert destination.read_bytes() == b"local-conflict"
    assert list(destination.parent.glob(".daily.parquet.restore-*.tmp")) == []


def test_restore_rejects_missing_remote_and_unsafe_uri(tmp_path: Path) -> None:
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    digest = hashlib.sha256(b"missing").hexdigest()
    missing_uri = (
        "s3://factor-lab/research-os/bronze/"
        f"sha256={digest}/missing.parquet"
    )

    with pytest.raises(ObjectStoreIntegrityError, match="is missing"):
        archive.restore_file(
            missing_uri,
            tmp_path / "missing.parquet",
            expected_sha256=digest,
            expected_size_bytes=7,
        )
    with pytest.raises(ValueError, match="configured s3:// bucket"):
        archive.restore_file(
            missing_uri.replace("factor-lab", "other-bucket", 1),
            tmp_path / "foreign.parquet",
            expected_sha256=digest,
            expected_size_bytes=7,
        )
    with pytest.raises(ValueError, match="escapes"):
        archive.restore_file(
            f"s3://factor-lab/research-os/../escape/sha256={digest}/file",
            tmp_path / "escape.parquet",
            expected_sha256=digest,
            expected_size_bytes=7,
        )


def test_restore_rejects_symlink_target_and_parent_escape(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"expected")
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = archive.archive_file(source, logical_path="silver/2026-08-22")

    sentinel = tmp_path / "sentinel.parquet"
    sentinel.write_bytes(b"do-not-touch")
    linked_target = tmp_path / "linked-target.parquet"
    outside = tmp_path / "outside"
    outside.mkdir()
    linked_parent = tmp_path / "linked-parent"
    try:
        linked_target.symlink_to(sentinel)
        linked_parent.symlink_to(outside, target_is_directory=True)
    except OSError as exc:  # Windows without Developer Mode/admin symlink rights.
        pytest.skip(f"symlinks unavailable: {exc}")

    with pytest.raises(ValueError, match="symlink/reparse"):
        archive.restore_file(archived, linked_target)
    with pytest.raises(ValueError, match="symlink/reparse"):
        archive.restore_file(archived, linked_parent / "escaped.parquet")

    assert sentinel.read_bytes() == b"do-not-touch"
    assert not (outside / "escaped.parquet").exists()


def test_concurrent_restore_is_atomic_and_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    payload = b"bounded-concurrent-restore" * 1024
    source.write_bytes(payload)
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = archive.archive_file(source, logical_path="bronze/tushare/daily")
    peer_archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    destination = tmp_path / "cache" / "daily.parquet"

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda index: (archive, peer_archive)[index % 2].restore_file(
                    archived, destination
                ),
                range(16),
            )
        )

    assert destination.read_bytes() == payload
    assert all(result.path == destination.resolve() for result in results)
    assert all(result.sha256 == archived.sha256 for result in results)
    assert any(result.reused for result in results)
    assert list(destination.parent.glob(".daily.parquet.restore-*.tmp")) == []


@pytest.mark.skipif(os.name != "nt", reason="Windows path aliases are case-insensitive")
def test_windows_equivalent_path_aliases_share_the_same_restore_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"case-folded-restore-lock")
    filesystem = _MemoryFileSystem()
    first = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    second = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = first.archive_file(source, logical_path="bronze/tushare/daily")
    destination = tmp_path / "Cache" / "Daily.parquet"
    alias = Path(os.fspath(destination).swapcase())

    assert object_store_module._restore_lock(destination) is (
        object_store_module._restore_lock(alias)
    )

    original = S3ImmutableArchive._restore_file_serialized
    state_lock = Lock()
    active = 0
    maximum_active = 0

    def guarded(self, *args, **kwargs):
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            time.sleep(0.05)
            return original(self, *args, **kwargs)
        finally:
            with state_lock:
                active -= 1

    monkeypatch.setattr(S3ImmutableArchive, "_restore_file_serialized", guarded)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(
            pool.map(
                lambda item: item[0].restore_file(archived, item[1]),
                ((first, destination), (second, alias)),
            )
        )

    assert maximum_active == 1
    assert all(item.sha256 == archived.sha256 for item in results)
    assert destination.read_bytes() == source.read_bytes()


@pytest.mark.skipif(os.name != "nt", reason="Windows directory handle semantics")
def test_windows_restore_pins_parent_against_reparse_substitution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"pinned-parent-restore")
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = archive.archive_file(source, logical_path="silver/2026-08-26")
    destination = tmp_path / "cache" / "daily.parquet"
    destination.parent.mkdir()
    moved = tmp_path / "cache-moved"
    entered = Event()
    proceed = Event()
    original = S3ImmutableArchive._restore_file_serialized

    def paused(self, *args, **kwargs):
        entered.set()
        if not proceed.wait(timeout=5):
            raise AssertionError("restore parent-pin test timed out")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(S3ImmutableArchive, "_restore_file_serialized", paused)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(archive.restore_file, archived, destination)
        assert entered.wait(timeout=5)
        try:
            with pytest.raises(OSError):
                destination.parent.rename(moved)
        finally:
            proceed.set()
        result = future.result(timeout=5)

    assert result.path == destination.resolve()
    assert destination.read_bytes() == source.read_bytes()
    assert not moved.exists()
    assert list(destination.parent.glob(".daily.parquet.restore-*.tmp")) == []


@pytest.mark.skipif(os.name != "nt", reason="Windows relative-handle semantics")
def test_windows_parent_swap_before_relative_pin_fails_without_outside_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"relative-parent-pin")
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(bucket="factor-lab", filesystem=filesystem)
    archived = archive.archive_file(source, logical_path="silver/2026-08-26")
    destination = tmp_path / "cache" / "daily.parquet"
    destination.parent.mkdir()
    moved = tmp_path / "cache-moved"
    outside = tmp_path / "outside"
    outside.mkdir()
    probe = tmp_path / "link-probe"
    try:
        probe.symlink_to(outside, target_is_directory=True)
        probe.unlink()
    except OSError as exc:  # Windows without Developer Mode/admin rights.
        pytest.skip(f"symlinks unavailable: {exc}")

    original = object_store_module._create_windows_relative_parent_pin

    def swap_then_create(parent_handle: int) -> tuple[int, str]:
        destination.parent.rename(moved)
        destination.parent.symlink_to(outside, target_is_directory=True)
        return original(parent_handle)

    monkeypatch.setattr(
        object_store_module,
        "_create_windows_relative_parent_pin",
        swap_then_create,
    )
    with pytest.raises(ValueError, match="symlink/reparse"):
        archive.restore_file(archived, destination)

    pin_pattern = ".factor-lab-restore-parent-*.pin"
    assert list(outside.glob(pin_pattern)) == []
    assert list(moved.glob(pin_pattern)) == []
    assert not (outside / destination.name).exists()
    assert not (moved / destination.name).exists()


def test_cleanup_failure_inside_caller_exception_is_not_suppressed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary = tmp_path / ".daily.parquet.restore-test.tmp"
    temporary.write_bytes(b"temporary")
    original_unlink = Path.unlink

    def deny_cleanup(path: Path, *args, **kwargs) -> None:
        if path == temporary:
            raise PermissionError("simulated cleanup denial")
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", deny_cleanup)
    caller_error = ValueError("unrelated caller exception")
    caller_notes = tuple(getattr(caller_error, "__notes__", ()))
    try:
        raise caller_error
    except ValueError:
        with pytest.raises(
            ObjectStoreIntegrityError, match="restore temporary cleanup failed"
        ):
            with object_store_module._cleanup_restore_temporary(temporary):
                pass

    assert tuple(getattr(caller_error, "__notes__", ())) == caller_notes
    assert temporary.exists()
    original_unlink(temporary)


def test_cleanup_failure_preserves_restore_body_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary = tmp_path / ".daily.parquet.restore-primary.tmp"
    temporary.write_bytes(b"temporary")
    original_unlink = Path.unlink

    def deny_cleanup(path: Path, *args, **kwargs) -> None:
        if path == temporary:
            raise PermissionError("simulated cleanup denial")
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", deny_cleanup)
    primary = ObjectStoreIntegrityError("primary restore failure")
    with pytest.raises(ObjectStoreIntegrityError) as captured:
        with object_store_module._cleanup_restore_temporary(temporary):
            raise primary

    assert captured.value is primary
    if hasattr(primary, "add_note"):
        assert "restore temporary cleanup also failed" in primary.__notes__
    assert temporary.exists()
    original_unlink(temporary)
