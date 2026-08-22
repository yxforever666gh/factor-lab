"""Content-addressed Bronze/Silver archive backed by the configured S3 store.

Local Parquet files are a process cache used by DuckDB/Polars.  This module
publishes the same bytes to MinIO under a digest-bearing immutable key so the
cache is never the only copy or the audit authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
from typing import Any, BinaryIO, Protocol
from urllib.parse import urlsplit


class ObjectStoreIntegrityError(RuntimeError):
    """Raised when an immutable object is missing or fails integrity checks."""


class ObjectStoreFileSystem(Protocol):
    def exists(self, path: str) -> bool: ...

    def open(self, path: str, mode: str = "rb") -> BinaryIO: ...


@dataclass(frozen=True)
class ArchivedObject:
    uri: str
    key: str
    sha256: str
    size_bytes: int
    reused: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RestoredObject:
    """A locally hydrated immutable object."""

    path: Path
    uri: str
    sha256: str
    size_bytes: int
    reused: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "uri": self.uri,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "reused": self.reused,
        }


_SAFE_SEGMENT = re.compile(r"[^A-Za-z0-9._=-]+")
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _safe_logical_path(value: str) -> str:
    pure = PurePosixPath(str(value).replace("\\", "/"))
    if pure.is_absolute() or ".." in pure.parts:
        raise ValueError("object-store logical path must stay within its prefix")
    rendered = [
        _SAFE_SEGMENT.sub("_", part).strip(".") or "unnamed"
        for part in pure.parts
        if part not in {"", "."}
    ]
    if not rendered:
        raise ValueError("object-store logical path is required")
    return "/".join(rendered)


def _digest_stream(handle: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = handle.read(1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _validated_sha256(value: str) -> str:
    digest = str(value).strip().lower()
    if not _SHA256.fullmatch(digest):
        raise ValueError("expected_sha256 must be a 64-character hexadecimal digest")
    return digest


def _validated_size(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("expected_size_bytes must be a non-negative integer")
    return value


def _is_link_or_reparse(path: Path) -> bool:
    """Detect symlinks and Windows junction/reparse points without following them."""

    try:
        details = os.lstat(path)
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(details.st_mode):
        return True
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(getattr(details, "st_file_attributes", 0) & reparse_flag)


def _assert_no_link_components(path: Path) -> None:
    """Reject a target when it or any existing parent redirects elsewhere."""

    current = path
    while True:
        if _is_link_or_reparse(current):
            raise ValueError(
                f"restore destination cannot traverse a symlink/reparse point: {current}"
            )
        parent = current.parent
        if parent == current:
            break
        current = parent


def _restore_destination(value: str | Path) -> Path:
    raw = Path(value)
    if ".." in raw.parts:
        raise ValueError("restore destination cannot contain parent traversal")
    destination = Path(os.path.abspath(os.fspath(raw)))
    if destination == destination.parent:
        raise ValueError("restore destination must name a file")
    _assert_no_link_components(destination)
    return destination


class S3ImmutableArchive:
    """Archive files once under keys that contain their SHA-256 digest."""

    def __init__(
        self,
        *,
        bucket: str,
        filesystem: ObjectStoreFileSystem,
        prefix: str = "research-os",
    ) -> None:
        bucket = str(bucket).strip().strip("/")
        if not bucket or "/" in bucket or ".." in bucket:
            raise ValueError("a single S3 bucket name is required")
        self.bucket = bucket
        self.filesystem = filesystem
        self.prefix = _safe_logical_path(prefix)

    @classmethod
    def from_connection(
        cls,
        *,
        endpoint: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        prefix: str = "research-os",
    ) -> "S3ImmutableArchive":
        try:
            import s3fs
        except ImportError as exc:  # pragma: no cover - infrastructure image path
            raise ObjectStoreIntegrityError(
                "s3fs is required for the Bronze/Silver object-store archive"
            ) from exc
        filesystem = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={"endpoint_url": endpoint},
            use_ssl=str(endpoint).lower().startswith("https://"),
        )
        return cls(bucket=bucket, filesystem=filesystem, prefix=prefix)

    def _remote_digest(self, remote: str) -> tuple[str, int]:
        with self.filesystem.open(remote, "rb") as handle:
            return _digest_stream(handle)

    def _restore_source(
        self,
        archived: ArchivedObject | str,
        *,
        expected_sha256: str | None,
        expected_size_bytes: int | None,
    ) -> tuple[str, str, str, int]:
        if isinstance(archived, ArchivedObject):
            digest = _validated_sha256(
                archived.sha256 if expected_sha256 is None else expected_sha256
            )
            size = _validated_size(
                archived.size_bytes
                if expected_size_bytes is None
                else expected_size_bytes
            )
            if digest != _validated_sha256(archived.sha256):
                raise ValueError("expected_sha256 differs from ArchivedObject")
            if size != _validated_size(archived.size_bytes):
                raise ValueError("expected_size_bytes differs from ArchivedObject")
            uri = str(archived.uri)
            declared_key = str(archived.key)
        else:
            if expected_sha256 is None or expected_size_bytes is None:
                raise ValueError(
                    "s3:// restore requires expected_sha256 and expected_size_bytes"
                )
            digest = _validated_sha256(expected_sha256)
            size = _validated_size(expected_size_bytes)
            uri = str(archived).strip()
            declared_key = ""

        parsed = urlsplit(uri)
        if (
            parsed.scheme.lower() != "s3"
            or parsed.netloc != self.bucket
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
        ):
            raise ValueError("restore URI must use the configured s3:// bucket")
        if not parsed.path.startswith("/") or "%" in parsed.path or "\\" in parsed.path:
            raise ValueError("restore URI contains an ambiguous object-store path")
        key = parsed.path[1:]
        pure = PurePosixPath(key)
        parts = pure.parts
        prefix_parts = PurePosixPath(self.prefix).parts
        if (
            not parts
            or str(pure) != key
            or any(part in {"", ".", ".."} for part in parts)
            or parts[: len(prefix_parts)] != prefix_parts
            or _safe_logical_path(key) != key
        ):
            raise ValueError("restore URI escapes the configured immutable prefix")
        if declared_key and declared_key != key:
            raise ValueError("ArchivedObject URI and key differ")
        if f"sha256={digest}" not in parts:
            raise ValueError("restore URI is not bound to the expected SHA-256 digest")
        return uri, key, digest, size

    @staticmethod
    def _local_digest(path: Path) -> tuple[str, int]:
        if not path.is_file() or _is_link_or_reparse(path):
            raise ObjectStoreIntegrityError(
                f"restore destination is not a regular file: {path}"
            )
        with path.open("rb") as handle:
            return _digest_stream(handle)

    def restore_file(
        self,
        archived: ArchivedObject | str,
        destination: str | Path,
        *,
        expected_sha256: str | None = None,
        expected_size_bytes: int | None = None,
    ) -> RestoredObject:
        """Hydrate an immutable object into a symlink-free local cache path.

        URI callers must supply both expected integrity fields.  An
        ``ArchivedObject`` already carries them, though matching explicit values
        may be supplied as an additional assertion.  Existing correct files are
        reused; existing conflicting files are preserved and rejected.
        """

        uri, key, digest, size = self._restore_source(
            archived,
            expected_sha256=expected_sha256,
            expected_size_bytes=expected_size_bytes,
        )
        remote = f"{self.bucket}/{key}"
        try:
            remote_exists = self.filesystem.exists(remote)
        except Exception as exc:
            raise ObjectStoreIntegrityError(
                f"cannot inspect immutable object at {uri}"
            ) from exc
        if not remote_exists:
            raise ObjectStoreIntegrityError(f"immutable object is missing at {uri}")

        target = _restore_destination(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        _assert_no_link_components(target)

        if os.path.lexists(target):
            local_digest, local_size = self._local_digest(target)
            if (local_digest, local_size) != (digest, size):
                raise ObjectStoreIntegrityError(
                    f"existing restore destination differs from {uri}: {target}"
                )
            try:
                remote_digest, remote_size = self._remote_digest(remote)
            except Exception as exc:
                raise ObjectStoreIntegrityError(
                    f"cannot read immutable object at {uri}"
                ) from exc
            if (remote_digest, remote_size) != (digest, size):
                raise ObjectStoreIntegrityError(
                    f"immutable object differs at {uri}"
                )
            return RestoredObject(target, uri, digest, size, True)

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.restore-",
            suffix=".tmp",
            dir=target.parent,
        )
        temporary = Path(temporary_name)
        try:
            downloaded_digest = hashlib.sha256()
            downloaded_size = 0
            try:
                with os.fdopen(descriptor, "wb") as output_handle:
                    with self.filesystem.open(remote, "rb") as input_handle:
                        while True:
                            chunk = input_handle.read(1024 * 1024)
                            if not chunk:
                                break
                            output_handle.write(chunk)
                            downloaded_digest.update(chunk)
                            downloaded_size += len(chunk)
                    output_handle.flush()
                    os.fsync(output_handle.fileno())
            except Exception as exc:
                raise ObjectStoreIntegrityError(
                    f"cannot restore immutable object from {uri}"
                ) from exc

            if (downloaded_digest.hexdigest(), downloaded_size) != (digest, size):
                raise ObjectStoreIntegrityError(
                    f"immutable object differs at {uri}"
                )

            # A concurrent exact hydration is safe to reuse.  Any conflicting
            # arrival remains untouched and fails closed.
            _assert_no_link_components(target)
            if os.path.lexists(target):
                local_digest, local_size = self._local_digest(target)
                if (local_digest, local_size) != (digest, size):
                    raise ObjectStoreIntegrityError(
                        f"concurrent restore destination differs from {uri}: {target}"
                    )
                return RestoredObject(target, uri, digest, size, True)

            try:
                os.replace(temporary, target)
            except PermissionError:
                # Windows may deny replacing a destination while another
                # concurrent verifier has it open.  If that peer already
                # installed the exact immutable bytes, reuse them and leave the
                # temporary file to the common cleanup path.
                if os.path.lexists(target):
                    local_digest, local_size = self._local_digest(target)
                    if (local_digest, local_size) == (digest, size):
                        return RestoredObject(target, uri, digest, size, True)
                raise
            final_digest, final_size = self._local_digest(target)
            if (final_digest, final_size) != (digest, size):
                raise ObjectStoreIntegrityError(
                    f"restored destination failed integrity verification: {target}"
                )
            return RestoredObject(target, uri, digest, size, False)
        finally:
            temporary.unlink(missing_ok=True)

    def archive_file(self, path: str | Path, *, logical_path: str) -> ArchivedObject:
        source = Path(path)
        if not source.is_file() or source.is_symlink():
            raise FileNotFoundError(source)
        with source.open("rb") as handle:
            digest, size = _digest_stream(handle)
        logical = _safe_logical_path(logical_path)
        filename = _safe_logical_path(source.name)
        key = f"{self.prefix}/{logical}/sha256={digest}/{filename}"
        remote = f"{self.bucket}/{key}"
        reused = self.filesystem.exists(remote)
        if reused:
            remote_digest, remote_size = self._remote_digest(remote)
            if (remote_digest, remote_size) != (digest, size):
                raise ObjectStoreIntegrityError(
                    f"immutable object differs at s3://{remote}"
                )
        else:
            # The key embeds the digest: concurrent writers can only be valid
            # when they upload the exact same bytes.  Always verify afterwards.
            with source.open("rb") as input_handle, self.filesystem.open(remote, "wb") as output_handle:
                while True:
                    chunk = input_handle.read(1024 * 1024)
                    if not chunk:
                        break
                    output_handle.write(chunk)
            remote_digest, remote_size = self._remote_digest(remote)
            if (remote_digest, remote_size) != (digest, size):
                raise ObjectStoreIntegrityError(
                    f"object verification failed at s3://{remote}"
                )
        return ArchivedObject(
            uri=f"s3://{remote}",
            key=key,
            sha256=digest,
            size_bytes=size,
            reused=reused,
        )


__all__ = [
    "ArchivedObject",
    "ObjectStoreIntegrityError",
    "ObjectStoreFileSystem",
    "RestoredObject",
    "S3ImmutableArchive",
]
