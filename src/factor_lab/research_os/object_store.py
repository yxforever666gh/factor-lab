"""Content-addressed Bronze/Silver archive backed by the configured S3 store.

Local Parquet files are a process cache used by DuckDB/Polars.  This module
publishes the same bytes to MinIO under a digest-bearing immutable key so the
cache is never the only copy or the audit authority.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import stat
import tempfile
from threading import RLock
from typing import Any, BinaryIO, Iterator, Protocol
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
_RESTORE_LOCK_STRIPES = tuple(RLock() for _ in range(64))


def _restore_lock(path: Path) -> RLock:
    """Return a bounded process lock for one canonical cache destination."""

    canonical = os.path.normcase(
        os.path.normpath(os.path.abspath(os.fspath(path)))
    )
    digest = hashlib.sha256(os.fsencode(canonical)).digest()
    index = int.from_bytes(digest[:2], "big") % len(_RESTORE_LOCK_STRIPES)
    return _RESTORE_LOCK_STRIPES[index]


def _create_windows_relative_parent_pin(parent_handle: int) -> tuple[int, str]:
    """Create a private delete-on-close child relative to an open directory.

    ``NtCreateFile`` is used deliberately: the relative name is resolved by the
    already validated parent handle, never by a mutable path string.  The
    child is opened without ``FILE_SHARE_DELETE`` so its parent chain cannot be
    renamed until the returned handle is closed.
    """

    if os.name != "nt":  # pragma: no cover - guarded by the Windows caller.
        raise ObjectStoreIntegrityError("Windows restore pin is unavailable")

    import ctypes
    from ctypes import wintypes

    delete_access = 0x00010000
    synchronize = 0x00100000
    file_read_attributes = 0x00000080
    file_share_read = 0x00000001
    file_share_write = 0x00000002
    file_attribute_hidden = 0x00000002
    file_attribute_temporary = 0x00000100
    file_create = 2
    file_non_directory_file = 0x00000040
    file_delete_on_close = 0x00001000
    obj_case_insensitive = 0x00000040
    obj_dont_reparse = 0x00001000
    status_object_name_collision = ctypes.c_long(0xC0000035).value

    class UnicodeString(ctypes.Structure):
        _fields_ = [
            ("length", wintypes.USHORT),
            ("maximum_length", wintypes.USHORT),
            ("buffer", wintypes.LPWSTR),
        ]

    class ObjectAttributes(ctypes.Structure):
        _fields_ = [
            ("length", wintypes.ULONG),
            ("root_directory", wintypes.HANDLE),
            ("object_name", ctypes.POINTER(UnicodeString)),
            ("attributes", wintypes.ULONG),
            ("security_descriptor", wintypes.LPVOID),
            ("security_quality_of_service", wintypes.LPVOID),
        ]

    class IoStatusValue(ctypes.Union):
        _fields_ = [("status", wintypes.LONG), ("pointer", wintypes.LPVOID)]

    class IoStatusBlock(ctypes.Structure):
        _anonymous_ = ("value",)
        _fields_ = [
            ("value", IoStatusValue),
            ("information", ctypes.c_size_t),
        ]

    ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
    nt_create_file = ntdll.NtCreateFile
    nt_create_file.argtypes = (
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.DWORD,
        ctypes.POINTER(ObjectAttributes),
        ctypes.POINTER(IoStatusBlock),
        ctypes.c_void_p,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
    )
    nt_create_file.restype = wintypes.LONG

    for _attempt in range(16):
        name = f".factor-lab-restore-parent-{secrets.token_hex(16)}.pin"
        name_buffer = ctypes.create_unicode_buffer(name)
        name_value = UnicodeString(
            length=len(name.encode("utf-16-le")),
            maximum_length=ctypes.sizeof(name_buffer),
            buffer=ctypes.cast(name_buffer, wintypes.LPWSTR),
        )
        attributes = ObjectAttributes(
            length=ctypes.sizeof(ObjectAttributes),
            root_directory=parent_handle,
            object_name=ctypes.pointer(name_value),
            attributes=obj_case_insensitive | obj_dont_reparse,
            security_descriptor=None,
            security_quality_of_service=None,
        )
        io_status = IoStatusBlock()
        pin_handle = wintypes.HANDLE()
        status = nt_create_file(
            ctypes.byref(pin_handle),
            delete_access | synchronize | file_read_attributes,
            ctypes.byref(attributes),
            ctypes.byref(io_status),
            None,
            file_attribute_hidden | file_attribute_temporary,
            file_share_read | file_share_write,
            file_create,
            file_non_directory_file | file_delete_on_close,
            None,
            0,
        )
        if status >= 0 and pin_handle.value:
            return int(pin_handle.value), name
        if status != status_object_name_collision:
            break
    raise ObjectStoreIntegrityError("restore parent pin cannot be created safely")


@contextmanager
def _pinned_non_reparse_parent_chain(target: Path) -> Iterator[None]:
    """Pin the Windows parent chain against rename/reparse substitution.

    The immediate parent is first opened and validated without creating any
    path-based child.  A random delete-on-close child is then created relative
    to that directory handle without ``FILE_SHARE_DELETE``.  Windows prevents
    the pinned parent chain from being renamed while restore commits, without
    blocking atomic replacement of sibling data files.  Revalidating the
    parent's final path after the child is pinned closes the remaining rename
    window.  Other platforms retain the existing component checks.
    """

    _assert_no_link_components(target)
    if os.name != "nt":
        yield
        return

    import ctypes
    from ctypes import wintypes

    file_list_directory = 0x00000001
    file_share_read = 0x00000001
    file_share_write = 0x00000002
    file_share_delete = 0x00000004
    file_read_attributes = 0x00000080
    open_existing = 3
    file_attribute_directory = 0x00000010
    file_attribute_reparse_point = 0x00000400
    file_flag_backup_semantics = 0x02000000
    file_flag_open_reparse_point = 0x00200000
    file_attribute_tag_info_class = 9
    invalid_handle_value = ctypes.c_void_p(-1).value

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("reparse_tag", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    get_information = kernel32.GetFileInformationByHandleEx
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    )
    get_information.restype = wintypes.BOOL
    get_final_path = kernel32.GetFinalPathNameByHandleW
    get_final_path.argtypes = (
        wintypes.HANDLE,
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    get_final_path.restype = wintypes.DWORD
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL

    parent = target.parent
    expected_parent = os.path.normcase(
        os.path.normpath(os.path.abspath(os.fspath(parent)))
    )
    parent_handle: int | None = None
    pin_handle: int | None = None
    failed = False
    reparse_found = False

    def normalized_final_path(handle: int) -> str | None:
        buffer = ctypes.create_unicode_buffer(32768)
        length = get_final_path(handle, buffer, len(buffer), 0)
        if length == 0 or length >= len(buffer):
            return None
        observed = buffer.value
        if observed.startswith("\\\\?\\UNC\\"):
            observed = "\\\\" + observed[8:]
        elif observed.startswith("\\\\?\\"):
            observed = observed[4:]
        return os.path.normcase(
            os.path.normpath(os.path.abspath(observed))
        )

    try:
        parent_handle = create_file(
            os.fspath(parent),
            file_list_directory | file_read_attributes,
            file_share_read | file_share_write | file_share_delete,
            None,
            open_existing,
            file_flag_backup_semantics | file_flag_open_reparse_point,
            None,
        )
        if parent_handle == invalid_handle_value:
            failed = True
            parent_handle = None
        else:
            information = FileAttributeTagInfo()
            if not get_information(
                parent_handle,
                file_attribute_tag_info_class,
                ctypes.byref(information),
                ctypes.sizeof(information),
            ):
                failed = True
            elif information.file_attributes & file_attribute_reparse_point:
                reparse_found = True
            elif not information.file_attributes & file_attribute_directory:
                failed = True
            observed_parent = normalized_final_path(parent_handle)
            if observed_parent is None:
                failed = True
            elif observed_parent != expected_parent:
                reparse_found = True
        if failed:
            raise ObjectStoreIntegrityError(
                "restore parent chain cannot be pinned safely"
            )
        if reparse_found or parent_handle is None:
            raise ValueError("restore path contains a symlink/reparse component")

        pin_handle, _pin_name = _create_windows_relative_parent_pin(parent_handle)
        # The pin now prevents any further rename.  If the parent moved between
        # its first validation and relative pin creation, its final path differs
        # and the delete-on-close child is removed without touching the path it
        # was meant to protect.
        observed_parent = normalized_final_path(parent_handle)
        if observed_parent is None:
            raise ObjectStoreIntegrityError(
                "restore parent chain cannot be pinned safely"
            )
        if observed_parent != expected_parent:
            raise ValueError("restore path contains a symlink/reparse component")
        _assert_no_link_components(target)
        yield
    finally:
        if pin_handle is not None:
            close_handle(pin_handle)
        if parent_handle is not None:
            close_handle(parent_handle)


@contextmanager
def _cleanup_restore_temporary(temporary: Path) -> Iterator[None]:
    """Remove a restore temporary without confusing caller exception state."""

    body_error: BaseException | None = None
    try:
        try:
            yield
        except BaseException as exc:
            body_error = exc
            raise
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            if body_error is None:
                raise ObjectStoreIntegrityError(
                    "restore temporary cleanup failed"
                ) from None
            if hasattr(body_error, "add_note"):
                body_error.add_note("restore temporary cleanup also failed")


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
        """Hydrate one destination atomically under a process-wide path lock.

        Windows does not allow a verifier to open a destination during another
        thread's ``os.replace``.  A fixed set of striped locks serializes only
        colliding cache paths without growing an unbounded path registry.
        Dagster's production run coordinator separately prevents cross-worker
        restore overlap until a distributed cache lock is introduced.
        """

        target = _restore_destination(destination)
        with _restore_lock(target):
            target.parent.mkdir(parents=True, exist_ok=True)
            with _pinned_non_reparse_parent_chain(target):
                return self._restore_file_serialized(
                    archived,
                    target,
                    expected_sha256=expected_sha256,
                    expected_size_bytes=expected_size_bytes,
                )

    def _restore_file_serialized(
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
        with _cleanup_restore_temporary(temporary):
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
