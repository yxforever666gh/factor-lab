"""Fail-closed publication of immutable Research OS Gold data to Iceberg.

The rest of Research OS deliberately depends on the small
``GoldSnapshotPublisher`` protocol rather than importing PyIceberg.  This keeps
unit tests lightweight while production still performs a real catalog commit
and creates an immutable named tag for every Research OS snapshot.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

import pandas as pd


_FORWARD_ONLY_COLUMN = re.compile(
    r"(^|_)(forward|future|lead|label)(_|$)|forward_return", re.IGNORECASE
)
_SNAPSHOT_KEY_PROPERTY = "factor_lab.snapshot_key"
_PARTITION_KEY_PROPERTY = "factor_lab.partition_key"


class IcebergPublicationError(RuntimeError):
    """Raised when a Gold commit or immutable tag cannot be verified."""


@dataclass(frozen=True)
class IcebergCommit:
    table_identifier: str
    snapshot_id: int
    tag: str
    row_count: int
    reused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@runtime_checkable
class GoldSnapshotPublisher(Protocol):
    def publish(
        self,
        frame: pd.DataFrame,
        *,
        table_identifier: str,
        tag: str,
        snapshot_key: str,
        partition_key: str,
    ) -> IcebergCommit:
        """Append data and bind ``tag`` to the resulting Iceberg snapshot."""

    def publish_research_panel(
        self,
        frame: pd.DataFrame,
        *,
        table_identifier: str,
        tag: str,
        snapshot_key: str,
        partition_key: str,
    ) -> IcebergCommit:
        """Replace the current full-history research view and preserve old snapshots."""


def _snapshot_summary(snapshot: Any) -> Mapping[str, Any]:
    summary = getattr(snapshot, "summary", None)
    if summary is None:
        return {}
    if isinstance(summary, Mapping):
        return summary
    additional = getattr(summary, "additional_properties", None)
    return additional if isinstance(additional, Mapping) else {}


def _snapshot_id(value: Any) -> int:
    raw = getattr(value, "snapshot_id", None)
    if raw is None:
        raise IcebergPublicationError("Iceberg snapshot has no snapshot_id")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise IcebergPublicationError("Iceberg snapshot_id is not an integer") from exc


def _existing_ref_snapshot_id(table: Any, tag: str) -> int | None:
    metadata = getattr(table, "metadata", None)
    refs = getattr(metadata, "refs", None)
    if not isinstance(refs, Mapping):
        return None
    reference = refs.get(tag)
    return None if reference is None else _snapshot_id(reference)


def _find_snapshot_by_key(
    table: Any, snapshot_key: str, *, prefer_current: bool = False
) -> int | None:
    metadata = getattr(table, "metadata", None)
    snapshots = getattr(metadata, "snapshots", ()) or ()
    found: list[int] = []
    for snapshot in snapshots:
        if str(_snapshot_summary(snapshot).get(_SNAPSHOT_KEY_PROPERTY) or "") == snapshot_key:
            found.append(_snapshot_id(snapshot))
    if len(set(found)) > 1:
        if prefer_current:
            current_snapshot = getattr(table, "current_snapshot", None)
            current = current_snapshot() if callable(current_snapshot) else None
            current_id = None if current is None else _snapshot_id(current)
            if current_id in found:
                return current_id
        raise IcebergPublicationError(
            "multiple Iceberg snapshots claim the same immutable Research OS key"
        )
    return found[0] if found else None


class PyIcebergGoldPublisher:
    """Publish a Pandas frame through a configured PyIceberg catalog.

    ``catalog_loader`` and ``arrow_builder`` are injection seams for tests.  In
    production both dependencies are imported lazily, so importing Dagster
    definitions remains possible even before the infrastructure extras are
    installed.
    """

    def __init__(
        self,
        catalog_name: str = "factorlab",
        *,
        catalog_loader: Callable[[str], Any] | None = None,
        arrow_builder: Callable[[pd.DataFrame], Any] | None = None,
    ) -> None:
        if not str(catalog_name).strip():
            raise ValueError("catalog_name is required")
        self.catalog_name = str(catalog_name).strip()
        self._catalog_loader = catalog_loader
        self._arrow_builder = arrow_builder

    def _load_catalog(self) -> Any:
        if self._catalog_loader is not None:
            return self._catalog_loader(self.catalog_name)
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as exc:  # pragma: no cover - infrastructure image path.
            raise IcebergPublicationError(
                "PyIceberg is required for Gold publication"
            ) from exc
        return load_catalog(self.catalog_name)

    def _to_arrow(self, frame: pd.DataFrame) -> Any:
        if self._arrow_builder is not None:
            return self._arrow_builder(frame.copy(deep=True))
        try:
            import pyarrow as pa
        except ImportError as exc:  # pragma: no cover - infrastructure image path.
            raise IcebergPublicationError(
                "PyArrow is required for Iceberg Gold publication"
            ) from exc
        table = pa.Table.from_pandas(frame, preserve_index=False)
        # Iceberg timestamp precision is microseconds.  PyArrow/Pandas often
        # produce nanoseconds, so make the conversion explicit and safe.
        fields = []
        changed = False
        for field in table.schema:
            dtype = field.type
            if pa.types.is_timestamp(dtype) and dtype.unit == "ns":
                dtype = pa.timestamp("us", tz=dtype.tz)
                changed = True
            fields.append(pa.field(field.name, dtype, nullable=field.nullable))
        return table.cast(pa.schema(fields), safe=True) if changed else table

    @staticmethod
    def _ensure_tag(table: Any, tag: str, snapshot_id: int) -> None:
        current = _existing_ref_snapshot_id(table, tag)
        if current is not None:
            if current != snapshot_id:
                raise IcebergPublicationError(
                    f"immutable Iceberg tag {tag!r} already points to snapshot {current}"
                )
            return
        manager = table.manage_snapshots()
        manager.create_tag(snapshot_id, tag).commit()
        refresh = getattr(table, "refresh", None)
        if callable(refresh):
            refresh()
        verified = _existing_ref_snapshot_id(table, tag)
        if verified is None:
            raise IcebergPublicationError(
                f"Iceberg tag {tag!r} was not visible after commit and refresh"
            )
        if verified != snapshot_id:
            raise IcebergPublicationError(
                f"Iceberg tag verification failed for {tag!r}: {verified}"
            )

    @staticmethod
    def _load_or_create_table(catalog: Any, identifier: str, arrow: Any) -> Any:
        namespace, separator, _ = identifier.rpartition(".")
        if not separator or not namespace:
            raise ValueError(
                "table_identifier must be qualified, for example 'factor_lab.gold_daily'"
            )
        create_namespace = getattr(catalog, "create_namespace_if_not_exists", None)
        if callable(create_namespace):
            create_namespace(namespace)
        table_exists = getattr(catalog, "table_exists", None)
        if callable(table_exists) and table_exists(identifier):
            return catalog.load_table(identifier)
        create = getattr(catalog, "create_table_if_not_exists", None)
        if not callable(create):
            raise IcebergPublicationError(
                "configured PyIceberg catalog cannot create tables idempotently"
            )
        return create(identifier, schema=arrow.schema)

    def publish(
        self,
        frame: pd.DataFrame,
        *,
        table_identifier: str,
        tag: str,
        snapshot_key: str,
        partition_key: str,
    ) -> IcebergCommit:
        if frame.empty:
            raise IcebergPublicationError("empty Gold frames cannot be published")
        forbidden = sorted(
            str(column) for column in frame.columns if _FORWARD_ONLY_COLUMN.search(str(column))
        )
        if forbidden:
            raise IcebergPublicationError(
                f"daily Gold data contains forward-only columns: {forbidden}"
            )
        if not table_identifier.strip() or not tag.strip() or not snapshot_key.strip():
            raise ValueError("table_identifier, tag and snapshot_key are required")
        if not partition_key.strip():
            raise ValueError("partition_key is required")

        catalog = self._load_catalog()
        arrow = self._to_arrow(frame)
        table = self._load_or_create_table(catalog, table_identifier, arrow)

        existing_snapshot = _find_snapshot_by_key(table, snapshot_key)
        if existing_snapshot is not None:
            self._ensure_tag(table, tag, existing_snapshot)
            return IcebergCommit(
                table_identifier=table_identifier,
                snapshot_id=existing_snapshot,
                tag=tag,
                row_count=len(frame),
                reused=True,
            )

        table.append(
            arrow,
            snapshot_properties={
                _SNAPSHOT_KEY_PROPERTY: snapshot_key,
                _PARTITION_KEY_PROPERTY: partition_key,
            },
        )
        refresh = getattr(table, "refresh", None)
        if callable(refresh):
            refresh()
        # Do not trust ``current_snapshot`` here: another writer may append
        # after us.  Bind the tag only to the unique snapshot carrying our
        # immutable key in its committed summary.
        committed_snapshot = _find_snapshot_by_key(table, snapshot_key)
        if committed_snapshot is None:
            raise IcebergPublicationError(
                "Iceberg append was not visible with the requested immutable snapshot key"
            )
        self._ensure_tag(table, tag, committed_snapshot)
        return IcebergCommit(
            table_identifier=table_identifier,
            snapshot_id=committed_snapshot,
            tag=tag,
            row_count=len(frame),
            reused=False,
        )

    @staticmethod
    def _validate_research_panel(frame: pd.DataFrame, table_identifier: str) -> None:
        forbidden = sorted(
            str(column)
            for column in frame.columns
            if _FORWARD_ONLY_COLUMN.search(str(column))
        )
        if not forbidden:
            raise IcebergPublicationError(
                "research panel publication requires an explicit research label"
            )
        if "research" not in table_identifier.lower():
            raise IcebergPublicationError(
                "labeled Gold data may only be published to a research-named table"
            )
        required = {"label_is_research_only", "label_available_at", "decision_cutoff"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise IcebergPublicationError(
                f"research label provenance columns are missing: {missing}"
            )
        marker = frame["label_is_research_only"].astype("boolean")
        if marker.isna().any() or not marker.all():
            raise IcebergPublicationError(
                "every labeled Gold row must be marked research-only"
            )
        label_columns = [
            name for name in frame.columns if "forward_return" in str(name).lower()
        ]
        if not label_columns:
            raise IcebergPublicationError("research panel has no forward-return label")
        available_at = pd.to_datetime(frame["label_available_at"], errors="coerce", utc=True)
        decision = pd.to_datetime(frame["decision_cutoff"], errors="coerce", utc=True)
        has_label = frame[label_columns].notna().any(axis=1)
        invalid = has_label & (
            available_at.isna() | decision.isna() | available_at.le(decision)
        )
        if invalid.any():
            raise IcebergPublicationError(
                "research labels are not temporally separated from their decision cutoff"
            )

    def publish_research_panel(
        self,
        frame: pd.DataFrame,
        *,
        table_identifier: str,
        tag: str,
        snapshot_key: str,
        partition_key: str,
    ) -> IcebergCommit:
        """Publish one full-history revision without appending duplicate rows.

        Iceberg ``overwrite`` creates a new immutable snapshot, so old tags keep
        providing time travel while the main branch exposes exactly one current
        full-history view.  An identical snapshot key is reused idempotently.
        """

        if frame.empty:
            raise IcebergPublicationError("empty research Gold frames cannot be published")
        if not table_identifier.strip() or not tag.strip() or not snapshot_key.strip():
            raise ValueError("table_identifier, tag and snapshot_key are required")
        if not partition_key.strip():
            raise ValueError("partition_key is required")
        self._validate_research_panel(frame, table_identifier)

        catalog = self._load_catalog()
        arrow = self._to_arrow(frame)
        namespace, separator, _ = table_identifier.rpartition(".")
        if not separator or not namespace:
            raise ValueError(
                "table_identifier must be qualified, for example 'factor_lab.gold_research_panel'"
            )
        create_namespace = getattr(catalog, "create_namespace_if_not_exists", None)
        if callable(create_namespace):
            create_namespace(namespace)
        table_exists = getattr(catalog, "table_exists", None)
        existed = bool(callable(table_exists) and table_exists(table_identifier))
        table = self._load_or_create_table(catalog, table_identifier, arrow)
        existing_snapshot = _find_snapshot_by_key(
            table, snapshot_key, prefer_current=True
        )
        if existing_snapshot is not None:
            self._ensure_tag(table, tag, existing_snapshot)
            return IcebergCommit(
                table_identifier=table_identifier,
                snapshot_id=existing_snapshot,
                tag=tag,
                row_count=len(frame),
                reused=True,
            )

        properties = {
            _SNAPSHOT_KEY_PROPERTY: snapshot_key,
            _PARTITION_KEY_PROPERTY: partition_key,
            "factor_lab.publication_mode": "full_history_overwrite",
        }
        if existed:
            overwrite = getattr(table, "overwrite", None)
            if not callable(overwrite):
                raise IcebergPublicationError(
                    "configured Iceberg table cannot atomically overwrite a research revision"
                )
            overwrite(arrow, snapshot_properties=properties)
        else:
            table.append(arrow, snapshot_properties=properties)
        refresh = getattr(table, "refresh", None)
        if callable(refresh):
            refresh()
        committed_snapshot = _find_snapshot_by_key(
            table, snapshot_key, prefer_current=True
        )
        if committed_snapshot is None:
            raise IcebergPublicationError(
                "research Iceberg revision was not visible with its immutable key"
            )
        self._ensure_tag(table, tag, committed_snapshot)
        return IcebergCommit(
            table_identifier=table_identifier,
            snapshot_id=committed_snapshot,
            tag=tag,
            row_count=len(frame),
            reused=False,
        )


__all__ = [
    "GoldSnapshotPublisher",
    "IcebergCommit",
    "IcebergPublicationError",
    "PyIcebergGoldPublisher",
]
