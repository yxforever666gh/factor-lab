from datetime import datetime, timezone
from pathlib import Path

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.snapshot_service import publish_cataloged_snapshot


def test_snapshot_service_registers_exact_published_manifest(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "partition.parquet").write_bytes(b"immutable partition")
    repository = Path(__file__).resolve().parents[1]
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        publication = publish_cataloged_snapshot(
            catalog,
            paths=[data / "partition.parquet"],
            base_dir=data,
            snapshot_root=tmp_path / "snapshots",
            tier="bronze",
            as_of=datetime(2026, 8, 21, tzinfo=timezone.utc),
            parent_snapshot_ids=(),
            quality_report={"status": "pass"},
            trust_labels=(),
            repository=repository,
            dependency_lock=repository / "uv.lock",
            configuration={"source": "local_test"},
        )
        record = catalog.get_snapshot(publication.snapshot_id)
        assert record is not None
        assert record.reference.content_hash == publication.content_hash
        assert Path(publication.manifest_path).is_file()
