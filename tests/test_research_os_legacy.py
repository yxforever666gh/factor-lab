from pathlib import Path

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.legacy import import_legacy_evidence


def test_legacy_import_is_deduplicated_and_never_becomes_current_evidence(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    expanded = artifacts / "expanded_long_only"
    expanded.mkdir(parents=True)
    (artifacts / "factor_lab.db").write_bytes(b"legacy sqlite placeholder")
    (expanded / "results.json").write_text("[]", encoding="utf-8")
    reference = expanded / "reference"
    reference.mkdir()
    (reference / "historical_st.parquet").write_bytes(b"empty cached table")

    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        first = import_legacy_evidence(catalog, artifacts)
        second = import_legacy_evidence(catalog, artifacts)
        rows = catalog.list_legacy_evidence(limit=100)

    assert first.imported == 3
    assert second.imported == 3
    assert len(rows) == 3
    assert {row.trust_label for row in rows} == {
        "legacy_sqlite_read_only",
        "legacy_execution_regression_only",
        "st_history_unverified",
    }
