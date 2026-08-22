from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from factor_lab.research_os.iceberg_service import PyIcebergGoldPublisher


def _windows_safe_warehouse_uri(path: Path) -> str:
    resolved = path.resolve().as_posix()
    if len(resolved) >= 2 and resolved[1] == ":":
        return f"file://{resolved}"
    return path.resolve().as_uri()


def test_real_pyiceberg_sql_catalog_publishes_and_reuses_immutable_tag(
    tmp_path: Path,
) -> None:
    catalog_module = pytest.importorskip("pyiceberg.catalog")
    database = (tmp_path / "iceberg_catalog.db").resolve().as_posix()
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = catalog_module.load_catalog(
        "factorlab_canary",
        type="sql",
        uri=f"sqlite:///{database}",
        warehouse=_windows_safe_warehouse_uri(warehouse),
        init_catalog_tables="true",
    )
    publisher = PyIcebergGoldPublisher(
        "factorlab_canary", catalog_loader=lambda _name: catalog
    )
    frame = pd.DataFrame(
        {
            "ticker": ["000001.SZ", "000002.SZ"],
            "trade_date": ["2024-01-02", "2024-01-02"],
            "close_adj": [10.0, 20.0],
        }
    )

    first = publisher.publish(
        frame,
        table_identifier="factor_lab.gold_canary",
        tag="ros_canary_v1",
        snapshot_key="a" * 64,
        partition_key="2024-01-02",
    )
    second = publisher.publish(
        frame,
        table_identifier="factor_lab.gold_canary",
        tag="ros_canary_v1",
        snapshot_key="a" * 64,
        partition_key="2024-01-02",
    )

    table = catalog.load_table("factor_lab.gold_canary")
    assert first.reused is False
    assert second.reused is True
    assert first.snapshot_id == second.snapshot_id
    assert table.metadata.refs["ros_canary_v1"].snapshot_id == first.snapshot_id


def test_real_pyiceberg_research_revision_overwrites_current_view_and_keeps_old_tag(
    tmp_path: Path,
) -> None:
    catalog_module = pytest.importorskip("pyiceberg.catalog")
    database = (tmp_path / "research_catalog.db").resolve().as_posix()
    warehouse = tmp_path / "research_warehouse"
    warehouse.mkdir()
    catalog = catalog_module.load_catalog(
        "factorlab_research_canary",
        type="sql",
        uri=f"sqlite:///{database}",
        warehouse=_windows_safe_warehouse_uri(warehouse),
        init_catalog_tables="true",
    )
    publisher = PyIcebergGoldPublisher(
        "factorlab_research_canary", catalog_loader=lambda _name: catalog
    )
    first_frame = pd.DataFrame(
        {
            "ticker": ["000001.SZ"],
            "decision_cutoff": pd.to_datetime(["2024-01-02T08:00:00Z"]),
            "label_available_at": pd.to_datetime(["2024-01-08T08:00:00Z"]),
            "label_is_research_only": [True],
            "forward_return_5d_open": [0.01],
        }
    )
    first = publisher.publish_research_panel(
        first_frame,
        table_identifier="factor_lab.gold_research_canary",
        tag="ros_research_v1",
        snapshot_key="a" * 64,
        partition_key="2024-01-08",
    )
    revised = publisher.publish_research_panel(
        first_frame.assign(forward_return_5d_open=0.02),
        table_identifier="factor_lab.gold_research_canary",
        tag="ros_research_v2",
        snapshot_key="b" * 64,
        partition_key="2024-01-08",
    )

    table = catalog.load_table("factor_lab.gold_research_canary")
    current = table.scan().to_pandas()
    assert len(current) == 1
    assert current.iloc[0]["forward_return_5d_open"] == pytest.approx(0.02)
    assert first.snapshot_id != revised.snapshot_id
    assert table.metadata.refs["ros_research_v1"].snapshot_id == first.snapshot_id
    assert table.metadata.refs["ros_research_v2"].snapshot_id == revised.snapshot_id
