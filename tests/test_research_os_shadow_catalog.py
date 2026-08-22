from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib

import pandas as pd
import pytest

from factor_lab.research_os.catalog import CatalogConflict, ResearchCatalog, ShadowEventInput
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    SnapshotTier,
)
from factor_lab.research_os.shadow import ShadowExecutionConfig
from factor_lab.research_os.shadow_catalog import (
    ShadowDataBlocked,
    ShadowStepAlreadyApplied,
    ShadowStepService,
)


OPENED_AT = datetime(2025, 12, 31, tzinfo=timezone.utc)
DECISION_AS_OF = datetime(2026, 1, 2, 7, tzinfo=timezone.utc)


@pytest.fixture
def catalog(tmp_path):
    with ResearchCatalog(tmp_path / "shadow-catalog.sqlite") as instance:
        instance.initialize_schema()
        yield instance


def _register_snapshot(
    catalog: ResearchCatalog,
    *,
    snapshot_id: str = "gold-2026-01-02",
    quality_status: DataQualityStatus = DataQualityStatus.ACCEPTED,
    trust_labels: tuple[str, ...] = (),
) -> None:
    sessions = ("2025-12-31", "2026-01-02", "2026-01-05", "2026-01-06")
    calendar_hash = hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest()
    catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id=snapshot_id,
            tier=SnapshotTier.GOLD,
            uri=f"s3://factor-lab/gold/{snapshot_id}",
            content_hash="a" * 64,
            as_of=DECISION_AS_OF,
            quality_status=quality_status,
            trust_labels=trust_labels,
            manifest={
                "trading_calendar": {
                    "source": "test-exchange-calendar",
                    "quality_status": "accepted",
                    "sessions": list(sessions),
                    "content_hash": calendar_hash,
                }
            },
        )
    )


def _create_account(catalog: ResearchCatalog, account_id: str = "paper-main") -> None:
    catalog.create_shadow_account(
        account_id=account_id,
        name="Research shadow",
        initial_capital=1_000_000,
        opened_at=OPENED_AT,
    )


def _bars(
    session: str,
    *,
    include_forward_label: bool = False,
    execution_available_at: str | None = None,
    snapshot_id: str = "gold-2026-01-02",
) -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "trade_date": session,
                "execution_event_time": f"{session}T09:30:00+08:00",
                "execution_available_at": execution_available_at
                or f"{session}T09:30:00+08:00",
                "mark_event_time": f"{session}T15:00:00+08:00",
                "mark_available_at": f"{session}T15:01:00+08:00",
                "gold_snapshot_id": snapshot_id,
                "open_adj": 10.0,
                "close_adj": 11.0,
                "adv_20": 100_000_000.0,
                "volatility_20": 0.02,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "is_suspended": False,
                "is_delisted": False,
            }
        ]
    )
    if include_forward_label:
        frame["forward_return_5d"] = 0.99
    return frame


def _service(catalog: ResearchCatalog) -> ShadowStepService:
    return ShadowStepService(
        catalog,
        ShadowExecutionConfig(max_position_weight=0.5, lot_size=100),
    )


def test_shadow_step_rehydrates_executes_next_open_and_commits_projections(catalog) -> None:
    _register_snapshot(catalog)
    _create_account(catalog)
    service = _service(catalog)

    result = service.step(
        account_id="paper-main",
        decision_date="2026-01-02",
        trade_date="2026-01-05",
        expected_next_session="2026-01-05",
        target_weights={"000001.SZ": 0.5},
        market_bars=_bars("2026-01-05"),
        snapshot_id="gold-2026-01-02",
        model_version="champion-v1",
        benchmark_return=0.01,
    )

    assert result.chain_verified is True
    assert result.domain_event_types == ("target_received", "fill", "mark_to_market")
    assert result.persisted_event_count == result.domain_event_count + 2
    assert result.first_event_sequence == 2
    assert result.position_count == 1

    events = catalog.list_shadow_events(account_id="paper-main", limit=20)
    fill = next(event for event in events if event.event_type == "fill")
    assert fill.payload["price"] == 10.0
    assert fill.payload["research_os_shadow_step"]["engine_event_hash"]
    target = next(event for event in events if event.event_type == "target_received")
    assert target.payload["timing"]["decision_cutoff"] == "2026-01-02T07:00:00+00:00"
    assert target.payload["timing"]["execution_event_time"] == "2026-01-05T01:30:00+00:00"
    position = catalog.list_shadow_positions("paper-main")[0]
    assert position.quantity > 0
    assert position.average_cost > 10.0
    assert position.market_price == 11.0
    account = catalog.get_shadow_account("paper-main")
    assert account is not None
    assert account.nav == pytest.approx(account.cash + position.market_value)
    assert account.benchmark_nav == pytest.approx(1_010_000)
    assert catalog.verify_shadow_chain("paper-main") is True

    # A new service instance proves that the second session is reconstructed
    # solely from catalog account/position projections, then fully exits.
    second = _service(catalog).step(
        account_id="paper-main",
        decision_date="2026-01-05",
        trade_date="2026-01-06",
        expected_next_session="2026-01-06",
        target_weights={},
        market_bars=_bars("2026-01-06"),
        snapshot_id="gold-2026-01-02",
        model_version="champion-v1",
    )
    assert second.position_count == 0
    assert catalog.list_shadow_positions("paper-main") == []
    assert catalog.verify_shadow_chain("paper-main") is True


def test_shadow_step_is_fail_closed_for_forward_labels_and_unverified_st(catalog) -> None:
    _register_snapshot(catalog)
    _create_account(catalog)
    service = _service(catalog)
    before = catalog.get_shadow_account("paper-main")
    assert before is not None

    with pytest.raises(ValueError, match="forward"):
        service.step(
            account_id="paper-main",
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={"000001.SZ": 0.5},
            market_bars=_bars("2026-01-05", include_forward_label=True),
            snapshot_id="gold-2026-01-02",
            model_version="champion-v1",
        )
    after = catalog.get_shadow_account("paper-main")
    assert after is not None
    assert after.last_event_sequence == before.last_event_sequence

    # A separately catalogued snapshot may never bypass the explicit current
    # ST-history downgrade merely because its generic status says accepted.
    catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id="gold-unverified-st",
            tier=SnapshotTier.GOLD,
            uri="s3://factor-lab/gold/unverified-st",
            content_hash="b" * 64,
            as_of=DECISION_AS_OF,
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=("st_history_unverified",),
        )
    )
    with pytest.raises(ShadowDataBlocked, match="st_history_unverified"):
        service.step(
            account_id="paper-main",
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={},
            market_bars=_bars("2026-01-05"),
            snapshot_id="gold-unverified-st",
            model_version="champion-v1",
        )


def test_shadow_step_requires_calendar_evidence_and_exact_next_session(catalog) -> None:
    _register_snapshot(catalog)
    _create_account(catalog)
    service = _service(catalog)
    common = {
        "account_id": "paper-main",
        "decision_date": "2026-01-02",
        "trade_date": "2026-01-05",
        "target_weights": {},
        "market_bars": _bars("2026-01-05"),
        "snapshot_id": "gold-2026-01-02",
        "model_version": "champion-v1",
    }
    with pytest.raises(TypeError, match="expected_next_session"):
        service.step(**common)
    with pytest.raises(ShadowDataBlocked, match="mandatory exchange-calendar evidence"):
        service.step(**common, expected_next_session=None)
    with pytest.raises(ShadowDataBlocked, match="not the exchange calendar's next session"):
        service.step(**common, expected_next_session="2026-01-06")


def test_shadow_step_rejects_missing_or_tampered_persisted_calendar(catalog) -> None:
    _create_account(catalog)
    for snapshot_id, snapshot_hash, manifest, expected in (
        ("gold-no-calendar", "d" * 64, {}, "persisted trusted trading calendar"),
        (
            "gold-tampered-calendar",
            "e" * 64,
            {
                "trading_calendar": {
                    "source": "test-exchange-calendar",
                    "quality_status": "accepted",
                    "sessions": ["2026-01-02", "2026-01-05"],
                    "content_hash": "0" * 64,
                }
            },
            "content hash mismatch",
        ),
    ):
        catalog.register_snapshot(
            DataSnapshotRef(
                snapshot_id=snapshot_id,
                tier=SnapshotTier.GOLD,
                uri=f"s3://factor-lab/gold/{snapshot_id}",
                content_hash=snapshot_hash,
                as_of=DECISION_AS_OF,
                quality_status=DataQualityStatus.ACCEPTED,
                manifest=manifest,
            )
        )
        with pytest.raises(ShadowDataBlocked, match=expected):
            _service(catalog).step(
                account_id="paper-main",
                decision_date="2026-01-02",
                trade_date="2026-01-05",
                expected_next_session="2026-01-05",
                target_weights={},
                market_bars=_bars("2026-01-05", snapshot_id=snapshot_id),
                snapshot_id=snapshot_id,
                model_version="champion-v1",
            )


def test_shadow_step_rejects_market_bars_bound_to_another_gold_snapshot(catalog) -> None:
    _register_snapshot(catalog)
    _create_account(catalog)
    with pytest.raises(ShadowDataBlocked, match="same accepted Gold snapshot"):
        _service(catalog).step(
            account_id="paper-main",
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={},
            market_bars=_bars("2026-01-05", snapshot_id="different-gold"),
            snapshot_id="gold-2026-01-02",
            model_version="champion-v1",
        )


def test_shadow_step_rejects_open_inputs_first_available_after_open(catalog) -> None:
    _register_snapshot(catalog)
    _create_account(catalog)
    before = catalog.get_shadow_account("paper-main")
    assert before is not None

    with pytest.raises(ShadowDataBlocked, match="live observation deadline"):
        _service(catalog).step(
            account_id="paper-main",
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={"000001.SZ": 0.5},
            market_bars=_bars(
                "2026-01-05",
                execution_available_at="2026-01-05T15:00:00+08:00",
            ),
            snapshot_id="gold-2026-01-02",
            model_version="champion-v1",
        )
    after = catalog.get_shadow_account("paper-main")
    assert after is not None
    assert after.last_event_sequence == before.last_event_sequence


def test_shadow_step_rejects_snapshot_published_after_decision_close(catalog) -> None:
    catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id="gold-after-close",
            tier=SnapshotTier.GOLD,
            uri="s3://factor-lab/gold/after-close",
            content_hash="c" * 64,
            as_of=datetime(2026, 1, 2, 7, 0, 1, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
        )
    )
    _create_account(catalog)
    with pytest.raises(ShadowDataBlocked, match="after the signal decision close"):
        _service(catalog).step(
            account_id="paper-main",
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={},
            market_bars=_bars("2026-01-05"),
            snapshot_id="gold-after-close",
            model_version="champion-v1",
        )


def test_shadow_step_accepts_persisted_after_close_cutoff_before_next_open(catalog) -> None:
    _register_snapshot(catalog)
    original = catalog.get_snapshot("gold-2026-01-02")
    assert original is not None
    late = original.reference.model_copy(
        update={
            "snapshot_id": "gold-after-close-valid",
            "content_hash": "9" * 64,
            "as_of": datetime(2026, 1, 2, 10, 30, tzinfo=timezone.utc),
        }
    )
    catalog.register_snapshot(late)
    _create_account(catalog)
    result = _service(catalog).step(
        account_id="paper-main",
        decision_date="2026-01-02",
        decision_cutoff="2026-01-02T19:00:00+08:00",
        trade_date="2026-01-05",
        expected_next_session="2026-01-05",
        target_weights={},
        market_bars=_bars("2026-01-05", snapshot_id="gold-after-close-valid"),
        snapshot_id="gold-after-close-valid",
        model_version="champion-v1",
    )
    assert result.chain_verified is True
    target = next(
        event
        for event in catalog.list_shadow_events(account_id="paper-main", limit=20)
        if event.event_type == "target_received"
    )
    assert target.payload["timing"]["decision_cutoff"] == (
        "2026-01-02T11:00:00+00:00"
    )


def test_shadow_step_retry_is_rejected_without_double_trading(catalog) -> None:
    _register_snapshot(catalog)
    _create_account(catalog)
    service = _service(catalog)
    arguments = {
        "account_id": "paper-main",
        "decision_date": "2026-01-02",
        "trade_date": "2026-01-05",
        "expected_next_session": "2026-01-05",
        "target_weights": {"000001.SZ": 0.5},
        "market_bars": _bars("2026-01-05"),
        "snapshot_id": "gold-2026-01-02",
        "model_version": "champion-v1",
    }
    first = service.step(**arguments)
    account = catalog.get_shadow_account("paper-main")
    assert account is not None

    with pytest.raises(ShadowStepAlreadyApplied, match=first.step_id):
        service.step(**arguments)
    unchanged = catalog.get_shadow_account("paper-main")
    assert unchanged is not None
    assert unchanged.last_event_sequence == account.last_event_sequence
    assert unchanged.last_event_hash == account.last_event_hash


def test_shadow_step_batch_failure_rolls_back_events_and_projections(catalog, monkeypatch) -> None:
    _register_snapshot(catalog)
    _create_account(catalog)
    service = _service(catalog)
    before = catalog.get_shadow_account("paper-main")
    assert before is not None
    original = catalog.append_shadow_events_atomic

    def append_with_late_backdated_event(*, account_id, events, expected_previous_hash=None):
        poisoned = list(events)
        poisoned.append(
            ShadowEventInput(
                event_type="invalid_backdated_tail",
                occurred_at=DECISION_AS_OF + timedelta(days=1),
                payload={},
            )
        )
        return original(
            account_id=account_id,
            events=poisoned,
            expected_previous_hash=expected_previous_hash,
        )

    monkeypatch.setattr(catalog, "append_shadow_events_atomic", append_with_late_backdated_event)
    with pytest.raises(CatalogConflict, match="chronological"):
        service.step(
            account_id="paper-main",
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={"000001.SZ": 0.5},
            market_bars=_bars("2026-01-05"),
            snapshot_id="gold-2026-01-02",
            model_version="champion-v1",
        )

    after = catalog.get_shadow_account("paper-main")
    assert after is not None
    assert after.last_event_sequence == before.last_event_sequence
    assert after.last_event_hash == before.last_event_hash
    assert catalog.list_shadow_positions("paper-main") == []
    assert catalog.verify_shadow_chain("paper-main") is True


def test_shadow_step_uses_sqlalchemy_authoritative_backend(tmp_path) -> None:
    pytest.importorskip("sqlalchemy")
    database_url = f"sqlite+pysqlite:///{(tmp_path / 'shadow-sqlalchemy.sqlite').as_posix()}"
    with ResearchCatalog(database_url) as catalog:
        catalog.initialize_schema()
        _register_snapshot(catalog)
        _create_account(catalog)
        result = _service(catalog).step(
            account_id="paper-main",
            decision_date="2026-01-02",
            trade_date="2026-01-05",
            expected_next_session="2026-01-05",
            target_weights={"000001.SZ": 0.5},
            market_bars=_bars("2026-01-05"),
            snapshot_id="gold-2026-01-02",
            model_version="champion-v1",
        )
        assert result.chain_verified is True
        assert catalog.get_shadow_account("paper-main").last_event_hash == result.last_event_hash
        assert catalog.list_shadow_positions("paper-main")[0].market_price == 11.0
