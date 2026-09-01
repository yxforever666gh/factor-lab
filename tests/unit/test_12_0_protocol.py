from __future__ import annotations

import json
from pathlib import Path

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "protocols" / "12.0-quarterly-pit-stock.json"


def test_protocol_identity_and_hashes_are_frozen() -> None:
    value = json.loads(PATH.read_text(encoding="utf-8"))
    assert value["protocol_id"] == "factor-lab/12.0/quarterly-pit-stock-v1"
    assert value["release"] == "12.0"
    assert value["status"] == "development_screening_falsified_max_drawdown_selection_unopened"
    assert value["payload_sha256"] == canonical_payload_sha256(value)
    assert file_sha256(PATH) == "0ba5356d99befe02dd2c8053c6ef360ade823f1d8ddc65a937095acedcc675ee"


def test_protocol_does_not_confuse_synthetic_and_real_share_execution() -> None:
    value = json.loads(PATH.read_text(encoding="utf-8"))
    screening = value["screening_execution_contract"]
    production = value["real_share_production_gate"]
    assert screening["price_basis"] == "adjusted_total_return"
    assert screening["lot_size"] == 0
    assert screening["real_100_share_claim_allowed"] is False
    assert production["required_before_winner_freeze_and_selection_open"] is True
    assert production["buy_lot_size_shares"] == 100
    assert production["adjusted_price_times_100_synthetic_units_forbidden"] is True


def test_terminal_failure_keeps_selection_closed() -> None:
    value = json.loads(PATH.read_text(encoding="utf-8"))
    result = value["terminal_screening_result"]
    assert result["selected_candidate_id"] is None
    assert result["failed_check"] == "base_max_drawdown_at_least_negative_0_35"
    assert result["selection_market_partitions_read"] is False
    assert result["real_share_gate_opened"] is False
    assert result["runner_up_fallback"] is False
