from __future__ import annotations

import json
from pathlib import Path

import pytest

from factor_lab.research_os.proposal_ports import (
    DirectModelProposalPort,
    ProposalPortError,
    proposal_port_from_config,
)


def _proposal() -> dict:
    return {
        "preregistration": {
            "hypothesis_id": "direct-model-value",
            "economic_mechanism": "valuation dispersion may mean revert",
            "direction": "positive",
            "falsification_criteria": ["outer OOS active return is non-positive"],
            "stop_rules": ["stop after the frozen protocol"],
        },
        "factor": {
            "factor_id": "direct-model-value",
            "family": "value_quality_v1",
            "name": "Direct model value",
            "mechanism": "valuation dispersion may mean revert",
            "expression": {
                "schema_version": "research-os/factor-dsl/v1",
                "output_id": "ranked",
                "nodes": [
                    {"id": "raw", "op": "field", "field": "book_to_price"},
                    {"id": "ranked", "op": "rank", "input": "raw"},
                ],
            },
            "direction": "higher_is_better",
            "falsification_criteria": ["outer OOS active return is non-positive"],
        },
    }


class Response:
    def raise_for_status(self) -> None:
        return None

    def json(self):
        return {"output_text": json.dumps(_proposal())}


class Session:
    def __init__(self) -> None:
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response()


def test_direct_model_reads_secret_ref_and_returns_proposal_only(
    tmp_path: Path,
) -> None:
    secret = tmp_path / "llm-key"
    secret.write_text("secret-value\n", encoding="utf-8")
    profiles = [
        {
            "name": "primary",
            "base_url": "https://model.example/v1",
            "model": "research-model",
            "api_format": "openai_responses",
            "credential_ref": "secret://llm_primary",
            "enabled": True,
        }
    ]
    env = {
        "FACTOR_LAB_LLM_PROFILES_JSON": json.dumps(profiles),
        "FACTOR_LAB_LLM_FALLBACK_ORDER": "primary",
        "LLM_PRIMARY_FILE": str(secret),
    }
    session = Session()
    port = DirectModelProposalPort.from_environment(env, session=session)
    result = port.propose(
        {
            "family_id": "value_quality_v1",
            "mechanism_key": "value-quality",
            "allowed_fields": ["book_to_price"],
            "field_registry_hash": "a" * 64,
        }
    )

    assert result == _proposal()
    assert set(result) == {"preregistration", "factor"}
    url, call = session.calls[0]
    assert url == "https://model.example/v1/responses"
    assert call["headers"]["Authorization"] == "Bearer secret-value"
    assert "secret-value" not in json.dumps(call["json"])
    assert '"allowed_fields":["book_to_price"]' in call["json"]["input"][1]["content"]


def test_production_rejects_raw_key_and_test_providers(tmp_path: Path) -> None:
    secret = tmp_path / "llm-key"
    secret.write_text("secret-value", encoding="utf-8")
    with pytest.raises(ProposalPortError, match="raw"):
        DirectModelProposalPort.from_environment(
            {
                "FACTOR_LAB_LLM_BASE_URL": "https://model.example/v1",
                "FACTOR_LAB_LLM_MODEL": "model",
                "FACTOR_LAB_LLM_API_KEY_REF": "secret://llm_primary",
                "FACTOR_LAB_LLM_API_KEY": "forbidden",
                "LLM_PRIMARY_FILE": str(secret),
            }
        )
    for provider in ("heuristic", "mock"):
        with pytest.raises(ProposalPortError, match="provider=direct_model"):
            proposal_port_from_config(
                {"provider": provider, "payload": _proposal()},
                env={},
                production=True,
            )


def test_heuristic_is_available_only_for_explicit_test_mode() -> None:
    port = proposal_port_from_config(
        {"provider": "heuristic"}, env={}, production=False
    )
    payload = port.propose(
        {
            "family_id": "value_quality_v1",
            "allowed_fields": ["book_to_price"],
        }
    )
    assert payload["factor"]["family"] == "value_quality_v1"
    assert set(payload) == {"preregistration", "factor"}
