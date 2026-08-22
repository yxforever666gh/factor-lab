from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from factor_lab.research_os import production_config as module
from factor_lab.research_os.data_sources import SourceContractError
from factor_lab.research_os.production_config import (
    ProductionOperation,
    ProductionConfigurationError,
    admit_production_operation,
    load_production_config,
    validate_production_config,
    validate_production_environment,
)


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION = ROOT / "configs" / "research_os_orchestration.production.json"


def _minimal_config(tmp_path: Path, **updates) -> Path:
    path = tmp_path / "research_os_orchestration.production.json"
    payload = {
        "schema_version": "research-os/application-services/v1",
        "repository": "/opt/factor-lab",
        "path_base": "/opt/factor-lab/runtime",
        "dependency_lock": "/opt/factor-lab/uv.lock",
        "daily": {
            "sources": [
                {
                    "source": "tushare",
                    "profile_name": "primary-tushare",
                    "credential_ref": "secret://tushare_token",
                    "partition_cadence": {"kind": "trading_session"},
                    "request": {"dataset": "daily"},
                }
            ],
            "bootstrap": {
                "source_start": "2016-06-01",
                "minimum_prewarm_trading_sessions": 120,
                "resume_from_partition_ledger": True,
                "legacy_bronze_seed": {
                    "mode": "hash_verified_checkpoint",
                    "root": "/opt/factor-lab/runtime/data/legacy/expanded_long_only",
                    "checkpoint": "download_checkpoint.json",
                    "datasets": ["daily", "daily_basic", "adj_factor"],
                    "promotion_policy": "bronze_only_fail_closed",
                },
            },
        },
        "sensors": {
            "partition_source": "postgresql_accepted_gold_calendar",
        },
    }
    payload.update(updates)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _environment(path: Path, secret: Path) -> dict[str, str]:
    return {
        "FACTOR_LAB_ENVIRONMENT": "production",
        "FACTOR_LAB_ORCHESTRATION_CONFIG": str(path),
        "FACTOR_LAB_RUNTIME_DATA_ROOT": "/opt/factor-lab/runtime/data",
        "FACTOR_LAB_RUNTIME_ARTIFACT_ROOT": "/opt/factor-lab/runtime/artifacts",
        "FACTOR_LAB_SECRETS_DIR": "/run/secrets",
        "FACTOR_LAB_SECRETS_ROOT": "/run/secrets",
        "FACTOR_LAB_SOURCE_BUNDLE_MANIFEST": "/opt/factor-lab/manifest.json",
        "TUSHARE_TOKEN_FILE": str(secret),
    }


def _replace_primary_source_with_diemeng(path: Path, base_url: str) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["daily"]["sources"] = [
        {
            "source": "diemeng",
            "profile_name": "primary-diemeng",
            "credential_ref": "secret://diemeng_api_key",
            "base_url": base_url,
            "partition_cadence": {"kind": "trading_session"},
            "request": {"dataset": "trade_calendar"},
        }
    ]
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_production_bootstrap_rejects_example_placeholder_windows_and_raw_secret(
    tmp_path: Path,
) -> None:
    example = tmp_path / "research_os_orchestration.example.json"
    example.write_text("{}", encoding="utf-8")
    with pytest.raises(ProductionConfigurationError, match="never an example"):
        load_production_config(example)

    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    placeholder = _minimal_config(tmp_path, role="shadow_forward_challenger")
    with pytest.raises(ProductionConfigurationError, match="placeholder"):
        validate_production_config(
            placeholder,
            env=_environment(placeholder, secret),
            require_mounts=False,
        )


@pytest.mark.parametrize(
    ("container_path", "credential_alias"),
    [
        (("request", "parameters"), "apiKey"),
        (("probe_parameters",), "api-key"),
        (("headers",), "Authorization"),
        (("constant_fields",), "accessToken"),
        (("lineage",), "clientSecret"),
        (("request", "parameters", "nested"), "password"),
    ],
)
def test_production_json_recursively_rejects_credential_shaped_aliases_without_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    container_path: tuple[str, ...],
    credential_alias: str,
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    target = payload["daily"]["sources"][0]
    for segment in container_path:
        target = target.setdefault(segment, {})
    marker = "FAKE-CREDENTIAL-MARKER-MUST-NOT-LEAK"
    target[credential_alias] = marker
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("unused", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "capture_epoch_provenance",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("credential-shaped config reached provenance capture")
        ),
    )

    with pytest.raises(ProductionConfigurationError) as caught:
        validate_production_config(
            config,
            env=_environment(config, secret),
            require_mounts=False,
        )

    assert "credential" in str(caught.value).casefold()
    assert marker not in str(caught.value)


@pytest.mark.parametrize("value", ["env://TUSHARE_TOKEN", "", None, {}])
def test_production_credential_ref_is_the_only_secret_slot_and_requires_secret_scheme(
    tmp_path: Path,
    value: object,
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    payload["daily"]["sources"][0]["credential_ref"] = value
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("unused", encoding="utf-8")

    with pytest.raises(ProductionConfigurationError, match="must use secret://"):
        validate_production_config(
            config,
            env=_environment(config, secret),
            require_mounts=False,
        )


def test_production_rejects_credential_ref_alias_but_accepts_public_security_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    source = payload["daily"]["sources"][0]
    reference = source.pop("credential_ref")
    source["credentialRef"] = reference
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("unused", encoding="utf-8")
    with pytest.raises(ProductionConfigurationError, match="raw credential field"):
        validate_production_config(
            config,
            env=_environment(config, secret),
            require_mounts=False,
        )

    source.pop("credentialRef")
    source["credential_ref"] = reference
    payload["source_capabilities"] = {
        "credential_policy": "secret_ref_only",
        "token_usage": "public aggregate counter",
    }
    payload["security"] = {
        "credential_rotation": {
            "tushare_token": {
                "status": "pending_vendor_rotation",
                "vendor_confirmation": "pending",
            },
            "diemeng_api_key": {
                "status": "pending_vendor_rotation",
                "vendor_confirmation": "pending",
            },
        }
    }
    config.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())

    evidence = validate_production_config(
        config,
        env=_environment(config, secret),
        require_mounts=False,
    )
    assert evidence.credential_refs == ("secret://tushare_token",)


def test_credential_rotation_status_object_cannot_tunnel_unreviewed_material(
    tmp_path: Path,
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    marker = "FAKE-CREDENTIAL-MARKER-MUST-NOT-LEAK"
    payload["security"] = {
        "credential_rotation": {
            "tushare_token": {
                "status": "pending_vendor_rotation",
                "vendor_confirmation": "pending",
                "value": marker,
            }
        }
    }
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("unused", encoding="utf-8")

    with pytest.raises(ProductionConfigurationError) as caught:
        validate_production_config(
            config,
            env=_environment(config, secret),
            require_mounts=False,
        )
    assert marker not in str(caught.value)


@pytest.mark.parametrize(
    ("base_url", "message"),
    [
        ("http://data.diemeng.chat/api", "HTTPS"),
        ("https://attacker.invalid/api", "data.diemeng.chat"),
        ("https://user:password@data.diemeng.chat/api", "userinfo"),
    ],
)
def test_production_rejects_unsafe_diemeng_routes_before_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base_url: str,
    message: str,
) -> None:
    config = _minimal_config(tmp_path)
    _replace_primary_source_with_diemeng(config, base_url)
    secret = tmp_path / "secret"
    secret.write_text("unused", encoding="utf-8")
    credential_reads = 0

    def forbidden_resolution(*_args, **_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        raise AssertionError("unsafe route reached credential resolution")

    monkeypatch.setattr(module, "resolve_credential_ref", forbidden_resolution)
    with pytest.raises(ProductionConfigurationError, match=message):
        validate_production_config(
            config, env=_environment(config, secret), require_mounts=False
        )
    assert credential_reads == 0


def test_production_diemeng_profile_route_is_repetition_not_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    _replace_primary_source_with_diemeng(
        config, "https://data.diemeng.chat/api"
    )
    secret = tmp_path / "secret"
    secret.write_text("unused", encoding="utf-8")
    env = _environment(config, secret)
    env["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"] = json.dumps(
        [
            {
                "name": "primary-diemeng",
                "source_type": "diemeng",
                "credential_ref": "secret://diemeng_api_key",
                "enabled": True,
                "extra": {"base_url": "https://data.diemeng.chat/other"},
            }
        ]
    )
    credential_reads = 0

    def forbidden_resolution(*_args, **_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        raise AssertionError("override reached credential resolution")

    monkeypatch.setattr(module, "resolve_credential_ref", forbidden_resolution)
    with pytest.raises(ProductionConfigurationError, match="must match reviewed"):
        validate_production_config(config, env=env, require_mounts=False)
    assert credential_reads == 0


def test_production_profile_extra_rejects_credential_looking_plaintext(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    _replace_primary_source_with_diemeng(
        config, "https://data.diemeng.chat/api"
    )
    secret = tmp_path / "secret"
    secret.write_text("unused", encoding="utf-8")
    env = _environment(config, secret)
    env["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"] = json.dumps(
        [
            {
                "name": "primary-diemeng",
                "source_type": "diemeng",
                "credential_ref": "secret://diemeng_api_key",
                "enabled": True,
                "extra": {
                    "base_url": "https://data.diemeng.chat/api",
                    "fallback_auth_token": "inline-material",
                },
            }
        ]
    )
    monkeypatch.setattr(
        module,
        "resolve_credential_ref",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw profile credential reached resolution")
        ),
    )
    with pytest.raises(ProductionConfigurationError, match="raw credential"):
        validate_production_config(config, env=env, require_mounts=False)

    windows = _minimal_config(tmp_path, input_path=r"H:\runtime\input.json")
    with pytest.raises(ProductionConfigurationError, match="Windows host path"):
        validate_production_config(
            windows,
            env=_environment(windows, secret),
            require_mounts=False,
        )

    with pytest.raises(ProductionConfigurationError, match="raw credentials"):
        validate_production_environment(
            {**_environment(windows, secret), "TUSHARE_TOKEN": "raw"},
            config_path=windows,
            require_mounts=False,
        )


def test_production_bootstrap_accepts_only_measured_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    sentinel = object()
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: sentinel)

    evidence = validate_production_config(
        config,
        env=_environment(config, secret),
        require_mounts=False,
    )

    assert evidence.provenance is sentinel
    assert evidence.credential_refs == ("secret://tushare_token",)
    assert evidence.status == "config_valid_canary_pending"
    assert evidence.formal_execution_capable is False
    assert evidence.historical_backfill_allowed is False
    assert evidence.formal_forward_evidence is False
    assert set(evidence.readiness_blockers) == {
        "daemon_inspected_oci_provenance_missing",
        "formal_execution_adapter_insufficient",
        "tushare_token_post_exposure_rotation_pending",
        "diemeng_api_key_post_exposure_rotation_pending",
        "tushare_https_transport_unverified",
        "persisted_production_readiness_audit_missing",
    }
    canary = admit_production_operation(
        evidence, ProductionOperation.ENGINEERING_CANARY
    )
    assert canary.allowed is False
    assert set(canary.blockers) == {
        "tushare_token_post_exposure_rotation_pending",
        "diemeng_api_key_post_exposure_rotation_pending",
        "tushare_https_transport_unverified",
    }
    probe = admit_production_operation(
        evidence, ProductionOperation.CALENDAR_CAPABILITY_PROBE
    )
    assert probe.allowed is False
    assert set(probe.blockers) == set(canary.blockers)
    backfill = admit_production_operation(
        evidence, ProductionOperation.AUTHORITATIVE_HISTORICAL_BACKFILL
    )
    assert backfill.allowed is False
    assert set(backfill.blockers) == {
        "tushare_token_post_exposure_rotation_pending",
        "diemeng_api_key_post_exposure_rotation_pending",
        "tushare_https_transport_unverified",
    }
    forward = admit_production_operation(
        evidence, ProductionOperation.FORMAL_FORWARD_ACTIVATION
    )
    assert forward.allowed is False
    assert set(forward.blockers) == set(evidence.readiness_blockers)


def test_unverified_tushare_transport_never_reads_exposed_token_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _minimal_config(tmp_path)
    secret = tmp_path / "must-not-open"
    secret.write_text("exposed-token", encoding="utf-8")
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())
    monkeypatch.setattr(
        module,
        "require_tushare_sdk_https_transport",
        lambda: (_ for _ in ()).throw(SourceContractError("HTTP blocked")),
    )
    reads: list[str] = []

    def forbidden_resolution(reference: str, **_kwargs):
        reads.append(reference)
        raise AssertionError("blocked Tushare credential was read")

    monkeypatch.setattr(module, "resolve_credential_ref", forbidden_resolution)
    evidence = validate_production_config(
        config,
        env=_environment(config, secret),
        require_mounts=False,
    )

    assert reads == []
    assert evidence.source_transport_blockers == (
        "tushare_https_transport_unverified",
    )


def test_static_config_never_self_approves_formal_epoch_readiness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    payload["daily"]["shadow"] = {
        "execution_market_data": {
            "source": "authoritative_exchange_events",
            "dataset": "accepted_open_execution",
            "formal_capability": {
                "status": "accepted",
                "formal_shadow_projection": "allowed",
            },
        }
    }
    payload["security"] = {
        "credential_rotation": {
            "tushare_token": {
                "status": "verified_post_exposure",
                "vendor_confirmation": "recorded",
            },
            "diemeng_api_key": {
                "status": "verified_post_exposure",
                "vendor_confirmation": "recorded",
            },
        }
    }
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")

    class FormalProof:
        formal_epoch_eligible = True

    monkeypatch.setattr(
        module, "capture_epoch_provenance", lambda **_kwargs: FormalProof()
    )

    with pytest.raises(
        ProductionConfigurationError,
        match="reviewed closed-set adapter",
    ):
        validate_production_config(
            config,
            env=_environment(config, secret),
            require_mounts=False,
            image_reference="factor-lab-research-os:verified",
        )


def test_tushare_realtime_open_is_structurally_capable_but_runtime_probe_gated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    payload["daily"]["shadow"] = {
        "execution_market_data": {
            "source": "tushare",
            "profile_name": "primary-tushare",
            "credential_ref": "secret://tushare_token",
            "dataset": "rt_min",
            "endpoint": "rt_min",
            "method": "SDK",
            "request": {
                "ts_code": "${decision_universe_csv}",
                "freq": "1MIN",
            },
            "batching": {
                "mode": "sorted_deterministic_chunks",
                "maximum_symbols_per_request": 300,
            },
            "contract": {
                "key_fields": ["ts_code", "time"],
                "event_time_field": "time",
                "fields": [
                    "ts_code",
                    "time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
            },
            "availability": {
                "mode": "collector_ingested_at",
                "event_time_field": "time",
                "available_at_field": "ingested_at",
                "maximum_delay_minutes": 5,
            },
            "formal_capability": {
                "status": "runtime_probe_required",
                "formal_shadow_projection": "runtime_probe_gated",
            },
            "end_of_day_mark": {
                "source": "accepted_gold_close_snapshot",
            },
        }
    }
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())

    evidence = validate_production_config(
        config,
        env=_environment(config, secret),
        require_mounts=False,
    )

    assert evidence.formal_execution_capable is True
    assert "formal_execution_adapter_insufficient" not in evidence.readiness_blockers
    assert "persisted_production_readiness_audit_missing" in evidence.readiness_blockers


def test_tushare_realtime_open_rejects_event_time_as_fake_availability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    payload["daily"]["shadow"] = {
        "execution_market_data": {
            "source": "tushare",
            "profile_name": "primary-tushare",
            "credential_ref": "secret://tushare_token",
            "dataset": "rt_min",
            "endpoint": "rt_min",
            "method": "SDK",
            "request": {
                "ts_code": "${decision_universe_csv}",
                "freq": "1MIN",
            },
            "batching": {
                "mode": "sorted_deterministic_chunks",
                "maximum_symbols_per_request": 300,
            },
            "contract": {
                "key_fields": ["ts_code", "time"],
                "event_time_field": "time",
                "fields": [
                    "ts_code",
                    "time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
            },
            "availability": {
                "mode": "event_timestamp",
                "event_time_field": "time",
                "available_at_field": "time",
                "maximum_delay_minutes": 5,
            },
            "formal_capability": {
                "status": "runtime_probe_required",
                "formal_shadow_projection": "runtime_probe_gated",
            },
            "end_of_day_mark": {"source": "accepted_gold_close_snapshot"},
        }
    }
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())

    with pytest.raises(ProductionConfigurationError, match="collector ingested_at"):
        validate_production_config(
            config,
            env=_environment(config, secret),
            require_mounts=False,
        )


def test_checked_in_production_config_has_no_self_reported_secrets_or_ids() -> None:
    text = PRODUCTION.read_text(encoding="utf-8")
    payload = json.loads(text)

    assert payload["repository"] == "/opt/factor-lab"
    assert payload["path_base"] == "/opt/factor-lab/runtime"
    assert "token_env" not in text
    assert "exp_catalog_authoritative_" not in text
    assert "shadow_forward_" not in text
    assert '"source": "local_file"' not in text
    for forbidden in (
        "market_bars_path",
        "monitor_inputs",
        "research_inputs",
        "trading_partitions_path",
    ):
        assert forbidden not in text
    assert payload["monthly"]["challenger"]["authority_mode"] == "catalog_roles"
    assert payload["monthly"]["proposal"] == {
        "provider": "direct_model",
        "profile_source": "runtime_env",
        "credential_policy": "secret_ref_only",
        "max_proposals_per_month": 3,
        "max_proposals_per_family": 1,
    }
    assert payload["daily"]["shadow"]["input_mode"] == "authoritative_pg"
    assert payload["monthly"]["weights"]["input_mode"] == "authoritative_pg"
    assert payload["daily"]["bootstrap"]["source_start"] == "2016-06-01"
    assert payload["daily"]["bootstrap"]["minimum_prewarm_trading_sessions"] >= 120
    assert payload["daily"]["bootstrap"]["legacy_bronze_seed"] == {
        "mode": "hash_verified_checkpoint",
        "root": "/opt/factor-lab/runtime/data/legacy/expanded_long_only",
        "checkpoint": "download_checkpoint.json",
        "datasets": ["daily", "daily_basic", "adj_factor"],
        "promotion_policy": "bronze_only_fail_closed",
    }
    assert payload["sensors"]["partition_source"] == "postgresql_accepted_gold_calendar"
    assert payload["security"]["credential_rotation"]["tushare_token"] == {
        "status": "pending_vendor_rotation",
        "vendor_confirmation": "pending",
        "required_before": [
            "historical_backfill",
            "formal_forward_epoch_activation",
        ],
    }
    assert payload["security"]["credential_rotation"]["diemeng_api_key"] == {
        "status": "pending_vendor_rotation",
        "vendor_confirmation": "pending",
        "required_before": [
            "authoritative_calendar_bootstrap",
            "historical_backfill",
            "formal_forward_epoch_activation",
        ],
    }

    for source in payload["daily"]["sources"]:
        assert source["partition_cadence"]["kind"] in {
            "trading_session",
            "static_snapshot",
            "event_date",
        }
        if source["source"] in {"tushare", "diemeng"}:
            assert source["credential_ref"].startswith("secret://")
        if source["source"] == "tushare":
            assert source["profile_name"] == "primary-tushare"
        if source["source"] == "diemeng":
            assert source["profile_name"] == "primary-diemeng"
        availability = source.get("canonicalization", {}).get("availability")
        if availability is None:
            continue
        dataset = source["request"]["dataset"]
        if dataset in {"trade_calendar", "daily", "daily_basic", "adj_factor"}:
            assert availability["lag_days"] == 0
        else:
            assert availability["lag_days"] == 1
        if dataset in {"daily", "daily_basic", "adj_factor"}:
            released = datetime.fromisoformat(
                f"2026-08-21T{availability['time']}+08:00"
            )
            next_session_open = datetime.fromisoformat("2026-08-24T09:30:00+08:00")
            assert released < next_session_open
    assert (
        payload["daily"]["shadow"]["execution_market_data"]["profile_name"]
        == "primary-diemeng"
    )
    execution = payload["daily"]["shadow"]["execution_market_data"]
    assert execution["request"] == {
        "stock_code": "${ticker}",
        "level": "1min",
        "start_time": "${partition_key} 09:30:00",
        "end_time": "${partition_key} 15:00:00",
        "page": 0,
        "page_size": 10000,
    }
    assert execution["availability"] == {
        "mode": "event_timestamp",
        "event_time_field": "trade_time",
        "available_at_field": "trade_time",
    }
    assert execution["contract"]["execution_observation"]["required_local_time"] == "09:30:00"
    assert execution["formal_capability"]["formal_shadow_projection"] == "blocked"
    assert execution["end_of_day_mark"]["source"] == "accepted_gold_close_snapshot"
    stock_limit = next(
        source
        for source in payload["daily"]["sources"]
        if source["request"]["dataset"] == "stock_limit"
    )
    stock_limit_fields = {
        field["name"]: field for field in stock_limit["contract"]["fields"]
    }
    assert stock_limit_fields["pre_close"]["nullable"] is True
    assert stock_limit_fields["up_limit"]["nullable"] is False
    assert stock_limit_fields["down_limit"]["nullable"] is False


def test_production_profile_ledger_is_exact_and_never_contains_raw_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    env = _environment(config, secret)
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())

    env["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"] = json.dumps(
        [
            {
                "name": "primary-tushare",
                "source_type": "tushare",
                "credential_ref": "secret://tushare_token",
                "api_key": "",
                "enabled": True,
            }
        ]
    )
    validate_production_config(config, env=env, require_mounts=False)

    raw = json.loads(env["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"])
    raw[0]["api_key"] = "must-not-be-in-env-file"
    env["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"] = json.dumps(raw)
    with pytest.raises(ProductionConfigurationError, match="raw credential"):
        validate_production_config(config, env=env, require_mounts=False)

    wrong = json.loads(json.dumps(raw))
    wrong[0]["api_key"] = ""
    wrong[0]["name"] = "renamed-tushare"
    env["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"] = json.dumps(wrong)
    with pytest.raises(ProductionConfigurationError, match="absent or disabled"):
        validate_production_config(config, env=env, require_mounts=False)


def test_production_legacy_seed_path_and_promotion_policy_are_fixed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())

    payload = json.loads(config.read_text(encoding="utf-8"))
    payload["daily"]["bootstrap"]["legacy_bronze_seed"]["root"] = (
        "/tmp/operator-selected-seed"
    )
    config.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProductionConfigurationError, match="fixed runtime-data"):
        validate_production_config(
            config, env=_environment(config, secret), require_mounts=False
        )

    payload["daily"]["bootstrap"]["legacy_bronze_seed"]["root"] = (
        "/opt/factor-lab/runtime/data/legacy/expanded_long_only"
    )
    payload["daily"]["bootstrap"]["legacy_bronze_seed"]["promotion_policy"] = (
        "promote_to_gold"
    )
    config.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProductionConfigurationError, match="Bronze-only"):
        validate_production_config(
            config, env=_environment(config, secret), require_mounts=False
        )

def test_production_rejects_missing_static_source_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    payload["daily"]["sources"][0]["request"]["dataset"] = "stock_basic_l"
    payload["daily"]["sources"][0].pop("partition_cadence")
    config.write_text(json.dumps(payload), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())

    with pytest.raises(ProductionConfigurationError, match="partition_cadence"):
        validate_production_config(
            config, env=_environment(config, secret), require_mounts=False
        )


def test_production_rejects_inline_llm_profile_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _minimal_config(tmp_path)
    secret = tmp_path / "secret"
    secret.write_text("credential", encoding="utf-8")
    env = _environment(config, secret)
    env["FACTOR_LAB_LLM_PROFILES_JSON"] = json.dumps(
        [{"name": "primary", "api_key": "raw-key", "credential_ref": ""}]
    )
    monkeypatch.setattr(module, "capture_epoch_provenance", lambda **_kwargs: object())

    with pytest.raises(ProductionConfigurationError, match="raw credential"):
        validate_production_config(config, env=env, require_mounts=False)
