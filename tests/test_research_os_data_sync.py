from pathlib import Path
import json
from types import SimpleNamespace
import traceback

import pandas as pd
import pytest

from factor_lab.research_os import data_sync as data_sync_module
from factor_lab.research_os.data_sync import (
    BronzeObservationError,
    bind_production_source_transport,
    resolve_credential,
    source_adapter_from_mapping,
    sync_bronze,
)
from factor_lab.research_os.data_sources import (
    DiemengSourceAdapter,
    LocalFileSourceAdapter,
    TushareSourceAdapter,
    tushare_client_uses_direct_transport,
)
from factor_lab.research_os.data_sources import (
    FetchRequest,
    SourceContractError,
    SourceHealth,
    SourceObservationError,
)
from factor_lab.research_os.object_store import S3ImmutableArchive
from factor_lab.research_os.fingerprint import content_fingerprint


class _MemoryWriter(__import__("io").BytesIO):
    def __init__(self, filesystem, path):
        super().__init__()
        self.filesystem = filesystem
        self.path = path

    def close(self):
        self.filesystem.objects[self.path] = self.getvalue()
        super().close()


class _MemoryFileSystem:
    def __init__(self):
        self.objects = {}

    def exists(self, path):
        return path in self.objects

    def open(self, path, mode="rb"):
        if mode == "rb":
            return __import__("io").BytesIO(self.objects[path])
        return _MemoryWriter(self, path)


def _local_spec(root: Path, *, profile_name: str | None = None) -> dict:
    spec = {
        "source": "local_file",
        "root": str(root),
        "path_templates": {"daily": "daily.parquet"},
        "contract": {
            "dataset": "daily",
            "key_fields": ["trade_date", "ticker"],
            "event_time_field": "trade_date",
            "release_timing": "same_session_close",
            "fields": [
                {"name": "trade_date", "dtype": "date", "nullable": False},
                {"name": "ticker", "dtype": "string", "nullable": False},
                {"name": "close", "dtype": "float", "nullable": False},
            ],
        },
        "request": {"dataset": "daily"},
    }
    if profile_name:
        spec["profile_name"] = profile_name
    return spec


def _diemeng_spec(*, profile_name: str) -> dict:
    return {
        "source": "diemeng",
        "profile_name": profile_name,
        "base_url": "https://data.diemeng.chat/api",
        "endpoint_map": {"daily": "/stock/history"},
        "method_map": {"daily": "POST"},
        "response_paths": {"daily": "data.items"},
        "contract": {
            "dataset": "daily",
            "key_fields": ["trade_date", "ticker"],
            "event_time_field": "trade_date",
            "release_timing": "after_close",
            "fields": [
                {"name": "trade_date", "dtype": "date", "nullable": False},
                {"name": "ticker", "dtype": "string", "nullable": False},
                {"name": "close", "dtype": "float", "nullable": False},
            ],
        },
        "request": {"dataset": "daily"},
    }


def _tushare_spec() -> dict:
    return {
        "source": "tushare",
        "profile_name": "primary-tushare",
        "credential_ref": "secret://tushare_token",
        "api_origin": "https://api.tushare.pro/dataapi",
        "endpoint_map": {"daily": "daily"},
        "contract": {
            "dataset": "daily",
            "key_fields": ["trade_date", "ticker"],
            "event_time_field": "trade_date",
            "release_timing": "after_close",
            "fields": [
                {"name": "trade_date", "dtype": "date", "nullable": False},
                {"name": "ticker", "dtype": "string", "nullable": False},
                {"name": "close", "dtype": "float", "nullable": False},
            ],
        },
        "request": {"dataset": "daily"},
    }


def test_source_profile_exactly_binds_local_root_without_exposing_secret(tmp_path: Path) -> None:
    configured = tmp_path / "configured"
    profile_root = tmp_path / "profile-root"
    profile_root.mkdir()
    env = {
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
            [
                {
                    "name": "pit-reference-local",
                    "source_type": "local_file",
                    "enabled": True,
                    "api_key": "must-not-enter-lineage",
                    "extra": {"root": str(profile_root), "probe_file": "probe.parquet"},
                }
            ]
        ),
        "FACTOR_LAB_DATA_SOURCE_ORDER": "pit-reference-local",
    }

    adapter = source_adapter_from_mapping(
        _local_spec(configured, profile_name="pit-reference-local"), env=env
    )

    assert isinstance(adapter, LocalFileSourceAdapter)
    assert adapter.root == profile_root.resolve()
    assert adapter.lineage["profile_name"] == "pit-reference-local"
    assert adapter.lineage["profile_source_type"] == "local_file"
    assert adapter.lineage["local_root"] == str(profile_root.resolve())
    assert adapter.lineage["path_templates"] == {"daily": "daily.parquet"}
    assert "must-not-enter-lineage" not in repr(adapter.lineage)


def test_inline_local_spec_ignores_unrelated_ambient_profile_ledger(tmp_path: Path) -> None:
    adapter = source_adapter_from_mapping(
        _local_spec(tmp_path),
        env={
            "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
                [
                    {
                        "name": "primary-tushare",
                        "source_type": "tushare",
                        "enabled": True,
                    }
                ]
            )
        },
    )
    assert isinstance(adapter, LocalFileSourceAdapter)
    assert adapter.root == tmp_path.resolve()


def test_profile_binding_fails_closed_when_profile_is_absent_or_disabled(tmp_path: Path) -> None:
    env = {
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
            [
                {
                    "name": "pit-reference-local",
                    "source_type": "local_file",
                    "enabled": False,
                    "extra": {"root": str(tmp_path)},
                }
            ]
        )
    }
    with pytest.raises(ValueError, match="absent or disabled"):
        source_adapter_from_mapping(
            _local_spec(tmp_path, profile_name="pit-reference-local"), env=env
        )


def test_secret_credential_reference_binds_diemeng_without_inline_value(
    tmp_path: Path,
) -> None:
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "diemeng_api_key").write_text("fixture-secret\n", encoding="utf-8")
    env = {
        "FACTOR_LAB_SECRETS_DIR": str(secrets),
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
            [
                {
                    "name": "secondary-diemeng",
                    "source_type": "diemeng",
                    "enabled": True,
                    "credential_ref": "secret://diemeng_api_key",
                    "extra": {"base_url": "https://diemeng.chat/api"},
                }
            ]
        ),
        "FACTOR_LAB_DATA_SOURCE_ORDER": "secondary-diemeng",
    }

    adapter = source_adapter_from_mapping(
        _diemeng_spec(profile_name="secondary-diemeng"), env=env
    )

    assert isinstance(adapter, DiemengSourceAdapter)
    assert adapter.base_url == "https://data.diemeng.chat/api"
    assert adapter.lineage["credential_binding"] == "secret://diemeng_api_key"
    assert "fixture-secret" not in repr(adapter.lineage)


@pytest.mark.parametrize(
    ("reviewed_url", "profile_url", "message"),
    [
        ("http://data.diemeng.chat/api", None, "HTTPS"),
        ("https://attacker.invalid/api", None, "data.diemeng.chat"),
        (
            "https://user:password@data.diemeng.chat/api",
            None,
            "userinfo",
        ),
        (
            "https://data.diemeng.chat/api",
            "https://data.diemeng.chat/other",
            "must match reviewed",
        ),
    ],
)
def test_diemeng_routing_fails_before_credential_resolution(
    monkeypatch: pytest.MonkeyPatch,
    reviewed_url: str,
    profile_url: str | None,
    message: str,
) -> None:
    spec = _diemeng_spec(profile_name="secondary-diemeng")
    spec["base_url"] = reviewed_url
    extra = {} if profile_url is None else {"base_url": profile_url}
    env = {
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
            [
                {
                    "name": "secondary-diemeng",
                    "source_type": "diemeng",
                    "enabled": True,
                    "credential_ref": "secret://not-read",
                    "extra": extra,
                }
            ]
        )
    }
    credential_reads = 0

    def forbidden_credential_read(**_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        raise AssertionError("unsafe routing reached credential resolution")

    monkeypatch.setattr(
        data_sync_module, "resolve_credential", forbidden_credential_read
    )
    with pytest.raises(ValueError, match=message):
        source_adapter_from_mapping(spec, env=env)
    assert credential_reads == 0


def test_diemeng_profile_base_url_cannot_supply_missing_reviewed_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _diemeng_spec(profile_name="secondary-diemeng")
    spec.pop("base_url")
    env = {
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
            [
                {
                    "name": "secondary-diemeng",
                    "source_type": "diemeng",
                    "credential_ref": "secret://not-read",
                    "extra": {"base_url": "https://data.diemeng.chat/api"},
                }
            ]
        )
    }
    monkeypatch.setattr(
        data_sync_module,
        "resolve_credential",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("credential must not be read")
        ),
    )
    with pytest.raises(ValueError, match="cannot replace reviewed"):
        source_adapter_from_mapping(spec, env=env)


@pytest.mark.parametrize(
    "configured_origin",
    [
        None,
        "http://api.tushare.pro/dataapi",
        "https://attacker.invalid/dataapi",
        "https://user:password@api.tushare.pro/dataapi",
        "https://api.tushare.pro:444/dataapi",
        "https://api.tushare.pro/other",
        "https://api.tushare.pro/dataapi?route=other",
        "https://api.tushare.pro/dataapi#fragment",
    ],
)
def test_unsafe_tushare_route_blocks_before_credential_resolution(
    monkeypatch: pytest.MonkeyPatch,
    configured_origin: str | None,
) -> None:
    credential_reads = 0

    def forbidden_credential_read(**_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        raise AssertionError("unsafe Tushare route reached credential resolution")

    monkeypatch.setattr(
        data_sync_module, "resolve_credential", forbidden_credential_read
    )
    spec = _tushare_spec()
    if configured_origin is None:
        spec.pop("api_origin")
    else:
        spec["api_origin"] = configured_origin
    with pytest.raises(ValueError, match="HTTPS origin") as caught:
        source_adapter_from_mapping(
            spec,
            env={
                "FACTOR_LAB_ENVIRONMENT": "production",
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
                    [
                        {
                            "name": "primary-tushare",
                            "source_type": "tushare",
                            "enabled": True,
                            "credential_ref": "secret://tushare_token",
                        }
                    ]
                ),
            },
        )

    assert credential_reads == 0
    assert "tushare_token" not in str(caught.value)


def test_tushare_profile_origin_cannot_replace_reviewed_route_before_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _tushare_spec()
    spec.pop("api_origin")
    monkeypatch.setattr(
        data_sync_module,
        "resolve_credential",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("profile route reached credential resolution")
        ),
    )
    with pytest.raises(ValueError, match="cannot replace reviewed"):
        source_adapter_from_mapping(
            spec,
            env={
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
                    [
                        {
                            "name": "primary-tushare",
                            "source_type": "tushare",
                            "enabled": True,
                            "credential_ref": "secret://tushare_token",
                            "extra": {
                                "api_origin": "https://api.tushare.pro/dataapi"
                            },
                        }
                    ]
                )
            },
        )


def test_production_transport_authority_injects_one_canonical_tushare_origin() -> None:
    source = _tushare_spec()
    source.pop("api_origin")
    production_config = {
        "security": {
            "source_transport": {
                "tushare": {
                    "api_origin": "HTTPS://API.TUSHARE.PRO:443/dataapi/"
                }
            }
        }
    }

    bound = bind_production_source_transport(
        source,
        production_config=production_config,
    )

    assert "api_origin" not in source
    assert bound["api_origin"] == "https://api.tushare.pro/dataapi"


def test_production_source_fingerprint_uses_canonical_tushare_origin() -> None:
    source = _tushare_spec()
    source.pop("api_origin")

    def bind(origin: str) -> dict:
        return bind_production_source_transport(
            source,
            production_config={
                "security": {
                    "source_transport": {"tushare": {"api_origin": origin}}
                }
            },
        )

    canonical = bind("https://api.tushare.pro/dataapi")
    equivalent = bind("HTTPS://API.TUSHARE.PRO:443/dataapi/")
    alternate = bind("https://api.waditu.com/dataapi")
    domain = "factor-lab/research-os/v1/source-partition-input"

    assert content_fingerprint(canonical, domain=domain) == content_fingerprint(
        equivalent,
        domain=domain,
    )
    assert content_fingerprint(canonical, domain=domain) != content_fingerprint(
        alternate,
        domain=domain,
    )


def test_production_transport_authority_rejects_source_route_conflict() -> None:
    source = _tushare_spec()
    source["api_origin"] = "https://api.waditu.com/dataapi"

    with pytest.raises(ValueError, match="conflicts"):
        bind_production_source_transport(
            source,
            production_config={
                "security": {
                    "source_transport": {
                        "tushare": {
                            "api_origin": "https://api.tushare.pro/dataapi"
                        }
                    }
                }
            },
        )


@pytest.mark.parametrize(
    "endpoint",
    ["query", "Query", "_DataApi__token"],
)
def test_reserved_tushare_endpoints_fail_before_credential_resolution(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
) -> None:
    credential_reads = 0

    def forbidden_credential_read(**_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        raise AssertionError("reserved endpoint reached credential resolution")

    monkeypatch.setattr(
        data_sync_module,
        "resolve_credential",
        forbidden_credential_read,
    )
    spec = _tushare_spec()
    spec["endpoint_map"] = {"daily": endpoint}

    with pytest.raises(SourceContractError, match="invalid or reserved"):
        source_adapter_from_mapping(
            spec,
            env={
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
                    [
                        {
                            "name": "primary-tushare",
                            "source_type": "tushare",
                            "enabled": True,
                            "credential_ref": "secret://tushare_token",
                        }
                    ]
                )
            },
        )

    assert credential_reads == 0


def test_reviewed_tushare_origin_rebinds_http_sdk_default_and_seals_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "private-token-must-not-leak"
    credential_reads = 0

    def resolve_once(**_kwargs):
        nonlocal credential_reads
        credential_reads += 1
        return marker

    monkeypatch.setattr(data_sync_module, "resolve_credential", resolve_once)
    adapter = source_adapter_from_mapping(
        _tushare_spec(),
        env={
            "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
                [
                    {
                        "name": "primary-tushare",
                        "source_type": "tushare",
                        "enabled": True,
                        "credential_ref": "secret://tushare_token",
                    }
                ]
            )
        },
    )

    assert isinstance(adapter, TushareSourceAdapter)
    assert credential_reads == 1
    assert adapter.base_url == "https://api.tushare.pro/dataapi"
    assert adapter.client._DataApi__http_url == adapter.base_url
    assert tushare_client_uses_direct_transport(adapter.client)
    session = getattr(adapter.client, "_factor_lab_direct_http_session")
    assert session.trust_env is False
    assert session.proxies == {}
    assert session.verify is True
    assert marker not in repr(adapter.lineage)


def test_credential_file_and_secret_traversal_rules(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("fixture-token\n", encoding="utf-8")
    assert (
        resolve_credential(
            credential_ref=None,
            env_name="TUSHARE_TOKEN",
            env={"TUSHARE_TOKEN_FILE": str(token_file)},
        )
        == "fixture-token"
    )
    with pytest.raises(ValueError, match="invalid secret"):
        resolve_credential(
            credential_ref="secret://../token",
            env_name="TUSHARE_TOKEN",
            env={"FACTOR_LAB_SECRETS_DIR": str(tmp_path)},
        )


def test_local_source_sync_writes_immutable_bronze_and_lineage(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    pd.DataFrame(
        {
            "ticker": ["000001.SZ"],
            "date": ["2026-08-21"],
            "close": [10.0],
        }
    ).to_parquet(source / "daily.parquet", index=False)
    spec = {
        "source": "local_file",
        "root": str(source),
        "priority": 30,
        "path_templates": {"daily": "daily.parquet"},
        "contract": {
            "dataset": "daily",
            "key_fields": ["ticker", "date"],
            "event_time_field": "date",
            "release_timing": "after_close",
            "fields": [
                {"name": "ticker", "dtype": "string", "nullable": False},
                {"name": "date", "dtype": "date", "nullable": False},
                {"name": "close", "dtype": "float64", "adjustment": "raw"},
            ],
        },
        "request": {"dataset": "daily"},
    }
    result = sync_bronze(spec, lake_root=tmp_path / "lake")
    assert result.rows == 1
    assert Path(result.data_path).is_file()
    assert Path(result.metadata_path).is_file()
    assert "bronze" in Path(result.data_path).parts


@pytest.mark.parametrize("failure_type", [ValueError, RuntimeError])
def test_optional_bronze_probe_raised_failures_are_not_provider_degradation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
) -> None:
    marker = "injected-local-programming-failure"

    def fail_probe():
        raise failure_type(marker)

    adapter = SimpleNamespace(source_id="optional-fixture", probe=fail_probe)
    monkeypatch.setattr(
        data_sync_module,
        "source_adapter_from_mapping",
        lambda *_args, **_kwargs: adapter,
    )

    with pytest.raises(failure_type, match=marker):
        sync_bronze(
            _local_spec(tmp_path),
            lake_root=tmp_path / "lake",
            classify_observation_failures=True,
        )


def test_optional_bronze_explicit_unhealthy_probe_is_sanitized_degradation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "provider-message-containing-private-token"
    adapter = SimpleNamespace(
        source_id="optional-fixture",
        probe=lambda: SimpleNamespace(
            health=SourceHealth.UNAVAILABLE,
            message=marker,
        ),
    )
    monkeypatch.setattr(
        data_sync_module,
        "source_adapter_from_mapping",
        lambda *_args, **_kwargs: adapter,
    )

    with pytest.raises(BronzeObservationError) as caught:
        sync_bronze(
            _local_spec(tmp_path),
            lake_root=tmp_path / "lake",
            classify_observation_failures=True,
        )

    assert caught.value.failure_type == "probe_unavailable"
    assert caught.value.__cause__ is None
    assert marker not in str(caught.value)


@pytest.mark.parametrize("failure_type", [ValueError, RuntimeError])
def test_optional_bronze_fetch_programming_failures_remain_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
) -> None:
    marker = "injected-fetch-programming-failure"

    def fail_fetch(_request):
        raise failure_type(marker)

    adapter = SimpleNamespace(
        source_id="optional-fixture",
        probe=lambda: SimpleNamespace(
            health=SourceHealth.HEALTHY,
            message="healthy",
        ),
        fetch=fail_fetch,
    )
    monkeypatch.setattr(
        data_sync_module,
        "source_adapter_from_mapping",
        lambda *_args, **_kwargs: adapter,
    )

    with pytest.raises(failure_type, match=marker):
        sync_bronze(
            _local_spec(tmp_path),
            lake_root=tmp_path / "lake",
            classify_observation_failures=True,
        )


def test_optional_bronze_typed_provider_failure_drops_sensitive_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "provider-private-token-must-not-enter-traceback"

    def fail_fetch(_request):
        raise SourceObservationError(marker, failure_kind="provider_error")

    adapter = SimpleNamespace(
        source_id="optional-fixture",
        probe=lambda: SimpleNamespace(
            health=SourceHealth.HEALTHY,
            message="healthy",
        ),
        fetch=fail_fetch,
    )
    monkeypatch.setattr(
        data_sync_module,
        "source_adapter_from_mapping",
        lambda *_args, **_kwargs: adapter,
    )

    with pytest.raises(BronzeObservationError) as caught:
        sync_bronze(
            _local_spec(tmp_path),
            lake_root=tmp_path / "lake",
            classify_observation_failures=True,
        )

    rendered = "".join(
        traceback.format_exception(caught.type, caught.value, caught.tb)
    )
    assert caught.value.failure_type == "provider_error"
    assert caught.value.__cause__ is None
    assert marker not in rendered


def test_bronze_sync_rejects_nested_credential_request_before_adapter_or_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = "FAKE-CREDENTIAL-MARKER-MUST-NOT-LEAK"
    spec = _local_spec(tmp_path)
    spec["request"]["parameters"] = {
        # Treat both keys and values as untrusted: neither may be reflected by
        # the fail-closed error path.
        "headers": [{f"api-key-{marker}": marker}],
    }
    adapter_calls = 0

    def forbidden_adapter(*_args, **_kwargs):
        nonlocal adapter_calls
        adapter_calls += 1
        raise AssertionError("unsafe request reached adapter construction")

    monkeypatch.setattr(
        data_sync_module,
        "source_adapter_from_mapping",
        forbidden_adapter,
    )
    lake = tmp_path / "lake"

    with pytest.raises(SourceContractError) as caught:
        sync_bronze(spec, lake_root=lake)

    assert adapter_calls == 0
    assert marker not in str(caught.value)
    assert not lake.exists()


def test_source_adapter_fetch_independently_rejects_credential_request_without_value_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = "FAKE-CREDENTIAL-MARKER-MUST-NOT-LEAK"
    adapter = source_adapter_from_mapping(_local_spec(tmp_path), env={})
    fetch_calls = 0

    def forbidden_fetch(_request):
        nonlocal fetch_calls
        fetch_calls += 1
        raise AssertionError("unsafe request reached provider")

    monkeypatch.setattr(adapter, "_fetch_frame", forbidden_fetch)

    with pytest.raises(SourceContractError) as caught:
        adapter.fetch(
            FetchRequest(
                dataset="daily",
                parameters={"nested": {"accessToken": marker}},
            )
        )

    assert fetch_calls == 0
    assert marker not in str(caught.value)


def test_bronze_sync_archives_parquet_and_lineage_to_object_store(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    pd.DataFrame(
        {"trade_date": ["2026-08-21"], "ticker": ["000001.SZ"], "close": [10.0]}
    ).to_parquet(source / "daily.parquet", index=False)
    spec = {
        "source": "local_file",
        "root": str(source),
        "path_templates": {"daily": "daily.parquet"},
        "contract": {
            "dataset": "daily",
            "key_fields": ["trade_date", "ticker"],
            "event_time_field": "trade_date",
            "release_timing": "same_session_close",
            "fields": [
                {"name": "trade_date", "dtype": "date", "nullable": False},
                {"name": "ticker", "dtype": "string", "nullable": False},
                {"name": "close", "dtype": "float", "nullable": False},
            ],
        },
        "request": {"dataset": "daily"},
    }
    filesystem = _MemoryFileSystem()
    result = sync_bronze(
        spec,
        lake_root=tmp_path / "lake",
        object_store_archive=S3ImmutableArchive(
            bucket="factor-lab", filesystem=filesystem
        ),
    )

    assert result.data_object_uri.startswith("s3://factor-lab/research-os/bronze/")
    assert result.metadata_object_uri.startswith("s3://factor-lab/research-os/bronze/")
    assert len(filesystem.objects) == 2
    # An identical retry reuses both the local immutable cache and S3 objects.
    retried = sync_bronze(
        spec,
        lake_root=tmp_path / "lake",
        object_store_archive=S3ImmutableArchive(
            bucket="factor-lab", filesystem=filesystem
        ),
    )
    assert retried.data_object_uri == result.data_object_uri
