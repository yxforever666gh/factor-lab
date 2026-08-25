from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import pytest
import requests

import factor_lab.research_os.reconciliation as reconciliation_module

from factor_lab.expanded_market_data import (
    HistoricalSTSnapshot,
    apply_historical_st_filter,
    audit_raw_partition,
    fetch_historical_st_history,
    filter_verified_raw_checkpoint,
    normalize_historical_st_snapshot,
)
from factor_lab.expanded_research_runner import cache_reference_data
from factor_lab.research_os.bitemporal import (
    BitemporalValidationError,
    CanonicalizationSpec,
    canonicalize_batch,
    point_in_time_view,
)
from factor_lab.research_os.data_quality import (
    DataQualityError,
    DataQualityGate,
    QualityReport,
    is_probable_mojibake,
    sha256_path,
)
from factor_lab.research_os.data_sources import (
    AkShareSourceAdapter,
    DatasetContract,
    DiemengSourceAdapter,
    FetchRequest,
    FieldContract,
    LocalFileSourceAdapter,
    RateLimit,
    SourceAdapter,
    SourceBatch,
    SourceContractError,
    SourceHealth,
    SourceObservationError,
    TokenBucketRateLimiter,
    TushareSourceAdapter,
    normalize_diemeng_base_url,
    source_adapter_public_contract_identity,
    tushare_client_uses_direct_transport,
    validate_tushare_https_origin,
)
from factor_lab.research_os.reconciliation import (
    ComparisonPolicy,
    production_comparison_policies,
    reconcile_observations,
)
from factor_lab.research_os.snapshots import (
    SnapshotIntegrityError,
    build_immutable_snapshot_manifest,
    publish_snapshot_manifest,
    verify_immutable_snapshot_manifest,
)


def _daily_contract() -> DatasetContract:
    return DatasetContract(
        dataset="daily",
        key_fields=("ts_code", "trade_date"),
        event_time_field="trade_date",
        release_timing="exchange close plus vendor publication latency",
        fields=(
            FieldContract("ts_code", "string", nullable=False),
            FieldContract("trade_date", "date", nullable=False),
            FieldContract("available_at", "datetime", nullable=False),
            FieldContract("close", "float64", nullable=False, unit="CNY", adjustment="raw"),
        ),
    )


def _daily_frame(close: float = 10.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["2024-01-02"],
            "available_at": ["2024-01-02T16:00:00Z"],
            "close": [close],
        }
    )


class _TushareFixture:
    def query(self, endpoint: str, **kwargs):
        if endpoint == "trade_cal":
            return pd.DataFrame({"exchange": ["SSE"], "cal_date": ["20240102"], "is_open": [1]})
        assert endpoint == "daily"
        return _daily_frame()


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        (
            "https://api.tushare.pro/dataapi",
            "https://api.tushare.pro/dataapi",
        ),
        (
            "https://api.waditu.com:443/dataapi/",
            "https://api.waditu.com/dataapi",
        ),
        (
            "HTTPS://API.TUSHARE.PRO:443/dataapi/",
            "https://api.tushare.pro/dataapi",
        ),
    ],
)
def test_reviewed_tushare_https_origin_accepts_only_exact_dataapi_routes(
    configured: str,
    expected: str,
) -> None:
    assert validate_tushare_https_origin(configured) == expected


@pytest.mark.parametrize(
    "configured",
    [
        "",
        "http://api.tushare.pro/dataapi",
        "https://attacker.invalid/dataapi",
        "https://user:password@api.tushare.pro/dataapi",
        "https://api.tushare.pro:444/dataapi",
        "https://api.tushare.pro/other",
        "https://api.tushare.pro/dataapi?",
        "https://api.tushare.pro/dataapi?route=other",
        "https://api.tushare.pro/dataapi#fragment",
    ],
)
def test_reviewed_tushare_https_origin_rejects_unsafe_routes(
    configured: str,
) -> None:
    with pytest.raises(SourceContractError, match="HTTPS origin"):
        validate_tushare_https_origin(configured)


class _AkShareFixture:
    def stock_zh_a_spot_em(self):
        return pd.DataFrame({"code": ["000001"]})

    def daily_fixture(self, **kwargs):
        return _daily_frame(10.01)


class _DiemengResponse:
    status_code = 200

    def json(self):
        return {
            "code": 200,
            "msg": "ok",
            "data": {"items": _daily_frame(10.02).to_dict("records")},
        }


class _DiemengSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        return _DiemengResponse()


class _VirtualClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_provider_rate_limiter_enforces_shared_burst_and_refill() -> None:
    clock = _VirtualClock()
    limiter = TokenBucketRateLimiter(
        monotonic_clock=clock,
        sleeper=clock.sleep,
    )
    limit = RateLimit(requests=2, per_seconds=1.0, burst=2)
    first = TushareSourceAdapter(
        _TushareFixture(),
        contracts=[_daily_contract()],
        rate_limits={"daily": limit},
        rate_limiter=limiter,
    )
    second = TushareSourceAdapter(
        _TushareFixture(),
        contracts=[_daily_contract()],
        rate_limits={"daily": limit},
        rate_limiter=limiter,
    )

    first.fetch(FetchRequest("daily"))
    second.fetch(FetchRequest("daily"))
    first.fetch(FetchRequest("daily"))

    assert clock.waits == [pytest.approx(0.5)]
    clock.advance(1.0)
    second.fetch(FetchRequest("daily"))
    first.fetch(FetchRequest("daily"))
    assert clock.waits == [pytest.approx(0.5)]


def test_provider_rate_limits_are_per_dataset_and_probe_endpoint() -> None:
    clock = _VirtualClock()
    limiter = TokenBucketRateLimiter(monotonic_clock=clock, sleeper=clock.sleep)
    daily_basic = replace(_daily_contract(), dataset="daily_basic")
    limit = RateLimit(requests=1, per_seconds=2.0, burst=1)

    class MultiEndpointFixture(_TushareFixture):
        def query(self, endpoint: str, **kwargs):
            if endpoint == "trade_cal":
                return super().query(endpoint, **kwargs)
            assert endpoint in {"daily", "daily_basic"}
            return _daily_frame()

    adapter = TushareSourceAdapter(
        MultiEndpointFixture(),
        contracts=[_daily_contract(), daily_basic],
        rate_limits={"daily": limit, "daily_basic": limit, "trade_cal": limit},
        rate_limiter=limiter,
    )

    adapter.fetch(FetchRequest("daily"))
    adapter.fetch(FetchRequest("daily_basic"))
    assert clock.waits == []
    adapter.fetch(FetchRequest("daily"))
    assert clock.waits == [pytest.approx(2.0)]
    adapter.probe()
    adapter.probe()
    assert clock.waits[-1] == pytest.approx(2.0)


def test_provider_without_rate_limit_never_sleeps() -> None:
    def forbidden_sleep(_seconds: float) -> None:
        raise AssertionError("unconfigured provider must not wait")

    limiter = TokenBucketRateLimiter(
        monotonic_clock=lambda: 0.0,
        sleeper=forbidden_sleep,
    )
    adapter = TushareSourceAdapter(
        _TushareFixture(),
        contracts=[_daily_contract()],
        rate_limiter=limiter,
    )

    assert adapter.fetch(FetchRequest("daily")).frame.iloc[0]["close"] == 10.0
    assert adapter.probe().health is SourceHealth.HEALTHY


def test_source_adapters_probe_real_endpoints_and_enforce_contracts(tmp_path: Path):
    contract = _daily_contract()
    tushare = TushareSourceAdapter(_TushareFixture(), contracts=[contract])
    assert tushare.probe().health is SourceHealth.HEALTHY
    batch = tushare.fetch(FetchRequest("daily"))
    assert batch.source_id == "tushare"
    assert batch.frame.iloc[0]["close"] == 10.0

    akshare = AkShareSourceAdapter(
        _AkShareFixture(),
        contracts=[contract],
        endpoint_map={"daily": "daily_fixture"},
    )
    assert akshare.probe().health is SourceHealth.HEALTHY
    assert akshare.fetch(FetchRequest("daily")).frame.iloc[0]["close"] == 10.01

    path = tmp_path / "daily.parquet"
    _daily_frame().to_parquet(path, index=False)
    local = LocalFileSourceAdapter(
        tmp_path,
        contracts=[contract],
        path_templates={"daily": "daily.parquet"},
    )
    assert local.probe().health is SourceHealth.HEALTHY
    assert len(local.fetch(FetchRequest("daily")).frame) == 1
    escaping = LocalFileSourceAdapter(
        tmp_path,
        contracts=[contract],
        path_templates={"daily": "../outside.parquet"},
    )
    with pytest.raises(ValueError, match="escapes"):
        escaping.fetch(FetchRequest("daily"))


def test_local_missing_file_is_a_typed_observation_failure(tmp_path: Path) -> None:
    adapter = LocalFileSourceAdapter(
        tmp_path,
        contracts=[_daily_contract()],
        path_templates={"daily": "missing.parquet"},
    )

    probe = adapter.probe()
    assert probe.health is SourceHealth.UNAVAILABLE
    with pytest.raises(SourceObservationError) as caught:
        adapter.fetch(FetchRequest("daily"))

    assert caught.value.failure_kind == "source_missing"
    assert "missing.parquet" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_akshare_raw_provider_failure_is_sanitized_at_call_boundary() -> None:
    marker = "akshare-provider-secret-must-not-escape"

    class LeakingProvider:
        @staticmethod
        def bounded_history(**_kwargs):
            raise RuntimeError(marker)

    adapter = AkShareSourceAdapter(
        LeakingProvider(),
        contracts=[_daily_contract()],
        endpoint_map={"daily": "bounded_history"},
        probe_endpoint="bounded_history",
    )

    probe = adapter.probe()
    assert probe.health is SourceHealth.UNAVAILABLE
    assert probe.message == "AkShare probe failed (provider_error)"
    assert marker not in json.dumps(asdict(probe), default=str, sort_keys=True)
    with pytest.raises(SourceObservationError) as caught:
        adapter.fetch(FetchRequest("daily"))

    assert caught.value.failure_kind == "provider_error"
    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_akshare_programming_error_after_provider_call_remains_fail_closed() -> None:
    class ExplodingFrame(pd.DataFrame):
        def copy(self, *args, **kwargs):
            raise AssertionError("post-provider programming failure")

    class Provider:
        @staticmethod
        def bounded_history(**_kwargs):
            return ExplodingFrame(_daily_frame())

    adapter = AkShareSourceAdapter(
        Provider(),
        contracts=[_daily_contract()],
        endpoint_map={"daily": "bounded_history"},
        probe_endpoint="bounded_history",
    )

    with pytest.raises(AssertionError, match="post-provider programming failure"):
        adapter.fetch(FetchRequest("daily"))


def test_provider_return_contract_violation_is_a_typed_observation() -> None:
    class IncompleteProvider:
        @staticmethod
        def bounded_history(**_kwargs):
            return _daily_frame().drop(columns=["close"])

    adapter = AkShareSourceAdapter(
        IncompleteProvider(),
        contracts=[_daily_contract()],
        endpoint_map={"daily": "bounded_history"},
        probe_endpoint="bounded_history",
    )

    with pytest.raises(SourceObservationError) as caught:
        adapter.fetch(FetchRequest("daily"))
    assert caught.value.failure_kind == "response_contract"
    assert "missing contracted fields" in str(caught.value)


def test_akshare_missing_endpoint_and_rate_limit_fault_are_not_observations() -> None:
    class Provider:
        pass

    adapter = AkShareSourceAdapter(
        Provider(),
        contracts=[_daily_contract()],
        endpoint_map={"daily": "not_configured"},
        probe_endpoint="not_configured",
    )
    with pytest.raises(TypeError, match="no callable endpoint") as endpoint_error:
        adapter.fetch(FetchRequest("daily"))
    assert not isinstance(endpoint_error.value, SourceObservationError)

    class ContractFaultProvider:
        @staticmethod
        def bounded_history(**_kwargs):
            raise SourceContractError("reviewed adapter configuration is invalid")

    contract_fault = AkShareSourceAdapter(
        ContractFaultProvider(),
        contracts=[_daily_contract()],
        endpoint_map={"daily": "bounded_history"},
        probe_endpoint="bounded_history",
    )
    with pytest.raises(
        SourceContractError, match="reviewed adapter configuration is invalid"
    ) as contract_error:
        contract_fault.fetch(FetchRequest("daily"))
    assert not isinstance(contract_error.value, SourceObservationError)
    with pytest.raises(
        SourceContractError, match="reviewed adapter configuration is invalid"
    ) as probe_contract_error:
        contract_fault.probe()
    assert not isinstance(probe_contract_error.value, SourceObservationError)

    class BrokenLimiter:
        @staticmethod
        def acquire(*_args, **_kwargs):
            raise ValueError("conflicting reviewed rate limit")

    rate_limited = AkShareSourceAdapter(
        Provider(),
        contracts=[_daily_contract()],
        endpoint_map={"daily": "not_configured"},
        probe_endpoint="not_configured",
        rate_limits={"not_configured": RateLimit(1, 60.0)},
        rate_limiter=BrokenLimiter(),
    )
    with pytest.raises(ValueError, match="conflicting reviewed rate limit") as rate_error:
        rate_limited.probe()
    assert not isinstance(rate_error.value, SourceObservationError)


def test_diemeng_raw_transport_failure_is_sanitized_observation() -> None:
    marker = "diemeng-transport-secret-must-not-escape"

    class LeakingSession:
        @staticmethod
        def request(_method, _url, **_kwargs):
            raise RuntimeError(marker)

    adapter = DiemengSourceAdapter(
        base_url="https://data.diemeng.chat/api",
        api_key=marker,
        contracts=[_daily_contract()],
        endpoint_map={"daily": "/stock/history"},
        session=LeakingSession(),
        max_attempts=1,
    )

    probe = adapter.probe()
    assert probe.health is SourceHealth.UNAVAILABLE
    assert probe.message == "Diemeng probe failed (transport_error)"
    assert marker not in json.dumps(asdict(probe), default=str, sort_keys=True)
    with pytest.raises(SourceObservationError) as caught:
        adapter.fetch(FetchRequest("daily"))

    assert caught.value.failure_kind == "transport_error"
    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_diemeng_adapter_uses_verified_contract_without_leaking_key() -> None:
    session = _DiemengSession()
    adapter = DiemengSourceAdapter(
        base_url="https://diemeng.chat/api",
        api_key="fixture-secret",
        contracts=[_daily_contract()],
        endpoint_map={"daily": "/stock/history"},
        method_map={"daily": "POST"},
        response_paths={"daily": "data.items"},
        session=session,
        max_attempts=1,
    )

    assert adapter.probe().health is SourceHealth.HEALTHY
    batch = adapter.fetch(FetchRequest("daily", parameters={"stock_code": "000001.SZ"}))
    assert batch.source_id == "diemeng"
    assert batch.frame.iloc[0]["close"] == pytest.approx(10.02)
    assert normalize_diemeng_base_url("https://mg.diemeng.chat") == (
        "https://data.diemeng.chat/api"
    )
    assert session.calls[-1][0] == "POST"
    assert session.calls[-1][1] == "https://data.diemeng.chat/api/stock/history"
    assert session.calls[-1][2]["headers"] == {"apiKey": "fixture-secret"}
    assert session.trust_env is False
    assert "fixture-secret" not in repr(batch.lineage)


def test_diemeng_requests_session_ignores_ambient_proxy_and_ca(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "https://ambient-proxy.invalid")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/ambient/unreviewed-ca.pem")
    session = requests.Session()
    assert session.trust_env is True

    adapter = DiemengSourceAdapter(
        base_url="https://data.diemeng.chat/api",
        api_key="fixture-secret",
        contracts=[_daily_contract()],
        endpoint_map={"daily": "/stock/history"},
        session=session,
        max_attempts=1,
    )

    assert adapter.session is session
    assert session.trust_env is False
    assert session.proxies == {}
    assert session.verify is True
    assert session.cert is None


def test_diemeng_rejects_explicit_unreviewed_requests_transport() -> None:
    session = requests.Session()
    session.proxies["https"] = "https://unreviewed-proxy.invalid"

    with pytest.raises(ValueError, match="unreviewed transport state"):
        DiemengSourceAdapter(
            base_url="https://data.diemeng.chat/api",
            api_key="fixture-secret",
            contracts=[_daily_contract()],
            endpoint_map={"daily": "/stock/history"},
            session=session,
            max_attempts=1,
        )


def test_current_http_tushare_sdk_is_rejected_without_constructing_session() -> None:
    import tushare as ts

    client = ts.pro_api("private-token-must-not-leak")

    with pytest.raises(SourceContractError, match="HTTPS transport") as caught:
        TushareSourceAdapter(client, contracts=[_daily_contract()])

    assert "private-token-must-not-leak" not in str(caught.value)
    assert "_factor_lab_direct_http_session" not in vars(client)


def test_tushare_transport_seal_fault_is_not_an_observation() -> None:
    import tushare as ts

    client = ts.pro_api("private-token-must-not-leak")
    client._DataApi__http_url = "https://api.waditu.com/dataapi"
    adapter = TushareSourceAdapter(client, contracts=[_daily_contract()])
    delattr(client, "_factor_lab_direct_transport_seal")

    with pytest.raises(SourceContractError, match="transport is not sealed") as caught:
        adapter.fetch(FetchRequest("daily"))
    assert not isinstance(caught.value, SourceObservationError)
    assert "private-token-must-not-leak" not in str(caught.value)


@pytest.mark.parametrize(
    "mutated_origin",
    [
        "https://api.tushare.pro/dataapi",
        "https://api.waditu.com:443/dataapi",
        "https://attacker.invalid/dataapi",
    ],
)
def test_tushare_sealed_origin_rejects_any_sdk_url_mutation(
    mutated_origin: str,
) -> None:
    import tushare as ts

    client = ts.pro_api("private-token-must-not-leak")
    client._DataApi__http_url = "https://api.waditu.com/dataapi"
    adapter = TushareSourceAdapter(client, contracts=[_daily_contract()])
    sealed_identity = adapter.public_contract_identity()
    client._DataApi__http_url = mutated_origin

    assert tushare_client_uses_direct_transport(client) is False
    with pytest.raises(SourceContractError, match="transport is not sealed") as caught:
        adapter.fetch(FetchRequest("daily"))

    assert adapter.public_contract_identity() == sealed_identity
    assert "private-token-must-not-leak" not in str(caught.value)


def test_https_tushare_sdk_uses_sealed_non_environment_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tushare as ts

    monkeypatch.setenv("HTTP_PROXY", "http://ambient-proxy.invalid")
    monkeypatch.setenv("CURL_CA_BUNDLE", "/ambient/unreviewed-ca.pem")
    client = ts.pro_api("private-token-must-not-leak")
    # Model a future vendor-confirmed SDK release.  Production never performs
    # this rewrite; its class-level preflight stays blocked while the installed
    # SDK declares HTTP.
    client._DataApi__http_url = "https://api.waditu.com/dataapi"
    adapter = TushareSourceAdapter(client, contracts=[_daily_contract()])
    session = getattr(client, "_factor_lab_direct_http_session")
    calls: list[tuple[str, dict]] = []

    class Response:
        status_code = 200

        @staticmethod
        def json():
            frame = _daily_frame()
            return {
                "code": 0,
                "data": {
                    "fields": list(frame.columns),
                    "items": frame.values.tolist(),
                },
            }

    def post(url: str, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr(session, "post", post)
    batch = adapter.fetch(FetchRequest("daily"))

    assert batch.frame.iloc[0]["close"] == pytest.approx(10.0)
    assert type(session) is requests.Session
    assert session.trust_env is False
    assert session.proxies == {}
    assert session.verify is True
    assert calls[0][0] == f"{client._DataApi__http_url.rstrip('/')}/daily"
    assert calls[0][1]["allow_redirects"] is False
    assert "private-token-must-not-leak" not in repr(batch.lineage)
    identity = adapter.public_contract_identity()
    assert identity["routing_contract"]["base_url"] == (
        "https://api.waditu.com/dataapi"
    )
    assert identity["routing_contract"]["transport_policy"] == {
        "schema_version": "research-os/tushare-sealed-https/v1",
        "https_only": True,
        "redirects_allowed": False,
        "trust_environment": False,
    }

    alternate = ts.pro_api("another-private-token")
    alternate._DataApi__http_url = "https://api.tushare.pro/dataapi"
    alternate_adapter = TushareSourceAdapter(
        alternate, contracts=[_daily_contract()]
    )
    assert alternate_adapter.public_contract_identity()["public_contract_hash"] != (
        identity["public_contract_hash"]
    )


def test_real_tushare_calls_only_the_sealed_query_with_expected_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tushare as ts

    client = ts.pro_api("private-token-must-not-leak")
    client._DataApi__http_url = "HTTPS://API.WADITU.COM:443/dataapi/"
    adapter = TushareSourceAdapter(client, contracts=[_daily_contract()])
    session = getattr(client, "_factor_lab_direct_http_session")
    dynamic_attribute_reads: list[str] = []

    def reject_sdk_attribute(_self, name: str):
        dynamic_attribute_reads.append(name)
        raise AssertionError("real adapter must not dispatch through SDK attributes")

    class Response:
        status_code = 200

        @staticmethod
        def json():
            frame = _daily_frame()
            return {
                "code": 0,
                "data": {
                    "fields": list(frame.columns),
                    "items": frame.values.tolist(),
                },
            }

    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(type(client), "__getattr__", reject_sdk_attribute)
    monkeypatch.setattr(
        session,
        "post",
        lambda url, **kwargs: (calls.append((url, kwargs)) or Response()),
    )

    result = adapter.fetch(
        FetchRequest(
            "daily",
            parameters={"ts_type_name": "https://attacker.invalid/dataapi"},
        )
    )

    assert result.frame.iloc[0]["close"] == pytest.approx(10.0)
    assert dynamic_attribute_reads == []
    assert client._DataApi__http_url == "https://api.waditu.com/dataapi"
    assert calls[0][0] == "https://api.waditu.com/dataapi/daily"
    assert calls[0][1]["json"]["api_name"] == "daily"
    assert calls[0][1]["json"]["params"]["ts_type_name"] == (
        "https://api.waditu.com/dataapi"
    )


def test_https_tushare_redirect_fails_closed_without_exposing_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tushare as ts

    client = ts.pro_api("private-token-must-not-leak")
    client._DataApi__http_url = "https://api.waditu.com/dataapi"
    adapter = TushareSourceAdapter(client, contracts=[_daily_contract()])
    session = getattr(client, "_factor_lab_direct_http_session")

    class RedirectResponse:
        status_code = 307

        @staticmethod
        def json():
            raise AssertionError("redirect body must not be parsed")

    monkeypatch.setattr(session, "post", lambda *_args, **_kwargs: RedirectResponse())
    with pytest.raises(SourceContractError, match="redirect 307 is forbidden") as caught:
        adapter.fetch(FetchRequest("daily"))

    assert "private-token-must-not-leak" not in str(caught.value)


def test_https_tushare_provider_message_never_crosses_public_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tushare as ts

    marker = "fake-tushare-api-key-must-never-escape"
    client = ts.pro_api(marker)
    client._DataApi__http_url = "https://api.waditu.com/dataapi"
    adapter = TushareSourceAdapter(client, contracts=[_daily_contract()])
    session = getattr(client, "_factor_lab_direct_http_session")

    class RejectedResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"code": 40203, "msg": marker, "data": None}

    monkeypatch.setattr(session, "post", lambda *_args, **_kwargs: RejectedResponse())

    with pytest.raises(
        SourceContractError,
        match=r"API rejected request \(code=40203\)",
    ) as caught:
        adapter.fetch(FetchRequest("daily"))
    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)
    assert marker not in json.dumps(vars(caught.value), default=str, sort_keys=True)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None

    probe = adapter.probe()
    assert probe.health is SourceHealth.UNAVAILABLE
    assert probe.message == "Tushare probe failed (api_code=40203)"
    assert marker not in json.dumps(asdict(probe), default=str, sort_keys=True)
    assert marker not in repr(probe)


def test_diemeng_redirect_fails_closed_without_cross_origin_follow() -> None:
    class RedirectResponse:
        status_code = 302
        headers = {"Location": "https://attacker.invalid/collect"}

        @staticmethod
        def json():
            raise AssertionError("redirect body must not be parsed")

    class RedirectSession:
        def __init__(self):
            self.calls = []

        def request(self, method, url, **kwargs):
            self.calls.append((method, url, kwargs))
            return RedirectResponse()

    session = RedirectSession()
    adapter = DiemengSourceAdapter(
        base_url="https://data.diemeng.chat/api",
        api_key="fixture-secret",
        contracts=[_daily_contract()],
        endpoint_map={"daily": "/stock/history"},
        session=session,
        max_attempts=1,
    )

    with pytest.raises(SourceContractError, match="redirect 302 is forbidden"):
        adapter.fetch(FetchRequest("daily", parameters={"trade_date": "20260821"}))

    assert len(session.calls) == 1
    method, url, kwargs = session.calls[0]
    assert method == "GET"
    assert url == "https://data.diemeng.chat/api/stock/history"
    assert kwargs["allow_redirects"] is False
    assert "attacker.invalid" not in url


def test_diemeng_provider_message_never_crosses_error_or_probe_boundary() -> None:
    marker = "fake-api-key-must-never-escape"

    class RejectedResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "code": 401,
                # Providers sometimes echo request material here. This value
                # intentionally equals the credential used by the adapter.
                "msg": marker,
                "data": None,
            }

    class RejectedSession:
        def request(self, _method, _url, **_kwargs):
            return RejectedResponse()

    adapter = DiemengSourceAdapter(
        base_url="https://data.diemeng.chat/api",
        api_key=marker,
        contracts=[_daily_contract()],
        endpoint_map={"daily": "/stock/history"},
        session=RejectedSession(),
        max_attempts=1,
    )

    with pytest.raises(
        SourceContractError,
        match=r"API rejected request \(code=401\)",
    ) as caught:
        adapter.fetch(FetchRequest("daily"))
    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert marker not in json.dumps(vars(caught.value), default=str, sort_keys=True)

    probe = adapter.probe()
    assert probe.health is SourceHealth.UNAVAILABLE
    assert probe.message == "Diemeng probe failed (api_code=401)"
    serialized = json.dumps(asdict(probe), default=str, sort_keys=True)
    assert marker not in serialized
    assert marker not in repr(probe)


def test_tushare_provider_errors_and_probe_never_echo_request_material() -> None:
    marker = "fake-tushare-token-must-never-escape"

    class LeakingFixture:
        def query(self, _endpoint: str, **_kwargs):
            raise RuntimeError(marker)

    adapter = TushareSourceAdapter(LeakingFixture(), contracts=[_daily_contract()])
    with pytest.raises(SourceContractError, match="provider request failed") as caught:
        adapter.fetch(FetchRequest("daily"))
    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None

    probe = adapter.probe()
    assert probe.health is SourceHealth.UNAVAILABLE
    assert probe.message == "Tushare probe failed (provider_error)"
    assert marker not in json.dumps(asdict(probe), default=str, sort_keys=True)
    assert marker not in repr(probe)


def test_akshare_explicit_field_mapping_keeps_raw_columns_and_lineage() -> None:
    class Fixture:
        def bounded_history(self, **kwargs):
            assert kwargs == {"symbol": "000001", "start_date": "20240102"}
            return pd.DataFrame(
                {"日期": ["2024-01-02"], "收盘": [10.1], "供应商备注": ["raw"]}
            )

    contract = DatasetContract(
        dataset="daily",
        key_fields=("ts_code", "trade_date"),
        event_time_field="trade_date",
        release_timing="after close",
        fields=(
            FieldContract("ts_code", "string", nullable=False),
            FieldContract("trade_date", "date", nullable=False),
            FieldContract("close", "float64", nullable=False, unit="CNY"),
        ),
    )
    adapter = AkShareSourceAdapter(
        Fixture(),
        contracts=[contract],
        endpoint_map={"daily": "bounded_history"},
        probe_endpoint="bounded_history",
        probe_parameters={"symbol": "000001", "start_date": "20240102"},
        column_mapping={"日期": "trade_date", "收盘": "close"},
        constant_fields={"ts_code": "000001.SZ"},
    )

    assert adapter.probe().health is SourceHealth.HEALTHY
    batch = adapter.fetch(
        FetchRequest(
            "daily",
            parameters={"symbol": "000001", "start_date": "20240102"},
            fields=("ts_code", "trade_date", "close"),
        )
    )
    assert {"日期", "收盘", "供应商备注", "ts_code", "trade_date", "close"} <= set(
        batch.frame.columns
    )
    assert batch.lineage["response_field_mapping"] == {
        "日期": "trade_date",
        "收盘": "close",
    }


def test_diemeng_public_contract_identity_is_complete_and_credential_free() -> None:
    def adapter(
        *,
        api_key: str,
        base_url: str,
        credential_ref: str,
        endpoint: str = "/stock/history",
        method: str = "POST",
        response_path: str = "data.items",
        column_mapping: dict[str, str] | None = None,
        constant_fields: dict[str, object] | None = None,
        probe_parameters: dict[str, object] | None = None,
    ) -> DiemengSourceAdapter:
        return DiemengSourceAdapter(
            base_url=base_url,
            api_key=api_key,
            contracts=[_daily_contract()],
            endpoint_map={"daily": endpoint},
            method_map={"daily": method},
            response_paths={"daily": response_path},
            column_mapping=column_mapping or {"vendor_close": "close"},
            constant_fields=constant_fields or {"exchange": "SSE"},
            probe_dataset="daily",
            probe_parameters=probe_parameters
            or {"level": "1min", "credential_ref": credential_ref},
            lineage={
                "profile_name": "secondary-diemeng",
                "profile_source_type": "diemeng",
                "credential_binding": credential_ref,
                "api_key": api_key,
                "client": _DiemengSession(),
            },
            session=_DiemengSession(),
            max_attempts=1,
        )

    first = adapter(
        api_key="first-private-key",
        base_url="https://private.example/api",
        credential_ref="secret://private/location-one",
    )
    rotated = adapter(
        api_key="second-private-key",
        base_url="https://private.example/api",
        credential_ref="secret://different/location-two",
    )

    identity = first.public_contract_identity()
    assert identity == source_adapter_public_contract_identity(first)
    assert identity == rotated.public_contract_identity()
    assert identity["source_id"] == "diemeng"
    assert identity["priority"] == 20
    assert identity["adapter_class"]["qualname"] == "DiemengSourceAdapter"
    assert identity["routing_contract"] == {
        "base_url": "https://private.example/api",
        "datasets": {
            "daily": {
                "endpoint": "/stock/history",
                "method": "POST",
                "response_path": "data.items",
            }
        },
        "response_field_mapping": {"vendor_close": "close"},
        "constant_fields": {"exchange": "SSE"},
        "probe": {
            "dataset": "daily",
            "parameters": {
                "credential_ref": {"redacted": "credential_material"},
                "level": "1min",
            },
        },
    }
    assert set(identity["profile_lineage"]) == {
        "profile_name",
        "profile_source_type",
        "public_hash",
    }
    assert len(identity["public_contract_hash"]) == 64
    serialized = repr(identity)
    for private_value in (
        "first-private-key",
        "second-private-key",
        "secret://private/location-one",
        "secret://different/location-two",
    ):
        assert private_value not in serialized

    with pytest.raises(ValueError, match="userinfo"):
        adapter(
            api_key="key",
            base_url="https://user:password@private.example/api",
            credential_ref="secret://x",
        )
    with pytest.raises(ValueError, match="lineage.*userinfo"):
        DiemengSourceAdapter(
            base_url="https://data.diemeng.chat/api",
            api_key="key",
            contracts=[_daily_contract()],
            endpoint_map={"daily": "/stock/history"},
            lineage={
                "provider": {
                    "documentation": "https://user:password@example.invalid/api"
                }
            },
        )

    variants = (
        adapter(
            api_key="key",
            base_url="https://private.example/api",
            credential_ref="secret://x",
            endpoint="/stock/other",
        ),
        adapter(
            api_key="key",
            base_url="https://private.example/api",
            credential_ref="secret://x",
            method="GET",
        ),
        adapter(
            api_key="key",
            base_url="https://private.example/api",
            credential_ref="secret://x",
            response_path="data.list",
        ),
        adapter(
            api_key="key",
            base_url="https://private.example/api",
            credential_ref="secret://x",
            column_mapping={"vendor_price": "close"},
        ),
        adapter(
            api_key="key",
            base_url="https://private.example/api",
            credential_ref="secret://x",
            constant_fields={"exchange": "SZSE"},
        ),
        adapter(
            api_key="key",
            base_url="https://private.example/api",
            credential_ref="secret://x",
            probe_parameters={"level": "5min"},
        ),
        adapter(
            api_key="key",
            base_url="https://mirror.example/api",
            credential_ref="secret://x",
        ),
    )
    assert all(
        item.public_contract_identity()["public_contract_hash"]
        != identity["public_contract_hash"]
        for item in variants
    )


def test_tushare_and_akshare_public_identity_captures_routing_and_parsing() -> None:
    contract = _daily_contract()
    tushare = TushareSourceAdapter(
        _TushareFixture(),
        contracts=[contract],
        endpoint_map={"daily": "daily_v2"},
        lineage={
            "profile_name": "primary-tushare",
            "profile_source_type": "tushare",
            "credential_binding": "secret://tushare/original/path",
        },
    )
    tushare_identity = tushare.public_contract_identity()
    assert tushare_identity["routing_contract"]["base_url"] is None
    assert tushare_identity["routing_contract"]["transport_policy"] == {
        "schema_version": "research-os/tushare-injected-fixture/v1",
        "formal_production_transport": False,
    }
    assert tushare_identity["routing_contract"]["datasets"]["daily"] == {
        "endpoint": "daily_v2"
    }
    assert tushare_identity["routing_contract"]["probe"] == {
        "endpoint": "trade_cal"
    }
    changed_tushare = TushareSourceAdapter(
        _TushareFixture(),
        contracts=[contract],
        endpoint_map={"daily": "pro_bar"},
        lineage={
            "profile_name": "primary-tushare",
            "profile_source_type": "tushare",
            "credential_binding": "secret://tushare/rotated/path",
        },
    )
    assert changed_tushare.public_contract_identity()["public_contract_hash"] != (
        tushare_identity["public_contract_hash"]
    )
    assert "secret://" not in repr(tushare_identity)

    akshare = AkShareSourceAdapter(
        _AkShareFixture(),
        contracts=[contract],
        endpoint_map={"daily": "stock_zh_a_hist"},
        probe_endpoint="stock_zh_a_hist",
        probe_parameters={"symbol": "000001", "period": "daily"},
        column_mapping={"日期": "trade_date", "收盘": "close"},
        constant_fields={"ts_code": "000001.SZ"},
    )
    akshare_identity = akshare.public_contract_identity()
    assert akshare_identity["routing_contract"] == {
        "datasets": {"daily": {"endpoint": "stock_zh_a_hist"}},
        "response_field_mapping": {"收盘": "close", "日期": "trade_date"},
        "constant_fields": {"ts_code": "000001.SZ"},
        "probe": {
            "endpoint": "stock_zh_a_hist",
            "parameters": {"period": "daily", "symbol": "000001"},
        },
    }
    changed_akshare = AkShareSourceAdapter(
        _AkShareFixture(),
        contracts=[contract],
        endpoint_map={"daily": "stock_zh_a_daily"},
        column_mapping={"日期": "trade_date", "收盘": "close"},
        constant_fields={"ts_code": "000001.SZ"},
    )
    assert changed_akshare.public_contract_identity()["public_contract_hash"] != (
        akshare_identity["public_contract_hash"]
    )


def test_custom_adapter_public_identity_has_deterministic_safe_fallback() -> None:
    class SecretClient:
        def __init__(self, secret: str) -> None:
            self.secret = secret

        def __repr__(self) -> str:
            return f"SecretClient(secret={self.secret!r})"

    class ControlledAdapter(SourceAdapter):
        def __init__(self, *, secret: str, credential_ref: str) -> None:
            super().__init__(
                source_id="controlled",
                priority=7,
                contracts=[_daily_contract()],
                lineage={
                    "profile_name": "controlled-canary",
                    "profile_source_type": "controlled",
                    "credential_binding": credential_ref,
                    "api_key": secret,
                },
            )
            self.client = SecretClient(secret)

        def probe(self):
            raise AssertionError("identity must not invoke probe")

        def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
            raise AssertionError("identity must not fetch")

    first = ControlledAdapter(
        secret="controlled-secret-one",
        credential_ref="secret://controlled/path-one",
    )
    second = ControlledAdapter(
        secret="controlled-secret-two",
        credential_ref="secret://controlled/path-two",
    )
    first_identity = first.public_contract_identity()
    assert first_identity == second.public_contract_identity()
    assert first_identity["routing_contract"] == {
        "datasets": {"daily": {"endpoint": "adapter_defined"}},
        "fallback": "declared_schema_and_adapter_class_only",
    }
    assert "controlled-secret" not in repr(first_identity)
    assert "secret://controlled" not in repr(first_identity)


def _batch(source: str, priority: int, close: float, ingested_at: str, revision: str) -> SourceBatch:
    contract = _daily_contract()
    return SourceBatch(
        source_id=source,
        source_priority=priority,
        dataset="daily",
        frame=_daily_frame(close),
        ingested_at=datetime.fromisoformat(ingested_at.replace("Z", "+00:00")),
        vendor_revision=revision,
        contract=contract,
        request=FetchRequest("daily"),
    )


def _canonical(batch: SourceBatch) -> pd.DataFrame:
    return canonicalize_batch(
        batch,
        CanonicalizationSpec(
            entity_columns=("ts_code",),
            event_time_column="trade_date",
            available_at_column="available_at",
            value_columns=("close",),
        ),
    )


def test_bitemporal_view_excludes_late_vendor_revision():
    original = _canonical(_batch("tushare", 10, 10.0, "2024-01-03T00:00:00Z", "r1"))
    corrected = _canonical(_batch("tushare", 10, 10.5, "2024-01-05T00:00:00Z", "r2"))
    observations = pd.concat([original, corrected], ignore_index=True)

    historical = point_in_time_view(
        observations,
        decision_cutoff="2024-01-04T00:00:00Z",
        system_cutoff="2024-01-04T00:00:00Z",
    )
    assert historical["value"].tolist() == [10.0]
    current = point_in_time_view(
        observations,
        decision_cutoff="2024-01-06T00:00:00Z",
        system_cutoff="2024-01-06T00:00:00Z",
    )
    assert current["value"].tolist() == [10.5]

    impossible = _batch("bad", 1, 10.0, "2024-01-02T12:00:00Z", "r1")
    with pytest.raises(BitemporalValidationError, match="temporal ordering"):
        _canonical(impossible)


def test_reconciliation_accepts_consensus_but_never_priority_overwrites_conflict():
    first = _canonical(_batch("tushare", 10, 10.0, "2024-01-03T00:00:00Z", "r1"))
    close = _canonical(_batch("akshare", 20, 10.0005, "2024-01-03T01:00:00Z", "r1"))
    accepted = reconcile_observations(
        pd.concat([first, close], ignore_index=True),
        policies={"close": ComparisonPolicy(absolute_tolerance=0.001)},
    )
    assert accepted.audit["status"] == "pass"
    assert accepted.accepted.iloc[0]["source_id"] == "tushare"
    assert accepted.accepted.iloc[0]["evidence_count"] == 2

    conflict = close.copy()
    conflict["value"] = 12.0
    disputed = reconcile_observations(
        pd.concat([first, conflict], ignore_index=True),
        policies={"close": ComparisonPolicy(absolute_tolerance=0.001)},
    )
    assert disputed.accepted.empty
    assert set(disputed.disputed["reconciliation_status"]) == {"disputed"}
    assert disputed.promotion_allowed is False

    missing = first.copy()
    missing["value"] = pd.NA
    quarantined = reconcile_observations(missing)
    assert quarantined.audit["quarantined_row_count"] == 1
    assert quarantined.quarantined.iloc[0]["quarantine_reason"] == "required_value_missing"


def test_reconciliation_singleton_fast_path_matches_forced_grouped_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    template = _canonical(
        _batch("tushare", 10, 10.0, "2024-01-03T00:00:00Z", "r1")
    ).iloc[0].to_dict()

    def row(
        entity_key: str,
        value: object,
        *,
        source_id: str = "tushare",
        source_priority: int = 10,
        ingested_at: str = "2024-01-03T00:00:00Z",
        vendor_revision: str = "r1",
        unit: object = "CNY",
        adjustment: object = "raw",
        nullable: bool = False,
    ) -> dict[str, object]:
        observation = dict(template)
        observation.update(
            entity_key=entity_key,
            value=value,
            source_id=source_id,
            source_priority=source_priority,
            ingested_at=ingested_at,
            vendor_revision=vendor_revision,
            unit=unit,
            adjustment=adjustment,
            nullable=nullable,
        )
        return observation

    observations = pd.DataFrame(
        [
            row("singleton-finite", 10.0),
            row("singleton-infinite", np.inf),
            row("singleton-negative-infinite", -np.inf),
            row("superseded", 9.0, vendor_revision="r1"),
            row(
                "superseded",
                10.0,
                ingested_at="2024-01-04T00:00:00Z",
                vendor_revision="r2",
            ),
            row("consensus", 10.0),
            row(
                "consensus",
                10.0005,
                source_id="akshare",
                source_priority=20,
            ),
            row("conflict", 10.0),
            row("conflict", 12.0, source_id="akshare", source_priority=20),
            row("unit-mismatch", 10.0),
            row(
                "unit-mismatch",
                10.0,
                source_id="akshare",
                source_priority=20,
                unit="USD",
            ),
            row("nullable-mismatch", 10.0),
            row(
                "nullable-mismatch",
                10.0,
                source_id="akshare",
                source_priority=20,
                nullable=True,
            ),
            row("optional-missing", pd.NA, nullable=True),
            row("nullable-retained", 11.0, nullable=True),
            row(
                "nullable-retained",
                pd.NA,
                nullable=True,
                ingested_at="2024-01-04T00:00:00Z",
                vendor_revision="r2",
            ),
            row("required-missing", pd.NA),
        ]
    )
    observations["provider_payload_id"] = [f"payload-{index}" for index in observations.index]
    policies = {"close": ComparisonPolicy(absolute_tolerance=0.001)}

    grouped_entities: list[str] = []
    original_group_digest = reconciliation_module._canonical_group_digest

    def recording_group_digest(group: pd.DataFrame) -> str:
        grouped_entities.append(str(group.iloc[0]["entity_key"]))
        return original_group_digest(group)

    monkeypatch.setattr(
        reconciliation_module,
        "_canonical_group_digest",
        recording_group_digest,
    )
    optimized = reconcile_observations(observations, policies=policies)
    assert set(grouped_entities) == {
        "superseded",
        "consensus",
        "conflict",
        "unit-mismatch",
        "nullable-mismatch",
    }

    monkeypatch.setattr(
        reconciliation_module,
        "_single_source_unique_group_mask",
        lambda frame: pd.Series(False, index=frame.index, dtype=bool),
    )
    reference = reconcile_observations(observations, policies=policies)

    pd.testing.assert_frame_equal(
        optimized.accepted, reference.accepted, check_exact=True
    )
    pd.testing.assert_frame_equal(
        optimized.disputed, reference.disputed, check_exact=True
    )
    pd.testing.assert_frame_equal(
        optimized.quarantined, reference.quarantined, check_exact=True
    )
    assert optimized.audit == reference.audit
    assert optimized.audit["superseded_row_count"] == 1
    assert optimized.audit["nullable_missing_row_count"] == 2
    assert optimized.accepted.set_index("entity_key").loc[
        "nullable-retained", "value"
    ] == pytest.approx(11.0)
    assert optimized.disputed.set_index("entity_key").loc[
        "singleton-infinite", "dispute_reason"
    ] == "value_conflict"
    assert optimized.disputed.set_index("entity_key").loc[
        "singleton-negative-infinite", "dispute_reason"
    ] == "value_conflict"


def test_reconciliation_singleton_fast_path_scales_without_per_group_pandas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row_count = 20_000
    event_time = pd.Timestamp("2024-01-02T00:00:00Z")
    available_at = pd.Timestamp("2024-01-02T16:00:00Z")
    ingested_at = pd.Timestamp("2024-01-03T00:00:00Z")
    observations = pd.DataFrame(
        {
            "dataset": "daily",
            "entity_key": [f"ticker-{index:05d}" for index in range(row_count)],
            "field": "close",
            "value": np.arange(row_count, dtype=float) / 100.0 + 1.0,
            "event_time": event_time,
            "available_at": available_at,
            "ingested_at": ingested_at,
            "vendor_revision": "r1",
            "source_id": "tushare",
            "source_priority": 10,
            "nullable": False,
            "unit": "CNY",
            "adjustment": "raw",
            "lineage": None,
        }
    )

    def unexpected_group_digest(group: pd.DataFrame) -> str:
        raise AssertionError(
            f"singleton group unexpectedly used pandas group path: {group.iloc[0]['entity_key']}"
        )

    monkeypatch.setattr(
        reconciliation_module,
        "_canonical_group_digest",
        unexpected_group_digest,
    )
    started_at = perf_counter()
    result = reconcile_observations(observations)
    elapsed = perf_counter() - started_at

    assert result.audit == {
        "schema_version": "research-os/data-reconciliation/v1",
        "input_row_count": row_count,
        "latest_source_row_count": row_count,
        "superseded_row_count": 0,
        "accepted_group_count": row_count,
        "disputed_group_count": 0,
        "quarantined_row_count": 0,
        "nullable_missing_row_count": 0,
        "status": "pass",
    }
    assert len(result.accepted) == row_count
    assert result.disputed.empty
    assert elapsed < 10.0


def test_production_reconciliation_thresholds_are_field_specific() -> None:
    policies = production_comparison_policies()
    assert policies["close"].absolute_tolerance == pytest.approx(0.01)
    assert policies["amount"].relative_tolerance == pytest.approx(0.01)
    assert policies["trade_status"].case_sensitive is False
    assert policies["adj_factor"].absolute_tolerance == pytest.approx(1e-8)


def test_nullable_factor_missing_is_coverage_only_but_core_missing_blocks() -> None:
    contract = DatasetContract(
        dataset="daily_basic",
        key_fields=("ts_code", "trade_date"),
        event_time_field="trade_date",
        release_timing="after close",
        fields=(
            FieldContract("ts_code", "string", nullable=False),
            FieldContract("trade_date", "date", nullable=False),
            FieldContract("available_at", "datetime", nullable=False),
            FieldContract("close", "float64", nullable=False),
            FieldContract("pe", "float64", nullable=True),
        ),
    )
    batch = SourceBatch(
        source_id="tushare",
        source_priority=10,
        dataset="daily_basic",
        frame=pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["2024-01-02"],
                "available_at": ["2024-01-02T16:00:00Z"],
                "close": [10.0],
                "pe": [pd.NA],
            }
        ),
        ingested_at=datetime(2024, 1, 3, tzinfo=timezone.utc),
        vendor_revision="r1",
        contract=contract,
        request=FetchRequest("daily_basic"),
    )
    canonical = canonicalize_batch(
        batch,
        CanonicalizationSpec(
            entity_columns=("ts_code",),
            event_time_column="trade_date",
            available_at_column="available_at",
            value_columns=("close", "pe"),
        ),
    )
    assert bool(canonical.set_index("field").loc["pe", "nullable"]) is True

    optional_missing = reconcile_observations(canonical)
    assert optional_missing.promotion_allowed is True
    assert optional_missing.audit["status"] == "pass"
    assert optional_missing.audit["nullable_missing_row_count"] == 1
    assert optional_missing.audit["quarantined_row_count"] == 0
    assert optional_missing.accepted["field"].tolist() == ["close"]

    core_missing = canonical.copy()
    core_missing.loc[core_missing["field"] == "close", "value"] = pd.NA
    blocked = reconcile_observations(core_missing)
    assert blocked.promotion_allowed is False
    assert blocked.audit["status"] == "blocked"
    assert blocked.audit["quarantined_row_count"] == 1
    assert blocked.quarantined.iloc[0]["quarantine_reason"] == "required_value_missing"


def test_fail_closed_quality_detects_empty_st_mojibake_and_partition_tampering(tmp_path: Path):
    assert is_probable_mojibake("ƽ������") is True
    gate = DataQualityGate()
    gate.check_text_encoding(pd.DataFrame({"name": ["ƽ������"]}), ["name"])
    gate.check_historical_st(pd.DataFrame(), available=True, degraded=False)
    with pytest.raises(DataQualityError) as blocked:
        gate.raise_if_blocked()
    assert {issue.code for issue in blocked.value.report.issues} == {
        "probable_mojibake",
        "st_history_unverified",
    }

    path = tmp_path / "raw" / "daily" / "trade_date=2024-01-02" / "part-000.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"valid-partition")
    plan = {
        "partitions": [
            {
                "key": "daily/2024-01-02",
                "dataset": "daily",
                "trade_date": "2024-01-02",
                "path": str(path),
            }
        ]
    }
    checkpoint = {
        "partitions": {
            "daily/2024-01-02": {
                "status": "complete",
                "dataset": "daily",
                "trade_date": "2024-01-02",
                "path": str(path),
                "row_count": 1,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_path(path),
            }
        }
    }
    assert DataQualityGate().check_partition_coverage(plan, checkpoint).report().status == "pass"
    path.write_bytes(b"tampered")
    report = DataQualityGate().check_partition_coverage(plan, checkpoint).report()
    codes = {issue.code for issue in report.issues}
    assert "size_mismatch" in codes
    assert "hash_mismatch" in codes


def _environment_hashes(seed: str = "a") -> dict[str, str]:
    return {
        "config_hash": seed * 64,
        "code_hash": "b" * 64,
        "dirty_patch_hash": "c" * 64,
        "dependency_lock_hash": "d" * 64,
    }


def test_immutable_snapshot_identity_verification_and_publish(tmp_path: Path):
    data = tmp_path / "data.parquet"
    data.write_bytes(b"snapshot-data")
    quality = QualityReport(issues=(), checks=("fixture",))
    first = build_immutable_snapshot_manifest(
        [data],
        base_dir=tmp_path,
        tier="bronze",
        as_of="2024-01-02T16:00:00Z",
        parent_snapshot_ids=[],
        environment_hashes=_environment_hashes(),
        quality_report=quality,
    )
    second = build_immutable_snapshot_manifest(
        [data],
        base_dir=tmp_path,
        tier="bronze",
        as_of="2024-01-02T16:00:00Z",
        parent_snapshot_ids=[],
        environment_hashes=_environment_hashes(),
        quality_report=quality,
    )
    assert first.snapshot_id == second.snapshot_id
    assert verify_immutable_snapshot_manifest(first, base_dir=tmp_path)["valid"] is True
    public_ref = first.to_snapshot_ref(uri="s3://factor-lab/bronze/fixture")
    assert public_ref.content_hash == first.snapshot_id
    assert public_ref.quality_status.value == "accepted"
    changed_environment = build_immutable_snapshot_manifest(
        [data],
        base_dir=tmp_path,
        tier="bronze",
        as_of="2024-01-02T16:00:00Z",
        parent_snapshot_ids=[],
        environment_hashes=_environment_hashes("e"),
        quality_report=quality,
    )
    assert changed_environment.snapshot_id != first.snapshot_id

    published = publish_snapshot_manifest(tmp_path / "snapshots", first)
    assert publish_snapshot_manifest(tmp_path / "snapshots", first) == published
    conflicting = replace(first, trust_labels=("tampered",))
    with pytest.raises(SnapshotIntegrityError, match="immutable identity"):
        publish_snapshot_manifest(tmp_path / "snapshots", conflicting)

    with pytest.raises(SnapshotIntegrityError, match="Gold snapshots require"):
        build_immutable_snapshot_manifest(
            [data],
            base_dir=tmp_path,
            tier="gold",
            as_of="2024-01-02T16:00:00Z",
            parent_snapshot_ids=["f" * 64],
            environment_hashes=_environment_hashes(),
            quality_report={"status": "blocked"},
        )
    blocked_gold = replace(first, tier="gold", quality_status="blocked")
    with pytest.raises(SnapshotIntegrityError, match="Gold publication is blocked"):
        publish_snapshot_manifest(tmp_path / "snapshots", blocked_gold)
    valid_gold = build_immutable_snapshot_manifest(
        [data],
        base_dir=tmp_path,
        tier="gold",
        as_of="2024-01-02T16:00:00Z",
        parent_snapshot_ids=[first.snapshot_id],
        environment_hashes=_environment_hashes(),
        quality_report=quality,
    )
    with pytest.raises(SnapshotIntegrityError, match="requires file verification"):
        publish_snapshot_manifest(tmp_path / "snapshots", valid_gold)
    assert publish_snapshot_manifest(
        tmp_path / "snapshots", valid_gold, base_dir=tmp_path
    ).exists()


class _EmptyNameChange:
    def namechange(self, **kwargs):
        return pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date", "change_reason"])


def test_legacy_expanded_path_no_longer_marks_empty_st_or_fake_checkpoint_as_valid(tmp_path: Path):
    snapshot = fetch_historical_st_history(
        _EmptyNameChange(),
        start_date="2017-01-01",
        end_date="2025-12-31",
    )
    assert snapshot.available is False
    assert snapshot.degraded is True
    assert snapshot.reason == "historical_st_empty_response"
    cached = normalize_historical_st_snapshot(
        HistoricalSTSnapshot(pd.DataFrame(), available=True, degraded=False)
    )
    assert cached.available is False
    candidates = pd.DataFrame({"ts_code": ["000001.SZ"], "as_of_date": ["2024-01-02"]})
    _, audit = apply_historical_st_filter(candidates, pd.DataFrame(), allow_degraded=True)
    assert audit["status"] == "degraded_unavailable"

    partition = {
        "key": "daily/2024-01-02",
        "dataset": "daily",
        "trade_date": "2024-01-02",
        "path": str(tmp_path / "missing.parquet"),
    }
    checkpoint = {
        "partitions": {
            partition["key"]: {
                **partition,
                "status": "complete",
                "sha256": "a" * 64,
                "row_count": 1,
                "size_bytes": 10,
            }
        }
    }
    verified, verification = filter_verified_raw_checkpoint(checkpoint)
    assert verified["partitions"] == {}
    assert verification["failure_counts"]["file_missing"] == 1

    empty_audit = audit_raw_partition(
        pd.DataFrame(columns=["ts_code", "trade_date"]),
        partition,
    )
    assert empty_audit["status"] == "fail"
    assert "empty_open_market_partition" in empty_audit["issues"]


def test_cached_legacy_reference_false_positive_is_downgraded_without_redownload(tmp_path: Path):
    reference = tmp_path / "output" / "reference"
    reference.mkdir(parents=True)
    pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "name": ["ƽ������"],
            "area": ["����"],
            "industry": ["����"],
        }
    ).to_parquet(reference / "stock_metadata.parquet", index=False)
    pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date", "change_reason"]).to_parquet(
        reference / "historical_st.parquet", index=False
    )
    (reference / "historical_st_status.json").write_text(
        '{"available": true, "degraded": false, "reason": null}', encoding="utf-8"
    )

    _, snapshot, status = cache_reference_data(
        object(),
        {"output_dir": str(tmp_path / "output")},
        {"analysis_start": "2017-01-01", "analysis_end": "2025-12-31"},
    )

    assert snapshot.available is False
    assert snapshot.degraded is True
    assert snapshot.reason == "historical_st_empty_cached_table"
    assert status["promotion_allowed"] is False
    assert status["trust_labels"] == ["st_history_unverified"]
    assert {issue["code"] for issue in status["quality"]["issues"]} == {
        "probable_mojibake",
        "st_history_unverified",
    }
