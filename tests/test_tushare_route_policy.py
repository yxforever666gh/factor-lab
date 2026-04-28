from __future__ import annotations

from factor_lab import tushare_provider


class _FakePro:
    def __init__(self):
        self.calls = []
        self._DataApi__timeout = None

    def stock_basic(self, **kwargs):
        self.calls.append(("stock_basic", kwargs))
        import pandas as pd

        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "PingAn",
                    "area": "SZ",
                    "industry": "bank",
                    "list_date": "19910403",
                }
            ]
        )


def _build_provider(monkeypatch):
    fake = _FakePro()
    monkeypatch.setattr(tushare_provider, "get_required_env", lambda name: "token")
    monkeypatch.setattr(tushare_provider.ts, "set_token", lambda token: None)
    monkeypatch.setattr(tushare_provider.ts, "pro_api", lambda token: fake)
    provider = tushare_provider.TushareDataProvider(token="token")
    return provider, fake


def test_direct_route_clears_proxy_and_sets_no_proxy(monkeypatch, tmp_path):
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.local:7890")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.local:7890")
    monkeypatch.setenv("FACTOR_LAB_TUSHARE_ROUTE_MODE", "direct")
    monkeypatch.setenv("FACTOR_LAB_TUSHARE_NO_PROXY_HOSTS", "api.waditu.com,example.com")
    monkeypatch.setattr(tushare_provider, "_route_status_path", lambda: tmp_path / "route.json")
    provider, _ = _build_provider(monkeypatch)

    with provider._route_env("direct"):
        assert "HTTP_PROXY" not in tushare_provider.os.environ
        assert "HTTPS_PROXY" not in tushare_provider.os.environ
        assert "api.waditu.com" in tushare_provider.os.environ["NO_PROXY"]
        assert "example.com" in tushare_provider.os.environ["NO_PROXY"]

    assert tushare_provider.os.environ["HTTP_PROXY"] == "http://proxy.local:7890"
    assert tushare_provider.os.environ["HTTPS_PROXY"] == "http://proxy.local:7890"


def test_hybrid_route_preserves_proxy_and_appends_no_proxy(monkeypatch, tmp_path):
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.local:7890")
    monkeypatch.setenv("NO_PROXY", "localhost")
    monkeypatch.setenv("FACTOR_LAB_TUSHARE_ROUTE_MODE", "hybrid")
    monkeypatch.setattr(tushare_provider, "_route_status_path", lambda: tmp_path / "route.json")
    provider, _ = _build_provider(monkeypatch)

    with provider._route_env("hybrid"):
        assert tushare_provider.os.environ["HTTP_PROXY"] == "http://proxy.local:7890"
        assert "localhost" in tushare_provider.os.environ["NO_PROXY"]
        assert "api.waditu.com" in tushare_provider.os.environ["NO_PROXY"]


def test_auto_route_uses_cached_good_route(monkeypatch, tmp_path):
    route_path = tmp_path / "route.json"
    route_path.write_text(
        '{"updated_at_utc": "2099-01-01T00:00:00+00:00", "resolved_mode": "proxy", "healthy": true, "last_probe_ms": 12.3}',
        encoding="utf-8",
    )
    monkeypatch.setenv("FACTOR_LAB_TUSHARE_ROUTE_MODE", "auto")
    monkeypatch.setenv("FACTOR_LAB_TUSHARE_PROXY_URL", "http://proxy.local:7890")
    monkeypatch.setattr(tushare_provider, "_route_status_path", lambda: route_path)
    provider, _ = _build_provider(monkeypatch)

    assert provider.route_policy.resolved_mode == "proxy"
    assert provider.route_status()["healthy"] is True
