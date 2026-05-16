from __future__ import annotations


def parse_portfolio_construction(config: dict) -> dict:
    raw = config.get("portfolio_construction") or {}
    mode = raw.get("mode") or "top_bottom"
    if mode == "top_bottom":
        return {"mode": "top_bottom"}
    if mode != "bucket_pair":
        raise ValueError(f"unsupported portfolio_construction.mode: {mode}")
    quantiles = int(raw.get("quantiles") or 5)
    if "long_quantile" not in raw or "short_quantile" not in raw:
        raise ValueError("bucket_pair requires long_quantile and short_quantile")
    long_q = int(raw.get("long_quantile"))
    short_q = int(raw.get("short_quantile"))
    if quantiles < 2:
        raise ValueError("quantiles must be >= 2")
    if not (0 <= long_q < quantiles and 0 <= short_q < quantiles):
        raise ValueError("bucket quantiles out of range")
    if long_q == short_q:
        raise ValueError("long_quantile and short_quantile must differ")
    return {"mode": "bucket_pair", "quantiles": quantiles, "long_quantile": long_q, "short_quantile": short_q}
