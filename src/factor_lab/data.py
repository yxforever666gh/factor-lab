from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd


@dataclass
class SampleDataset:
    frame: pd.DataFrame


class SampleDataGenerator:
    def __init__(self, seed: int = 7) -> None:
        self.rng = np.random.default_rng(seed)

    def generate(self, num_stocks: int = 60, num_days: int = 220) -> SampleDataset:
        tickers = [f"STK{i:03d}" for i in range(1, num_stocks + 1)]
        dates = pd.bdate_range("2025-01-01", periods=num_days)

        base_quality = self.rng.normal(0.0, 1.0, num_stocks)
        base_value = self.rng.normal(0.0, 1.0, num_stocks)
        base_liquidity = self.rng.normal(0.0, 1.0, num_stocks)
        price_anchor = self.rng.uniform(20.0, 120.0, num_stocks)

        rows = []
        prev_close = price_anchor.copy()
        trailing_returns = {ticker: [] for ticker in tickers}
        trailing_turnover = {ticker: [] for ticker in tickers}

        for day_idx, date in enumerate(dates):
            market_noise = self.rng.normal(0.0, 0.004)
            quality_drift = self.rng.normal(0.0, 0.02, num_stocks)
            value_drift = self.rng.normal(0.0, 0.02, num_stocks)
            liquidity_drift = self.rng.normal(0.0, 0.03, num_stocks)

            base_quality = base_quality + quality_drift
            base_value = base_value + value_drift
            base_liquidity = base_liquidity + liquidity_drift

            roe = 0.12 + base_quality * 0.03 + self.rng.normal(0.0, 0.01, num_stocks)
            pb = 2.2 - base_value * 0.35 + self.rng.normal(0.0, 0.08, num_stocks)
            earnings_yield = 0.055 + base_value * 0.015 + self.rng.normal(0.0, 0.004, num_stocks)
            turnover = np.exp(0.6 + base_liquidity * 0.35 + self.rng.normal(0.0, 0.2, num_stocks))

            momentum_20 = []
            momentum_60 = []
            momentum_120 = []
            momentum_60_skip_5 = []
            turnover_shock = []
            daily_returns = []
            closes = []

            for i, ticker in enumerate(tickers):
                past_rets = trailing_returns[ticker]
                past_turnovers = trailing_turnover[ticker]
                mom = float(np.sum(past_rets[-20:])) if past_rets else 0.0
                mom60 = float(np.sum(past_rets[-60:])) if past_rets else 0.0
                mom120 = float(np.sum(past_rets[-120:])) if past_rets else 0.0
                skip_tail = past_rets[:-5] if len(past_rets) > 5 else []
                mom60_skip_5 = float(np.sum(skip_tail[-60:])) if skip_tail else 0.0
                t5 = float(np.mean(past_turnovers[-5:])) if len(past_turnovers) >= 5 else float(turnover[i])
                t20 = float(np.mean(past_turnovers[-20:])) if len(past_turnovers) >= 20 else float(turnover[i])
                shock = (t5 / t20) - 1.0 if t20 else 0.0

                alpha = (
                    0.04 * mom
                    + 0.60 * (float(earnings_yield[i]) - 0.055)
                    + 0.40 * (float(roe[i]) - 0.12)
                    + 0.01 * shock
                )
                ret = float(np.clip(market_noise + alpha + self.rng.normal(0.0, 0.02), -0.12, 0.12))
                close = max(float(prev_close[i] * (1.0 + ret)), 1.0)

                momentum_20.append(mom)
                momentum_60.append(mom60)
                momentum_120.append(mom120)
                momentum_60_skip_5.append(mom60_skip_5)
                turnover_shock.append(shock)
                daily_returns.append(ret)
                closes.append(close)

            frame_day = pd.DataFrame(
                {
                    "date": date,
                    "ticker": tickers,
                    "close": closes,
                    "return_1d": daily_returns,
                    "roe": roe,
                    "pb": pb,
                    "earnings_yield": earnings_yield,
                    "turnover": turnover,
                    "momentum_20": momentum_20,
                    "momentum_60": momentum_60,
                    "momentum_120": momentum_120,
                    "momentum_60_skip_5": momentum_60_skip_5,
                    "turnover_shock_5_20": turnover_shock,
                }
            )
            rows.append(frame_day)

            for i, (ticker, close, ret, to) in enumerate(zip(tickers, closes, daily_returns, turnover)):
                prev_close[i] = close
                trailing_returns[ticker].append(ret)
                trailing_turnover[ticker].append(float(to))

        frame = pd.concat(rows, ignore_index=True)
        frame["forward_return_5d"] = (
            frame.groupby("ticker")["return_1d"].transform(
                lambda s: s.shift(-1).rolling(5).sum()
            )
        )
        frame = frame.dropna(subset=["forward_return_5d"]).reset_index(drop=True)
        return SampleDataset(frame=frame)
