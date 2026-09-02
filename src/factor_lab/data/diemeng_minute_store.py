"""Offline verifier/adapter for a finalized 13.0 candidate minute capture."""

from __future__ import annotations

import json
from math import isfinite, isinf
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from factor_lab.release_integrity import canonical_payload_sha256, file_sha256
from factor_lab.research.pit_stock import PITStockContractError, canonical_sha256
from factor_lab.research.pit_stock_minute_execution import (
    MINUTE_EXECUTION_BAR_COLUMNS,
    MINUTE_EXECUTION_CONTEXT_COLUMNS,
)

from .diemeng_minutes import DIEMENG_MINUTE_COLUMNS


WINDOW_CLOCKS = {
    "A": ("09:31:00", "09:35:00"),
    "B": ("09:37:00", "09:41:00"),
    "C": ("09:43:00", "09:47:00"),
}
MINUTE_AUCTION_COLUMNS = (
    "ticker",
    "trade_time",
    "observable_at",
    "open",
    "zero_liquidity_flat_price",
)
MINUTE_AUCTION_READ_COLUMNS = (
    "ticker",
    "trade_time",
    "observable_at",
    "open",
    "high",
    "low",
    "close",
    "volume_shares",
    "amount_rmb",
)
LIMIT_COLUMNS = ("trade_date", "ticker", "pre_close", "up_limit", "down_limit")
SNAPSHOT_IDENTITY_COLUMNS = (
    "ticker",
    "adv20",
    "vol63",
    "mom12",
    "mom6",
    "industry",
    "size_bucket",
    "universe_member",
)


def candidate_snapshot_payload(frame: pd.DataFrame) -> str:
    if not set(SNAPSHOT_IDENTITY_COLUMNS).issubset(frame.columns):
        raise PITStockContractError("candidate minute snapshot columns differ")
    value = frame.loc[:, SNAPSHOT_IDENTITY_COLUMNS].copy()
    if value["ticker"].astype(str).duplicated().any():
        raise PITStockContractError("candidate minute snapshot tickers are duplicate")
    value["ticker"] = value["ticker"].astype(str)
    for column in ("adv20", "vol63", "mom12", "mom6"):
        numeric = pd.to_numeric(value[column], errors="raise").astype(float)
        if numeric.map(isinf).any():
            raise PITStockContractError(
                "candidate minute snapshot contains infinite factor"
            )
        value[column] = numeric.astype(object).where(numeric.notna(), None)
    value["universe_member"] = value["universe_member"].astype(bool)
    for column in ("industry", "size_bucket"):
        value[column] = value[column].astype(object).where(value[column].notna(), None)
    value = value.sort_values("ticker", kind="mergesort").reset_index(drop=True)
    return canonical_sha256(value.to_dict("records"))


class CandidateMinuteStore:
    """Read a complete immutable capture; this class never calls a provider."""

    def __init__(
        self,
        root: str | Path,
        *,
        expected_manifest_payload_sha256: str | None = None,
        expected_manifest_file_sha256: str | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        manifest_path = self.root / "manifest.json"
        plan_path = self.root / "capture-plan.json"
        for role, value in (
            ("payload", expected_manifest_payload_sha256),
            ("file", expected_manifest_file_sha256),
        ):
            text = "" if value is None else str(value)
            if (
                len(text) != 64
                or any(character not in "0123456789abcdef" for character in text)
            ):
                raise PITStockContractError(
                    f"external candidate minute manifest {role} SHA is required"
                )
        if not manifest_path.is_file() or not plan_path.is_file():
            raise FileNotFoundError("finalized candidate minute capture is absent")
        self.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if (
            self.manifest.get("payload_sha256")
            != canonical_payload_sha256(self.manifest)
            or self.plan.get("payload_sha256") != canonical_payload_sha256(self.plan)
            or self.manifest.get("status")
            != "candidate_minutes_captured_return_unopened"
            or self.manifest.get("plan_payload_sha256")
            != self.plan.get("payload_sha256")
        ):
            raise PITStockContractError("candidate minute manifest/plan differs")
        if self.manifest["payload_sha256"] != str(
            expected_manifest_payload_sha256
        ):
            raise PITStockContractError("candidate minute manifest identity differs")
        if file_sha256(manifest_path) != str(expected_manifest_file_sha256):
            raise PITStockContractError("candidate minute manifest file differs")
        self._pairs: dict[tuple[str, str], dict[str, Any]] = {}
        for value in self.plan.get("pairs", []):
            key = (str(value["execution_date"]), str(value["ticker"]))
            if key in self._pairs:
                raise PITStockContractError("candidate minute plan pair is duplicate")
            self._pairs[key] = dict(value)
        self._executions = {
            (str(value["signal_date"]), str(value["execution_date"])): dict(value)
            for value in self.plan.get("executions", [])
        }
        if len(self._executions) != int(self.plan.get("signal_count", -1)):
            raise PITStockContractError("candidate minute execution plan differs")
        self._pair_anchors: dict[tuple[str, str], dict[str, Any]] = {}
        self._limit_anchors: dict[str, dict[str, Any]] = {}
        for artifact in self.manifest.get("artifacts", []):
            role = artifact.get("role")
            if role == "minute_pair":
                identity = artifact.get("identity") or {}
                key = (
                    str(identity.get("execution_date")),
                    str(identity.get("ticker")),
                )
                if key in self._pair_anchors:
                    raise PITStockContractError(
                        "candidate minute manifest pair is duplicate"
                    )
                self._pair_anchors[key] = dict(artifact)
            elif role == "price_limits":
                date = str(artifact.get("execution_date"))
                if date in self._limit_anchors:
                    raise PITStockContractError(
                        "candidate limit manifest date is duplicate"
                    )
                self._limit_anchors[date] = dict(artifact)
            else:
                raise PITStockContractError(
                    "candidate minute manifest artifact role differs"
                )
        if set(self._pair_anchors) != set(self._pairs) or set(
            self._limit_anchors
        ) != {value[1] for value in self._executions}:
            raise PITStockContractError(
                "candidate minute manifest artifact scope differs"
            )
        self._pair_artifact_cache: dict[tuple[str, str], Path] = {}
        self._limit_cache: dict[str, pd.DataFrame] = {}

    def _pair_path(self, date: str, ticker: str) -> Path:
        return (
            self.root
            / "minutes"
            / f"execution_date={date}"
            / f"ticker={ticker}"
        )

    def _verify_pair_artifact(self, date: str, ticker: str) -> Path:
        key = (date, ticker)
        if key in self._pair_artifact_cache:
            return self._pair_artifact_cache[key]
        if key not in self._pairs:
            raise PITStockContractError(
                f"candidate minute scope lacks {ticker} on {date}"
            )
        root = self._pair_path(date, ticker)
        receipt = json.loads((root / "receipt.json").read_text(encoding="utf-8"))
        anchor = self._pair_anchors[key]
        path = root / "data.parquet"
        if (
            receipt.get("payload_sha256") != canonical_payload_sha256(receipt)
            or receipt.get("payload_sha256")
            != anchor.get("receipt_payload_sha256")
            or file_sha256(root / "receipt.json")
            != anchor.get("receipt_file_sha256")
            or receipt.get("identity") != self._pairs[key]
            or not path.is_file()
            or path.stat().st_size != int(receipt["artifact"]["size_bytes"])
            or file_sha256(path) != receipt["artifact"]["file_sha256"]
            or file_sha256(path) != anchor.get("data_file_sha256")
        ):
            raise PITStockContractError("candidate minute pair artifact differs")
        parquet = pq.ParquetFile(path)
        if (
            tuple(map(str, parquet.schema_arrow.names)) != DIEMENG_MINUTE_COLUMNS
            or parquet.metadata is None
            or parquet.metadata.num_rows != int(receipt["artifact"]["row_count"])
        ):
            raise PITStockContractError("candidate minute pair frame differs")
        self._pair_artifact_cache[key] = path
        return path

    def _read_pair_slice(
        self,
        date: str,
        ticker: str,
        *,
        trade_times: tuple[pd.Timestamp, ...],
        columns: tuple[str, ...],
    ) -> pd.DataFrame:
        if not trade_times or len(trade_times) != len(set(trade_times)):
            raise PITStockContractError("candidate minute requested clocks differ")
        if not set(columns).issubset(DIEMENG_MINUTE_COLUMNS):
            raise PITStockContractError("candidate minute requested columns differ")
        expected_times = tuple(pd.Timestamp(value) for value in trade_times)
        expected_set = set(expected_times)
        expected_date = pd.Timestamp(date).normalize()
        if any(value.normalize() != expected_date for value in expected_times):
            raise PITStockContractError("candidate minute requested date differs")
        path = self._verify_pair_artifact(date, ticker)
        frame = pd.read_parquet(
            path,
            columns=list(columns),
            filters=[("trade_time", "in", list(expected_times))],
        )
        if tuple(map(str, frame.columns)) != columns:
            raise PITStockContractError("candidate minute filtered columns differ")
        if not frame.empty:
            times = pd.to_datetime(frame["trade_time"], errors="coerce")
            observable = pd.to_datetime(frame["observable_at"], errors="coerce")
            if (
                times.isna().any()
                or observable.isna().any()
                or frame["ticker"].astype(str).ne(ticker).any()
                or times.dt.date.astype(str).ne(date).any()
                or times.duplicated().any()
                or not set(times).issubset(expected_set)
                or not observable.eq(times + pd.Timedelta(minutes=1)).all()
            ):
                raise PITStockContractError(
                    "candidate minute pair identity/time differs"
                )
            frame = frame.copy()
            frame["trade_time"] = times
            frame["observable_at"] = observable
            frame = frame.sort_values("trade_time", kind="mergesort").reset_index(
                drop=True
            )
        return frame

    def _read_limits(self, date: str) -> pd.DataFrame:
        if date in self._limit_cache:
            return self._limit_cache[date].copy()
        root = self.root / "limits" / f"execution_date={date}"
        receipt = json.loads((root / "receipt.json").read_text(encoding="utf-8"))
        anchor = self._limit_anchors[date]
        path = root / "limits.parquet"
        if (
            receipt.get("payload_sha256") != canonical_payload_sha256(receipt)
            or receipt.get("payload_sha256")
            != anchor.get("receipt_payload_sha256")
            or file_sha256(root / "receipt.json")
            != anchor.get("receipt_file_sha256")
            or receipt.get("execution_date") != date
            or not path.is_file()
            or path.stat().st_size != int(receipt["artifact"]["size_bytes"])
            or file_sha256(path) != receipt["artifact"]["file_sha256"]
            or file_sha256(path) != anchor.get("data_file_sha256")
        ):
            raise PITStockContractError("candidate price-limit artifact differs")
        frame = pd.read_parquet(path)
        if (
            tuple(map(str, frame.columns)) != LIMIT_COLUMNS
            or frame["ticker"].astype(str).duplicated().any()
            or (not frame.empty and not frame["trade_date"].astype(str).eq(date).all())
        ):
            raise PITStockContractError("candidate price-limit frame differs")
        frame = frame.copy()
        frame["ticker"] = frame["ticker"].astype(str)
        for column in ("pre_close", "up_limit", "down_limit"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)
        finite_positive_limits = frame[["up_limit", "down_limit"]].map(
            lambda value: isfinite(float(value)) and float(value) > 0.0
        )
        invalid_pre_close = frame["pre_close"].map(
            lambda value: isinf(float(value))
            or (isfinite(float(value)) and float(value) <= 0.0)
        )
        if (
            not bool(finite_positive_limits.all(axis=None))
            or bool(invalid_pre_close.any())
            or frame["down_limit"].gt(frame["up_limit"]).any()
        ):
            invalid = frame.loc[
                ~finite_positive_limits.all(axis=1)
                | invalid_pre_close
                | frame["down_limit"].gt(frame["up_limit"]),
                ["ticker", "pre_close", "up_limit", "down_limit"],
            ]
            raise PITStockContractError(
                "candidate price-limit values differ: "
                f"execution_date={date} rows={invalid.head(10).to_dict('records')}"
            )
        frame = frame.sort_values("ticker", kind="mergesort").reset_index(drop=True)
        self._limit_cache[date] = frame.copy()
        return frame

    def _execution_scope(
        self,
        *,
        signal_date: Any,
        execution_date: Any,
        required_tickers: set[str],
    ) -> tuple[str, dict[str, Any], set[str]]:
        signal = pd.Timestamp(signal_date).date().isoformat()
        execution = pd.Timestamp(execution_date).date().isoformat()
        plan = self._executions.get((signal, execution))
        if plan is None:
            raise PITStockContractError("candidate minute signal/execution differs")
        expected = {str(value) for value in required_tickers}
        if not expected.issubset(set(map(str, plan["tickers"]))):
            raise PITStockContractError(
                "candidate minute mark/order scope is incomplete"
            )
        return execution, plan, expected

    def build_context(
        self,
        *,
        signal_date: Any,
        execution_date: Any,
        required_tickers: set[str],
        signal_snapshot: pd.DataFrame,
    ) -> pd.DataFrame:
        """Return only immutable signal and daily-limit execution context."""

        execution, plan, expected = self._execution_scope(
            signal_date=signal_date,
            execution_date=execution_date,
            required_tickers=required_tickers,
        )
        if plan.get("mark_only") is True:
            raise PITStockContractError(
                "mark-only sentinel does not permit execution context reads"
            )
        if candidate_snapshot_payload(signal_snapshot) != plan.get(
            "snapshot_payload_sha256"
        ):
            raise PITStockContractError("candidate minute frozen snapshot differs")
        if not expected:
            return pd.DataFrame(columns=MINUTE_EXECUTION_CONTEXT_COLUMNS)
        snapshot = signal_snapshot.copy()
        if snapshot["ticker"].astype(str).duplicated().any():
            raise PITStockContractError("candidate minute signal snapshot is duplicate")
        snapshot["ticker"] = snapshot["ticker"].astype(str)
        snapshot = snapshot.set_index("ticker", drop=False)
        limits = self._read_limits(execution).set_index("ticker")
        rows: list[dict[str, Any]] = []
        for ticker in sorted(expected):
            if ticker not in snapshot.index:
                raise PITStockContractError("candidate minute signal input is missing")
            adv = float(snapshot.at[ticker, "adv20"])
            volatility = float(snapshot.at[ticker, "vol63"]) / (252.0**0.5)
            if (
                isfinite(adv)
                and adv <= 0.0
                or isfinite(volatility)
                and volatility < 0.0
                or isinf(adv)
                or isinf(volatility)
            ):
                raise PITStockContractError(
                    "candidate minute signal inputs are invalid"
                )
            value: dict[str, Any] = {
                "ticker": ticker,
                "signal_adv20": adv,
                "signal_vol_daily": volatility,
            }
            if ticker not in limits.index:
                value["up_limit"] = float("nan")
                value["down_limit"] = float("nan")
            else:
                limit = limits.loc[ticker]
                value["up_limit"] = float(limit["up_limit"])
                value["down_limit"] = float(limit["down_limit"])
            rows.append(value)
        return pd.DataFrame(rows, columns=MINUTE_EXECUTION_CONTEXT_COLUMNS)

    def build_auction_anchors(
        self,
        *,
        signal_date: Any,
        execution_date: Any,
        required_tickers: set[str],
    ) -> tuple[pd.DataFrame, set[str]]:
        """Return exact 09:30 anchors plus attested complete-no-anchor names."""

        execution, _plan, expected = self._execution_scope(
            signal_date=signal_date,
            execution_date=execution_date,
            required_tickers=required_tickers,
        )
        rows: list[dict[str, Any]] = []
        expected_trade = pd.Timestamp(f"{execution} 09:30:00")
        expected_observable = pd.Timestamp(f"{execution} 09:31:00")
        complete_no_anchor: set[str] = set()
        for ticker in sorted(expected):
            anchor = self._read_pair_slice(
                execution,
                ticker,
                trade_times=(expected_trade,),
                columns=MINUTE_AUCTION_READ_COLUMNS,
            )
            if anchor.empty:
                complete_no_anchor.add(ticker)
                continue
            if len(anchor) != 1:
                raise PITStockContractError(
                    "candidate minute 09:30 auction anchor is duplicate"
                )
            row = anchor.iloc[0]
            opening = float(row["open"])
            prices = tuple(
                float(row[column]) for column in ("open", "high", "low", "close")
            )
            volume = float(row["volume_shares"])
            amount = float(row["amount_rmb"])
            zero_liquidity_flat_price = (
                volume == 0.0
                and amount == 0.0
                and max(prices) - min(prices) <= 1e-12
            )
            if (
                pd.Timestamp(row["observable_at"]) != expected_observable
                or not isfinite(opening)
                or opening <= 0.0
                or not all(isfinite(value) and value > 0.0 for value in prices)
                or not isfinite(volume)
                or not isfinite(amount)
                or volume < 0.0
                or amount < 0.0
                or (volume == 0.0) != (amount == 0.0)
            ):
                raise PITStockContractError(
                    "candidate minute 09:30 auction anchor differs"
                )
            rows.append(
                {
                    "ticker": ticker,
                    "trade_time": expected_trade,
                    "observable_at": expected_observable,
                    "open": opening,
                    "zero_liquidity_flat_price": zero_liquidity_flat_price,
                }
            )
        return (
            pd.DataFrame(rows, columns=MINUTE_AUCTION_COLUMNS),
            complete_no_anchor,
        )

    def build_window(
        self,
        *,
        signal_date: Any,
        execution_date: Any,
        required_tickers: set[str],
        window: str,
    ) -> tuple[pd.DataFrame, set[str]]:
        """Return one causally completed aggregate window and its no-bar scope."""

        execution, plan, expected = self._execution_scope(
            signal_date=signal_date,
            execution_date=execution_date,
            required_tickers=required_tickers,
        )
        if window not in WINDOW_CLOCKS:
            raise PITStockContractError("candidate minute window must be A, B, or C")
        if plan.get("mark_only") is True:
            raise PITStockContractError(
                "mark-only sentinel does not permit A/B/C window reads"
            )
        start_clock, end_clock = WINDOW_CLOCKS[window]
        clocks = pd.date_range(
            f"2000-01-01 {start_clock}",
            f"2000-01-01 {end_clock}",
            freq="1min",
        ).strftime("%H:%M:%S").tolist()
        requested_times = tuple(
            pd.Timestamp(f"{execution} {clock}") for clock in clocks
        )
        expected_trade = pd.Timestamp(f"{execution} {end_clock}")
        expected_observable = expected_trade + pd.Timedelta(minutes=1)
        rows: list[dict[str, Any]] = []
        complete_no_bar: set[str] = set()
        for ticker in sorted(expected):
            bars = self._read_pair_slice(
                execution,
                ticker,
                trade_times=requested_times,
                columns=MINUTE_EXECUTION_BAR_COLUMNS,
            )
            if bars.empty:
                complete_no_bar.add(ticker)
                continue
            if len(bars) != len(requested_times):
                raise PITStockContractError(
                    "candidate minute aggregate window is partial"
                )
            volume = float(bars["volume_shares"].sum())
            amount = float(bars["amount_rmb"].sum())
            value = {
                "ticker": ticker,
                "trade_time": expected_trade,
                "observable_at": expected_observable,
                "open": float(bars.iloc[0]["open"]),
                "high": float(bars["high"].max()),
                "low": float(bars["low"].min()),
                "close": float(bars.iloc[-1]["close"]),
                "volume_shares": volume,
                "amount_rmb": amount,
            }
            numeric = tuple(
                float(value[field])
                for field in (
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume_shares",
                    "amount_rmb",
                )
            )
            if (
                not all(isfinite(item) for item in numeric)
                or min(numeric[:4]) <= 0.0
                or volume < 0.0
                or amount < 0.0
                or (volume == 0.0) != (amount == 0.0)
            ):
                raise PITStockContractError(
                    "candidate minute aggregate window differs"
                )
            rows.append(value)
        return (
            pd.DataFrame(rows, columns=MINUTE_EXECUTION_BAR_COLUMNS),
            complete_no_bar,
        )


__all__ = [
    "CandidateMinuteStore",
    "LIMIT_COLUMNS",
    "MINUTE_AUCTION_COLUMNS",
    "SNAPSHOT_IDENTITY_COLUMNS",
    "WINDOW_CLOCKS",
    "candidate_snapshot_payload",
]
