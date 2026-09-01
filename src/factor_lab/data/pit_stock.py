"""Compact raw-data adapter for quarterly point-in-time stock snapshots."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import json
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.data.catalog import RuntimeLayout
from factor_lab.data.security_master import audit_security_master
from factor_lab.research.pit_stock import (
    PITStockContractError,
    PITStockStrategyConfig,
    canonical_sha256,
    official_quarter_end_sessions,
)


@dataclass(frozen=True)
class PITPanelBuild:
    panel: pd.DataFrame
    signal_dates: tuple[str, ...]
    maximum_read_date: str
    source_receipt: dict[str, Any]
    panel_payload_sha256: str


def _date(value: Any, *, field: str) -> pd.Timestamp:
    if value is None or value is pd.NaT or pd.isna(value):
        raise PITStockContractError(f"{field} must be known")
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise PITStockContractError(f"invalid {field}: {value!r}")
    result = pd.Timestamp(parsed)
    if result.tzinfo is not None:
        result = result.tz_convert(None)
    return result.normalize()


def _date_series(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.strip()
    compact = text.str.fullmatch(r"\d{8}", na=False)
    result = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    if compact.any():
        result.loc[compact] = pd.to_datetime(
            text.loc[compact], format="%Y%m%d", errors="coerce"
        ).dt.normalize()
    ordinary = ~compact & text.notna() & text.ne("")
    if ordinary.any():
        result.loc[ordinary] = pd.to_datetime(
            text.loc[ordinary], errors="coerce"
        ).dt.normalize()
    return result


def _normalized_master(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"ts_code", "exchange", "curr_type", "list_date", "delist_date"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise PITStockContractError(f"security master missing columns: {missing}")
    work = frame[list(required)].copy()
    work["ts_code"] = work["ts_code"].astype("string").str.strip()
    work["list_date"] = _date_series(work["list_date"])
    work["delist_date"] = _date_series(work["delist_date"])
    work = work.loc[
        work["exchange"].isin(["SSE", "SZSE"])
        & work["curr_type"].astype("string").str.strip().eq("CNY")
    ].copy()
    if work["ts_code"].isna().any() or work["ts_code"].eq("").any():
        raise PITStockContractError("security master contains an unknown ticker")
    if work["list_date"].isna().any() or work.duplicated("ts_code").any():
        raise PITStockContractError("security master identity/list dates are invalid")
    invalid = work["delist_date"].notna() & work["delist_date"].lt(work["list_date"])
    if invalid.any():
        raise PITStockContractError("security master delist date precedes list date")
    return work.set_index("ts_code").sort_index()


def _indexed(frame: pd.DataFrame, *, role: str) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{role} must be a pandas DataFrame")
    work = frame.copy()
    if "ticker" in work.columns:
        identifier = "ticker"
    elif "ts_code" in work.columns:
        identifier = "ts_code"
    elif work.index.name in {"ticker", "ts_code"}:
        work = work.reset_index()
        identifier = str(work.columns[0])
    else:
        raise PITStockContractError(f"{role} requires ticker or ts_code")
    work[identifier] = work[identifier].astype("string").str.strip()
    if work[identifier].isna().any() or work[identifier].eq("").any():
        raise PITStockContractError(f"{role} contains an unknown ticker")
    if work.duplicated(identifier).any():
        raise PITStockContractError(f"{role} contains duplicate tickers")
    return work.set_index(identifier).rename_axis("ticker").sort_index()


def _size_buckets(values: pd.Series) -> pd.Series:
    result = pd.Series("UNKNOWN_SIZE", index=values.index, dtype="string")
    valid = pd.to_numeric(values, errors="coerce")
    valid = valid.loc[np.isfinite(valid) & valid.gt(0)]
    if len(valid) >= 3:
        ranks = valid.rank(method="first")
        result.loc[valid.index] = pd.qcut(
            ranks, 3, labels=["small", "mid", "large"]
        ).astype("string")
    return result


def build_pit_snapshot(
    *,
    signal_date: Any,
    official_sessions: Sequence[Any],
    security_master: pd.DataFrame,
    close_history: pd.DataFrame,
    amount_history: pd.DataFrame,
    current_market: pd.DataFrame,
    daily_basic: pd.DataFrame,
    st_tickers: Iterable[str],
    industry: pd.Series | Mapping[str, Any],
    industry_source_date: Any,
    config: PITStockStrategyConfig = PITStockStrategyConfig(),
) -> pd.DataFrame:
    """Build every active security row and mark the exact Top-1000 universe."""

    signal = _date(signal_date, field="signal_date")
    sessions = tuple(_date(value, field="official session") for value in official_sessions)
    if not sessions or list(sessions) != sorted(sessions) or len(set(sessions)) != len(sessions):
        raise PITStockContractError("official sessions must be unique and increasing")
    if signal not in sessions:
        raise PITStockContractError("signal_date is not an official session")
    signal_index = sessions.index(signal)
    expected_close_dates = sessions[
        signal_index - config.long_start_lag : signal_index + 1
    ]
    expected_amount_dates = sessions[signal_index - 19 : signal_index + 1]
    if len(expected_close_dates) != config.long_start_lag + 1 or len(expected_amount_dates) != 20:
        raise PITStockContractError("official history does not cover frozen factor windows")
    close = close_history.copy()
    amount = amount_history.copy()
    close.columns = tuple(_date(value, field="close_history date") for value in close.columns)
    amount.columns = tuple(_date(value, field="amount_history date") for value in amount.columns)
    if tuple(close.columns) != expected_close_dates:
        raise PITStockContractError("close history is not the exact 253-session window")
    if tuple(amount.columns) != expected_amount_dates:
        raise PITStockContractError("amount history is not the exact 20-session window")
    if close.index.duplicated().any() or amount.index.duplicated().any():
        raise PITStockContractError("history contains duplicate tickers")
    master = _normalized_master(security_master)
    active = master.loc[
        master["list_date"].le(signal)
        & (master["delist_date"].isna() | master["delist_date"].gt(signal))
    ].copy()
    frame = pd.DataFrame(index=active.index)
    frame.index.name = "ticker"
    frame["signal_date"] = signal
    market = _indexed(current_market, role="current_market")
    required_market = {"close_adj", "amount_rmb"}
    missing_market = sorted(required_market - set(market.columns))
    if missing_market:
        raise PITStockContractError(f"current_market missing columns: {missing_market}")
    basic = _indexed(daily_basic, role="daily_basic")
    for column in ("total_mv", "circ_mv"):
        if column not in basic:
            basic[column] = np.nan
    industries = pd.Series(industry, dtype="string")
    industries.index = industries.index.astype("string")
    industries = industries[~industries.index.duplicated(keep="first")]
    frame["industry"] = industries.reindex(frame.index).fillna("UNKNOWN")
    frame["industry"] = frame["industry"].mask(
        frame["industry"].str.strip().eq(""), "UNKNOWN"
    )
    source_date = _date(industry_source_date, field="industry_source_date")
    if source_date > signal or source_date.to_period("Q") != signal.to_period("Q"):
        raise PITStockContractError("industry source must be available in the signal quarter")
    frame["industry_source_date"] = source_date
    frame["total_mv"] = pd.to_numeric(basic["total_mv"], errors="coerce").reindex(frame.index)
    frame["circ_mv"] = pd.to_numeric(basic["circ_mv"], errors="coerce").reindex(frame.index)
    frame["has_signal_bar"] = frame.index.isin(market.index)
    frame["signal_amount_positive"] = (
        pd.to_numeric(market["amount_rmb"], errors="coerce").reindex(frame.index).fillna(0).gt(0)
    )
    st_values = {str(value).strip() for value in st_tickers if str(value).strip()}
    frame["is_st"] = frame.index.isin(st_values)
    listed_positions = np.searchsorted(
        np.asarray(sessions, dtype="datetime64[ns]"),
        active["list_date"].to_numpy(dtype="datetime64[ns]"),
        side="left",
    )
    frame["listing_session_age"] = signal_index - listed_positions + 1
    aligned_close = close.reindex(frame.index)
    aligned_amount = amount.reindex(frame.index)
    current_close = pd.to_numeric(market["close_adj"], errors="coerce").reindex(
        frame.index
    )
    current_amount = pd.to_numeric(market["amount_rmb"], errors="coerce").reindex(
        frame.index
    )
    observed = frame["has_signal_bar"]
    close_matches = np.isclose(
        aligned_close.iloc[:, -1].loc[observed].to_numpy(dtype=float),
        current_close.loc[observed].to_numpy(dtype=float),
        rtol=0.0,
        atol=0.0,
        equal_nan=True,
    )
    amount_matches = np.isclose(
        aligned_amount.iloc[:, -1].loc[observed].to_numpy(dtype=float),
        current_amount.loc[observed].to_numpy(dtype=float),
        rtol=0.0,
        atol=0.0,
        equal_nan=True,
    )
    if not close_matches.all() or not amount_matches.all():
        raise PITStockContractError(
            "signal-day market differs from the final history observation"
        )
    frame["complete_253_session_history"] = aligned_close.notna().all(axis=1)
    frame["complete_20_session_amount"] = aligned_amount.notna().all(axis=1)
    frame["mom12"] = (
        aligned_close[expected_close_dates[-config.end_lag - 1]]
        / aligned_close[expected_close_dates[0]]
        - 1.0
    )
    short_start = expected_close_dates[-config.short_start_lag - 1]
    frame["mom6"] = (
        aligned_close[expected_close_dates[-config.end_lag - 1]]
        / aligned_close[short_start]
        - 1.0
    )
    recent = aligned_close.iloc[:, -config.volatility_sessions - 1 :]
    frame["vol63"] = np.log(recent).diff(axis=1).std(axis=1, ddof=1) * np.sqrt(252.0)
    frame["adv20"] = aligned_amount.mean(axis=1)
    factor_columns = ["mom12", "mom6", "vol63", "adv20"]
    factors_finite = np.isfinite(frame[factor_columns].to_numpy(dtype=float)).all(axis=1)
    frame["base_eligible"] = (
        frame["has_signal_bar"]
        & frame["signal_amount_positive"]
        & ~frame["is_st"]
        & frame["listing_session_age"].ge(config.minimum_listing_sessions)
        & frame["complete_253_session_history"]
        & frame["complete_20_session_amount"]
        & factors_finite
        & frame["adv20"].gt(0)
        & frame["vol63"].ge(0)
    )
    eligible = frame.loc[frame["base_eligible"]].copy()
    if len(eligible) < config.universe_size:
        raise PITStockContractError(
            f"only {len(eligible)} base-eligible stocks; need {config.universe_size}"
        )
    ranked = eligible.assign(_ticker=eligible.index).sort_values(
        ["adv20", "_ticker"], ascending=[False, True], kind="mergesort"
    )
    members = ranked.index[: config.universe_size]
    frame["universe_member"] = frame.index.isin(members)
    frame["size_bucket"] = "OUTSIDE_UNIVERSE"
    frame.loc[members, "size_bucket"] = _size_buckets(
        frame.loc[members, "circ_mv"]
    ).astype(str)

    def reason(row: pd.Series) -> str:
        if not bool(row["has_signal_bar"]):
            return "missing_signal_bar"
        if not bool(row["signal_amount_positive"]):
            return "nonpositive_signal_amount"
        if bool(row["is_st"]):
            return "st_on_signal"
        if int(row["listing_session_age"]) < config.minimum_listing_sessions:
            return "insufficient_listing_age"
        if not bool(row["complete_253_session_history"]):
            return "incomplete_253_session_history"
        if not bool(row["complete_20_session_amount"]):
            return "incomplete_20_session_amount"
        if not bool(row["base_eligible"]):
            return "invalid_factor_or_liquidity"
        if not bool(row["universe_member"]):
            return "outside_top_adv_universe"
        return "included"

    frame["exclusion_reason"] = frame.apply(reason, axis=1)
    frame = frame.reset_index().sort_values("ticker", kind="mergesort").reset_index(drop=True)
    if frame["universe_member"].sum() != config.universe_size:
        raise PITStockContractError("universe membership count changed during assembly")
    return frame


class PITStockRawStore:
    """Read-only, explicit-cutoff adapter over the repository raw partitions."""

    def __init__(
        self,
        project_root: str | Path,
        *,
        maximum_read_date: Any,
        calendar_through_date: Any | None = None,
        raw_root: str | Path | None = None,
        verify_hashes: bool = True,
    ) -> None:
        self.project_root = Path(project_root).resolve()
        self.raw_root = (
            Path(raw_root).resolve()
            if raw_root is not None
            else (self.project_root / "runtime" / "data" / "raw").resolve()
        )
        self.maximum_read_date = _date(maximum_read_date, field="maximum_read_date")
        self.calendar_through_date = _date(
            calendar_through_date
            if calendar_through_date is not None
            else maximum_read_date,
            field="calendar_through_date",
        )
        if self.calendar_through_date < self.maximum_read_date:
            raise PITStockContractError(
                "calendar_through_date cannot precede maximum_read_date"
            )
        if not self.raw_root.is_dir():
            raise FileNotFoundError(self.raw_root)
        self.verify_hashes = bool(verify_hashes)
        self._verified_files: dict[Path, str] = {}
        self._alias_collision_count = 0
        self._checkpoint_path = self.raw_root / "checkpoint.json"
        self._stock_st_checkpoint_path = self.raw_root / "stock-st-checkpoint.json"
        self._enrichment_checkpoint_path = self.raw_root / "enrichment-checkpoint.json"
        self._checkpoint = self._read_checkpoint(
            self._checkpoint_path, role="market checkpoint"
        )
        self._stock_st_checkpoint = self._read_checkpoint(
            self._stock_st_checkpoint_path, role="stock_st checkpoint"
        )
        self._enrichment_checkpoint = self._read_checkpoint(
            self._enrichment_checkpoint_path, role="enrichment checkpoint"
        )
        self._aliases = self._load_aliases()
        self._master_path, self._master, self._master_receipt = self._load_master()
        (
            self._sessions,
            self._market_sessions,
            self._calendar_receipts,
        ) = self._load_sessions()

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _read_checkpoint(self, path: Path, *, role: str) -> dict[str, Any]:
        if not path.is_file():
            raise FileNotFoundError(path)
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PITStockContractError(f"{role} is not valid JSON") from exc
        if not isinstance(value, dict) or value.get("schema_version") != 1:
            raise PITStockContractError(f"{role} schema is unsupported")
        if not isinstance(value.get("partitions"), dict):
            raise PITStockContractError(f"{role} lacks partitions")
        return value

    def _verified_artifact(
        self,
        path_value: Any,
        *,
        expected_sha256: Any,
        expected_size: Any | None = None,
        role: str,
    ) -> Path:
        path = Path(str(path_value)).resolve()
        try:
            path.relative_to(self.raw_root)
        except ValueError as exc:
            raise PITStockContractError(f"{role} escapes the raw root") from exc
        if not path.is_file():
            raise FileNotFoundError(path)
        if expected_size is not None and path.stat().st_size != int(expected_size):
            raise PITStockContractError(f"{role} size differs from checkpoint")
        expected = str(expected_sha256 or "").strip().lower()
        if len(expected) != 64:
            raise PITStockContractError(f"{role} checkpoint SHA-256 is invalid")
        if self.verify_hashes:
            actual = self._verified_files.get(path)
            if actual is None:
                actual = self._sha256_file(path)
                self._verified_files[path] = actual
            if actual != expected:
                raise PITStockContractError(f"{role} SHA-256 differs from checkpoint")
        return path

    def _load_aliases(self) -> tuple[dict[str, Any], ...]:
        self._alias_config_path = self.project_root / "configs" / "data.json"
        value = json.loads(self._alias_config_path.read_text(encoding="utf-8"))
        self._alias_config_sha256 = self._sha256_file(self._alias_config_path)
        rows = []
        for raw in value.get("enrichment", {}).get("security_code_aliases", ()):
            row = dict(raw)
            required = {
                "canonical_ts_code",
                "vendor_ts_code",
                "effective_from",
                "effective_to",
                "source",
            }
            if not required.issubset(row):
                raise PITStockContractError("alias config lacks explicit identity interval")
            row["effective_from"] = _date(row["effective_from"], field="alias start")
            row["effective_to"] = _date(row["effective_to"], field="alias end")
            if (
                row["effective_from"] > row["effective_to"]
                or str(row["canonical_ts_code"]) == str(row["vendor_ts_code"])
                or not str(row["source"]).strip()
            ):
                raise PITStockContractError("alias config contains an invalid interval")
            rows.append(row)
        rows.sort(
            key=lambda row: (
                str(row["vendor_ts_code"]),
                row["effective_from"],
                row["effective_to"],
                str(row["canonical_ts_code"]),
            )
        )
        for index, left in enumerate(rows):
            for right in rows[index + 1 :]:
                if right["vendor_ts_code"] != left["vendor_ts_code"]:
                    break
                if right["effective_from"] <= left["effective_to"]:
                    raise PITStockContractError(
                        f"overlapping alias intervals for {left['vendor_ts_code']}"
                    )
        return tuple(rows)

    def _load_master(self) -> tuple[Path, pd.DataFrame, dict[str, Any]]:
        layout = RuntimeLayout.from_config(
            config_path=self._alias_config_path,
            repo_root=self.project_root,
        )
        semantic_audit = audit_security_master(layout)
        if semantic_audit.get("status") != "pass":
            raise PITStockContractError(
                f"security-master semantic audit failed: {semantic_audit.get('issues')}"
            )
        checkpoint_path = (
            self.raw_root
            / "reference"
            / "stock_basic"
            / "security-master-checkpoint.json"
        )
        if not checkpoint_path.is_file():
            raise FileNotFoundError(checkpoint_path)
        value = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        current = str(value.get("current_snapshot_sha256") or "")
        entry = (value.get("snapshots") or {}).get(current)
        if (
            value.get("schema_version") != 1
            or not isinstance(entry, dict)
            or entry.get("status") != "complete"
            or entry.get("snapshot_sha256") != current
        ):
            raise PITStockContractError("security-master checkpoint is incomplete")
        parquet = self._verified_artifact(
            entry.get("parquet_path"),
            expected_sha256=entry.get("parquet_sha256"),
            expected_size=entry.get("parquet_size_bytes"),
            role="security-master parquet",
        )
        manifest = self._verified_artifact(
            entry.get("manifest_path"),
            expected_sha256=entry.get("manifest_sha256"),
            role="security-master manifest",
        )
        frame = pd.read_parquet(parquet)
        if len(frame) != int(entry.get("row_count", -1)):
            raise PITStockContractError("security-master row count differs")
        receipt = {
            "checkpoint_path": str(checkpoint_path.resolve()),
            "checkpoint_sha256": self._sha256_file(checkpoint_path),
            "snapshot_sha256": current,
            "parquet_path": str(parquet),
            "parquet_sha256": str(entry["parquet_sha256"]),
            "manifest_path": str(manifest),
            "manifest_sha256": str(entry["manifest_sha256"]),
            "row_count": len(frame),
            "captured_at_utc": entry.get("completed_at_utc"),
            "semantic_audit": semantic_audit,
        }
        return parquet, frame, receipt

    def _load_sessions(
        self,
    ) -> tuple[tuple[pd.Timestamp, ...], tuple[pd.Timestamp, ...], tuple[dict[str, Any], ...]]:
        daily_entries = {
            _date(key.split("/", 1)[1], field="daily checkpoint date"): entry
            for key, entry in self._checkpoint["partitions"].items()
            if key.startswith("daily/")
            and isinstance(entry, dict)
            and entry.get("status") == "complete"
        }
        daily_dates = sorted(
            date for date in daily_entries if date <= self.maximum_read_date
        )
        if not daily_dates or daily_dates[-1] != self.maximum_read_date:
            raise PITStockContractError(
                "maximum_read_date must be a complete checkpointed daily partition"
            )
        first_market_date = daily_dates[0]
        calendar_rows: dict[pd.Timestamp, tuple[bool, pd.Timestamp]] = {}
        receipts: list[dict[str, Any]] = []
        for key, entry in sorted((self._checkpoint.get("calendars") or {}).items()):
            if (
                not isinstance(entry, dict)
                or entry.get("status") != "complete"
                or entry.get("exchange") != "SSE"
                or _date(entry.get("start_date"), field="calendar start")
                > self.calendar_through_date
                or _date(entry.get("end_date"), field="calendar end")
                < first_market_date
            ):
                continue
            path = self._verified_artifact(
                entry.get("path"),
                expected_sha256=entry.get("artifact_sha256"),
                role=f"trade_cal {key} parquet",
            )
            manifest = self._verified_artifact(
                entry.get("manifest_path"),
                expected_sha256=entry.get("manifest_sha256"),
                role=f"trade_cal {key} manifest",
            )
            frame = pd.read_parquet(path)
            required = {"exchange", "cal_date", "is_open", "pretrade_date"}
            if set(frame.columns) != required or len(frame) != int(entry.get("row_count", -1)):
                raise PITStockContractError("trade_cal schema/row count differs")
            dates = _date_series(frame["cal_date"])
            pretrade = _date_series(frame["pretrade_date"])
            if dates.isna().any() or pretrade.isna().any() or frame["exchange"].ne("SSE").any():
                raise PITStockContractError("trade_cal contains invalid values")
            if dates.duplicated().any():
                raise PITStockContractError("trade_cal artifact contains duplicate dates")
            if pd.api.types.is_bool_dtype(frame["is_open"].dtype):
                open_flags = frame["is_open"].astype(bool)
            else:
                numeric_flags = pd.to_numeric(frame["is_open"], errors="coerce")
                if numeric_flags.isna().any() or not numeric_flags.isin([0, 1]).all():
                    raise PITStockContractError("trade_cal is_open is not strict 0/1")
                open_flags = numeric_flags.astype(bool)
            for date, is_open, previous in zip(dates, open_flags, pretrade):
                date = pd.Timestamp(date)
                if date < first_market_date or date > self.calendar_through_date:
                    continue
                value = (bool(is_open), pd.Timestamp(previous))
                if date in calendar_rows and calendar_rows[date] != value:
                    raise PITStockContractError("overlapping trade_cal artifacts disagree")
                calendar_rows[date] = value
            receipts.append(
                {
                    "calendar_content_sha256": key,
                    "parquet_path": str(path),
                    "parquet_sha256": str(entry["artifact_sha256"]),
                    "manifest_path": str(manifest),
                    "manifest_sha256": str(entry["manifest_sha256"]),
                    "start_date": entry["start_date"],
                    "end_date": entry["end_date"],
                }
            )
        expected_calendar_days = tuple(
            pd.date_range(
                first_market_date, self.calendar_through_date, freq="D"
            ).normalize()
        )
        actual_calendar_days = tuple(sorted(calendar_rows))
        if actual_calendar_days != expected_calendar_days:
            missing = sorted(set(expected_calendar_days) - set(actual_calendar_days))
            extra = sorted(set(actual_calendar_days) - set(expected_calendar_days))
            raise PITStockContractError(
                f"trade_cal does not exactly cover calendar_through_date: missing={missing[:3]}, extra={extra[:3]}"
            )
        sessions = tuple(
            date for date, value in sorted(calendar_rows.items()) if value[0]
        )
        if not sessions or sessions[-1] < self.maximum_read_date:
            raise PITStockContractError("official trade_cal does not cover market cutoff")
        market_sessions = tuple(
            date for date in sessions if date <= self.maximum_read_date
        )
        if tuple(daily_dates) != market_sessions:
            missing = sorted(set(market_sessions) - set(daily_dates))
            extra = sorted(set(daily_dates) - set(market_sessions))
            raise PITStockContractError(
                f"daily partitions differ from official sessions: missing={missing[:3]}, extra={extra[:3]}"
            )
        for dataset in ("daily", "daily_basic", "adj_factor"):
            missing = [
                date
                for date in market_sessions
                if not isinstance(
                    self._checkpoint["partitions"].get(
                        f"{dataset}/{date.date().isoformat()}"
                    ),
                    dict,
                )
                or self._checkpoint["partitions"][
                    f"{dataset}/{date.date().isoformat()}"
                ].get("status")
                != "complete"
            ]
            if missing:
                raise PITStockContractError(
                    f"{dataset} misses official sessions: {missing[:3]}"
                )
        return sessions, market_sessions, tuple(receipts)

    @property
    def sessions(self) -> tuple[pd.Timestamp, ...]:
        return self._sessions

    @property
    def market_sessions(self) -> tuple[pd.Timestamp, ...]:
        return self._market_sessions

    @property
    def security_master(self) -> pd.DataFrame:
        return self._master.copy()

    def canonical_ticker(self, ticker: str, value: Any) -> str:
        date = _date(value, field="canonical ticker date")
        code = str(ticker).strip()
        matches = {
            str(row["canonical_ts_code"])
            for row in self._aliases
            if str(row["vendor_ts_code"]) == code
            and _date(row["effective_from"], field="alias start") <= date
            <= _date(row["effective_to"], field="alias end")
        }
        if len(matches) > 1:
            raise PITStockContractError(f"ambiguous alias for {code} on {date.date()}")
        return next(iter(matches), code)

    def _require_date(self, value: Any) -> pd.Timestamp:
        date = _date(value, field="partition date")
        if date > self.maximum_read_date:
            raise PITStockContractError(
                f"refusing to read {date.date()} after frozen cutoff {self.maximum_read_date.date()}"
            )
        if date not in self._market_sessions:
            raise PITStockContractError(
                f"{date.date()} is not a complete market-data session"
            )
        return date

    def _partition(
        self,
        dataset: str,
        date: pd.Timestamp,
        *,
        checkpoint: Mapping[str, Any] | None = None,
        key: str | None = None,
    ) -> tuple[Path, Mapping[str, Any]]:
        source = checkpoint if checkpoint is not None else self._checkpoint
        partition_key = key or f"{dataset}/{date.date().isoformat()}"
        entry = (source.get("partitions") or {}).get(partition_key)
        if (
            not isinstance(entry, Mapping)
            or entry.get("status") != "complete"
            or entry.get("dataset") != dataset
            or _date(entry.get("trade_date"), field=f"{dataset} checkpoint date")
            != date
        ):
            raise PITStockContractError(
                f"{dataset} checkpoint entry is missing/incomplete for {date.date()}"
            )
        path = self._verified_artifact(
            entry.get("path"),
            expected_sha256=entry.get("sha256"),
            expected_size=entry.get("size_bytes"),
            role=f"{dataset}/{date.date()} parquet",
        )
        return path, entry

    @staticmethod
    def _validate_partition_date(
        frame: pd.DataFrame, date: pd.Timestamp, *, role: str
    ) -> None:
        if "trade_date" not in frame:
            raise PITStockContractError(f"{role} lacks trade_date")
        values = _date_series(frame["trade_date"])
        if values.isna().any() or not values.eq(date).all():
            raise PITStockContractError(f"{role} contains rows from another date")

    def _canonicalize(self, frame: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
        work = frame.copy()
        if work["ts_code"].astype("string").str.strip().duplicated().any():
            raise PITStockContractError("provider partition contains duplicate ts_code")
        active: dict[str, str] = {}
        for row in self._aliases:
            if _date(row["effective_from"], field="alias start") <= date <= _date(
                row["effective_to"], field="alias end"
            ):
                active[str(row["vendor_ts_code"])] = str(row["canonical_ts_code"])
        work["_source_code"] = work["ts_code"].astype("string").str.strip()
        work["ts_code"] = work["_source_code"].replace(active)
        work["_priority"] = (~work["_source_code"].isin(active)).astype(int)
        duplicate = work.duplicated("ts_code", keep=False)
        if duplicate.any():
            for canonical, group in work.loc[duplicate].groupby(
                "ts_code", sort=False
            ):
                sources = set(group["_source_code"].astype(str))
                expected_vendor = {
                    vendor for vendor, target in active.items() if target == canonical
                }
                if not expected_vendor or not sources.issubset(
                    expected_vendor | {str(canonical)}
                ) or not (sources & expected_vendor):
                    raise PITStockContractError(
                        f"unexplained canonical alias collision for {canonical}"
                    )
                self._alias_collision_count += 1
        work = work.sort_values(
            ["ts_code", "_priority", "_source_code"], kind="mergesort"
        ).drop_duplicates("ts_code", keep="first")
        return work.drop(columns=["_source_code", "_priority"])

    def read_market(self, value: Any) -> pd.DataFrame:
        date = self._require_date(value)
        daily_path, daily_entry = self._partition("daily", date)
        factor_path, factor_entry = self._partition("adj_factor", date)
        daily = pd.read_parquet(
            daily_path,
            columns=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "amount",
            ],
        )
        factors = pd.read_parquet(
            factor_path, columns=["ts_code", "trade_date", "adj_factor"]
        )
        if len(daily) != int(daily_entry.get("row_count", -1)) or len(factors) != int(
            factor_entry.get("row_count", -1)
        ):
            raise PITStockContractError("market partition row count differs")
        self._validate_partition_date(daily, date, role="daily")
        self._validate_partition_date(factors, date, role="adj_factor")
        daily = daily.drop(columns="trade_date")
        factors = factors.drop(columns="trade_date")
        daily = self._canonicalize(daily, date)
        factors = self._canonicalize(factors, date)
        daily = daily.loc[
            daily["ts_code"].astype("string").str.endswith((".SH", ".SZ"))
        ].copy()
        factors = factors.loc[
            factors["ts_code"].astype("string").str.endswith((".SH", ".SZ"))
        ].copy()
        work = daily.merge(factors, on="ts_code", how="left", validate="one_to_one")
        numeric = ["open", "high", "low", "close", "pre_close", "amount", "adj_factor"]
        work[numeric] = work[numeric].apply(pd.to_numeric, errors="coerce")
        if not np.isfinite(work[numeric].to_numpy(dtype=float)).all():
            raise PITStockContractError(f"{date.date()} market contains non-finite values")
        if work["amount"].lt(0).any() or work["adj_factor"].le(0).any():
            raise PITStockContractError(f"{date.date()} market amount/factor is invalid")
        work["open_adj"] = work["open"] * work["adj_factor"]
        work["close_adj"] = work["close"] * work["adj_factor"]
        work["amount_rmb"] = work["amount"] * 1000.0
        return work.sort_values("ts_code", kind="mergesort").reset_index(drop=True)

    def read_daily_basic(self, value: Any) -> pd.DataFrame:
        date = self._require_date(value)
        path, entry = self._partition("daily_basic", date)
        frame = pd.read_parquet(
            path,
            columns=["ts_code", "trade_date", "total_mv", "circ_mv"],
        )
        if len(frame) != int(entry.get("row_count", -1)):
            raise PITStockContractError("daily_basic partition row count differs")
        self._validate_partition_date(frame, date, role="daily_basic")
        frame = frame.drop(columns="trade_date")
        frame = self._canonicalize(frame, date)
        frame = frame.loc[
            frame["ts_code"].astype("string").str.endswith((".SH", ".SZ"))
        ].copy()
        return frame.sort_values(
            "ts_code", kind="mergesort"
        ).reset_index(drop=True)

    def read_stock_st(self, value: Any) -> set[str]:
        date = self._require_date(value)
        path, entry = self._partition(
            "stock_st",
            date,
            checkpoint=self._stock_st_checkpoint,
            key=f"stock_st/trade_date={date.date().isoformat()}",
        )
        frame = pd.read_parquet(path, columns=["ts_code", "trade_date"])
        if len(frame) != int(entry.get("row_count", -1)):
            raise PITStockContractError("stock_st partition row count differs")
        self._validate_partition_date(frame, date, role="stock_st")
        frame = self._canonicalize(frame.drop(columns="trade_date"), date)
        return set(frame["ts_code"].astype(str))

    def read_industry(self, value: Any) -> tuple[pd.Timestamp, pd.Series]:
        date = self._require_date(value)
        candidates: list[tuple[pd.Timestamp, str]] = []
        for key, entry in self._enrichment_checkpoint["partitions"].items():
            if not key.startswith("bak_basic/trade_date=") or not isinstance(entry, Mapping):
                continue
            source = _date(key.split("=", 1)[1], field="bak_basic checkpoint date")
            if source <= date:
                candidates.append((source, key))
        if not candidates:
            raise PITStockContractError(f"no PIT industry snapshot for {date.date()}")
        source, key = max(candidates, key=lambda item: item[0])
        if source.to_period("Q") != date.to_period("Q"):
            raise PITStockContractError("latest industry snapshot is not in signal quarter")
        path, entry = self._partition(
            "bak_basic",
            source,
            checkpoint=self._enrichment_checkpoint,
            key=key,
        )
        raw = pd.read_parquet(path, columns=["ts_code", "trade_date", "industry"])
        if len(raw) != int(entry.get("row_count", -1)):
            raise PITStockContractError("bak_basic partition row count differs")
        self._validate_partition_date(raw, source, role="bak_basic")
        frame = self._canonicalize(
            raw.drop(columns="trade_date"), source
        )
        series = frame.set_index("ts_code")["industry"].astype("string")
        return source, series

    def source_receipt(self) -> dict[str, Any]:
        partition_contract = []
        for dataset in ("daily", "daily_basic", "adj_factor"):
            for date in self._market_sessions:
                entry = self._checkpoint["partitions"][
                    f"{dataset}/{date.date().isoformat()}"
                ]
                partition_contract.append(
                    {
                        "dataset": dataset,
                        "trade_date": str(date.date()),
                        "sha256": entry["sha256"],
                        "size_bytes": entry["size_bytes"],
                        "row_count": entry["row_count"],
                    }
                )
        payload = {
            "raw_root": str(self.raw_root),
            "maximum_read_date": str(self.maximum_read_date.date()),
            "calendar_through_date": str(self.calendar_through_date.date()),
            "official_session_count": len(self.sessions),
            "market_session_count_through_cutoff": len(self.market_sessions),
            "first_market_session": str(self.market_sessions[0].date()),
            "market_checkpoint_path": str(self._checkpoint_path.resolve()),
            "market_checkpoint_sha256": self._sha256_file(self._checkpoint_path),
            "stock_st_checkpoint_path": str(self._stock_st_checkpoint_path.resolve()),
            "stock_st_checkpoint_sha256": self._sha256_file(
                self._stock_st_checkpoint_path
            ),
            "enrichment_checkpoint_path": str(
                self._enrichment_checkpoint_path.resolve()
            ),
            "enrichment_checkpoint_sha256": self._sha256_file(
                self._enrichment_checkpoint_path
            ),
            "market_partition_contract_sha256": canonical_sha256(
                partition_contract
            ),
            "calendar_artifacts": list(self._calendar_receipts),
            "security_master": self._master_receipt,
            "alias_config_path": str(self._alias_config_path.resolve()),
            "alias_config_sha256": self._alias_config_sha256,
            "alias_collision_policy": "interval-authoritative vendor row wins; explained collisions counted",
            "explained_alias_collision_partition_count": self._alias_collision_count,
            "historical_vintage_class": "reconstructed_effective_date_pit_from_2026_provider_capture",
            "contemporaneous_historical_vendor_vintage_verified": False,
        }
        payload["payload_sha256"] = canonical_sha256(payload)
        return payload

    def source_allowlist(self) -> dict[str, Any]:
        """Freeze exact checkpoint entries so later checkpoint appends cannot rewrite a run."""

        def compact(entry: Mapping[str, Any]) -> dict[str, Any]:
            return {
                key: entry.get(key)
                for key in (
                    "dataset",
                    "trade_date",
                    "path",
                    "sha256",
                    "size_bytes",
                    "row_count",
                    "status",
                    "completed_at_utc",
                )
            }

        market = []
        for dataset in ("daily", "daily_basic", "adj_factor"):
            for date in self._market_sessions:
                market.append(
                    compact(
                        self._checkpoint["partitions"][
                            f"{dataset}/{date.date().isoformat()}"
                        ]
                    )
                )
        stock_st = []
        for key, entry in sorted(self._stock_st_checkpoint["partitions"].items()):
            if not key.startswith("stock_st/trade_date=") or not isinstance(
                entry, Mapping
            ):
                continue
            date = _date(entry.get("trade_date"), field="stock_st allowlist date")
            if date <= self.maximum_read_date:
                stock_st.append(compact(entry))
        industry = []
        for key, entry in sorted(self._enrichment_checkpoint["partitions"].items()):
            if not key.startswith("bak_basic/trade_date=") or not isinstance(
                entry, Mapping
            ):
                continue
            date = _date(entry.get("trade_date"), field="bak_basic allowlist date")
            if date <= self.maximum_read_date:
                industry.append(compact(entry))
        value: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_12_0_exact_cutoff_source_allowlist",
            "maximum_market_date": str(self.maximum_read_date.date()),
            "calendar_through_date": str(self.calendar_through_date.date()),
            "market_partitions": market,
            "stock_st_partitions": stock_st,
            "bak_basic_partitions": industry,
            "calendar_artifacts": list(self._calendar_receipts),
            "security_master": self._master_receipt,
            "alias_config": {
                "path": str(self._alias_config_path.resolve()),
                "sha256": self._alias_config_sha256,
            },
        }
        value["payload_sha256"] = canonical_sha256(value)
        return value


def build_quarterly_panel(
    store: PITStockRawStore,
    *,
    first_signal: Any,
    last_signal: Any,
    config: PITStockStrategyConfig = PITStockStrategyConfig(),
) -> PITPanelBuild:
    first = _date(first_signal, field="first_signal")
    last = _date(last_signal, field="last_signal")
    quarter_ends = official_quarter_end_sessions(
        store.sessions, calendar_through_date=store.calendar_through_date
    )
    signals = tuple(value for value in quarter_ends if first <= value <= last)
    if not signals or signals[0] != first or signals[-1] != last:
        raise PITStockContractError("first/last signal must be exact official quarter ends")
    first_index = store.sessions.index(first)
    if first_index < config.long_start_lag:
        raise PITStockContractError("raw source lacks the first signal lookback")
    close_history: deque[pd.Series] = deque(maxlen=config.long_start_lag + 1)
    amount_history: deque[pd.Series] = deque(maxlen=20)
    rows: list[pd.DataFrame] = []
    start_index = first_index - config.long_start_lag
    signal_set = set(signals)
    for date in store.sessions[start_index : store.sessions.index(last) + 1]:
        market = store.read_market(date)
        indexed = market.set_index("ts_code")
        close_history.append(indexed["close_adj"].rename(date))
        amount_history.append(indexed["amount_rmb"].rename(date))
        if date not in signal_set:
            continue
        source_date, industry = store.read_industry(date)
        snapshot = build_pit_snapshot(
            signal_date=date,
            official_sessions=store.sessions,
            security_master=store.security_master,
            close_history=pd.concat(list(close_history), axis=1),
            amount_history=pd.concat(list(amount_history), axis=1),
            current_market=market,
            daily_basic=store.read_daily_basic(date),
            st_tickers=store.read_stock_st(date),
            industry=industry,
            industry_source_date=source_date,
            config=config,
        )
        rows.append(snapshot)
    panel = pd.concat(rows, ignore_index=True)
    identity_columns = [
        "ticker",
        "signal_date",
        "universe_member",
        "mom12",
        "mom6",
        "vol63",
        "adv20",
        "industry",
        "industry_source_date",
        "size_bucket",
        "exclusion_reason",
    ]
    identity = panel[identity_columns].copy()
    for column in ("signal_date", "industry_source_date"):
        identity[column] = pd.to_datetime(identity[column]).dt.strftime("%Y-%m-%d")
    identity = identity.sort_values(["signal_date", "ticker"], kind="mergesort")
    identity = identity.astype(object).where(pd.notna(identity), None)
    digest = canonical_sha256(identity.to_dict("records"))
    return PITPanelBuild(
        panel=panel,
        signal_dates=tuple(str(value.date()) for value in signals),
        maximum_read_date=str(store.maximum_read_date.date()),
        source_receipt=store.source_receipt(),
        panel_payload_sha256=digest,
    )


__all__ = [
    "PITPanelBuild",
    "PITStockRawStore",
    "build_pit_snapshot",
    "build_quarterly_panel",
]
