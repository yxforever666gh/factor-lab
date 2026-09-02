"""Deterministic 13.0 minute-capture scopes.

The two scopes are deliberately derived without reading minute data or a
stage-1 result.  Stage 1 follows every candidate name ever targeted through
each raw-open boundary.  Stage 2 follows the corresponding cumulative union
of candidate and mechanically generated ADV500 names.  An effective delist
date removes a name beginning on that date, and the final boundary is retained
as a mark-only sentinel.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

STAGE1_RECORD_FIELDS = (
    "signal_date",
    "execution_date",
    "ticker",
    "in_previous_target",
    "in_current_target",
    "in_cumulative_target",
    "mark_only",
)
STAGE2_RECORD_FIELDS = (
    "signal_date",
    "execution_date",
    "ticker",
    "in_candidate_target",
    "in_adv500_target",
    "in_cumulative_all_role_scope",
    "mark_only",
)
PAIR_SORT_FIELDS = ("execution_date", "ticker")

FORMAL_SIGNAL_COUNT = 22
FORMAL_STAGE1_PAIR_COUNT = 4_729
FORMAL_STAGE1_UNIQUE_TICKER_COUNT = 501
FORMAL_STAGE1_PAYLOAD_SHA256 = (
    "5860e7321107e4fa92be044d0fd027835ac650fa0f426e5c3a76d6edb45567e4"
)
FORMAL_STAGE2_PAIR_COUNT = 33_984
FORMAL_STAGE2_UNIQUE_TICKER_COUNT = 2_252
FORMAL_STAGE2_PAYLOAD_SHA256 = (
    "520bb9267bcdf6e0ee45c2724a5fb31389cd96cfe1ccc1115fa23535ffb1aa2e"
)

PANEL_RELATIVE_PATH = Path(
    "runtime/data/pit-stock-12.0/development/quarterly-snapshots.parquet"
)
TARGETS_RELATIVE_PATH = Path(
    "runtime/data/pit-stock-12.0/development/targets.parquet"
)
PANEL_FILE_SHA256 = (
    "d51cad0de60484292ca24e4909d2c7617e0d83d3b8df58b29358f438eb5a48ca"
)
TARGETS_FILE_SHA256 = (
    "ce3cd6c37dd1b04e77c170055e1bb7351cd99b6e2a201e7745ac1cf29a9ea06c"
)
TARGETS_PAYLOAD_SHA256 = (
    "1022288372cd07f97b0e963b670c3a7e9ddfc94414b4d21cb7cbea9c643e76be"
)
DEVELOPMENT_MANIFEST_RELATIVE_PATH = Path(
    "runtime/data/pit-stock-12.0/development/manifest.json"
)
DEVELOPMENT_MANIFEST_FILE_SHA256 = (
    "82fbba704066d9779c73b8b6d90c3ce566447b96b7e1fcfcd1803525c3a09763"
)
DEVELOPMENT_MANIFEST_PAYLOAD_SHA256 = (
    "5b4815885c500656af0c597f1cf4b0030932b7ea82556a052a376b494f61a870"
)


class MinuteScopeError(ValueError):
    """Raised when inputs cannot prove one of the frozen capture scopes."""


@dataclass(frozen=True)
class MinutePairScope:
    """One immutable-by-contract ordered pair scope."""

    stage: str
    record_fields: tuple[str, ...]
    records: tuple[Mapping[str, Any], ...]
    payload_sha256: str
    signal_count: int
    unique_ticker_count: int

    @property
    def pair_count(self) -> int:
        return len(self.records)


@dataclass(frozen=True)
class DevelopmentMinuteScopes:
    """The mechanically related stage-1 and stage-2 development scopes."""

    stage1: MinutePairScope
    stage2: MinutePairScope


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping_payload_sha256(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("payload_sha256", None)
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _date(value: Any, *, field: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise MinuteScopeError(f"{field} is not a valid date")
    result = pd.Timestamp(parsed)
    if result.tzinfo is not None:
        result = result.tz_convert("Asia/Shanghai").tz_localize(None)
    return result.normalize()


def _ticker(value: Any) -> str:
    result = str(value).strip().upper()
    if not (
        len(result) == 9
        and result[:6].isdigit()
        and result[6:] in {".SH", ".SZ"}
    ):
        raise MinuteScopeError("scope contains a non-canonical A-share ticker")
    return result


def _signals(values: Iterable[Any]) -> tuple[pd.Timestamp, ...]:
    normalized = tuple(sorted(_date(value, field="signal_date") for value in values))
    if not normalized or len(set(normalized)) != len(normalized):
        raise MinuteScopeError("signal dates are empty or duplicate")
    return normalized


def _execution_map(
    signals: Sequence[pd.Timestamp], values: Mapping[Any, Any]
) -> dict[pd.Timestamp, pd.Timestamp]:
    result: dict[pd.Timestamp, pd.Timestamp] = {}
    for raw_signal, raw_execution in values.items():
        signal = _date(raw_signal, field="execution-map signal_date")
        if signal in result:
            raise MinuteScopeError("execution map aliases duplicate signal dates")
        execution = _date(raw_execution, field="execution_date")
        if execution <= signal:
            raise MinuteScopeError("execution date is not after its signal date")
        result[signal] = execution
    if set(result) != set(signals):
        raise MinuteScopeError("execution map does not cover the exact signals")
    if len(set(result.values())) != len(result):
        raise MinuteScopeError("execution dates are duplicate")
    return result


def _target_map(
    signals: Sequence[pd.Timestamp],
    values: Mapping[Any, Iterable[Any]],
    *,
    role: str,
) -> dict[pd.Timestamp, set[str]]:
    partial: dict[pd.Timestamp, set[str]] = {}
    for raw_signal, raw_tickers in values.items():
        signal = _date(raw_signal, field=f"{role} signal_date")
        if signal in partial:
            raise MinuteScopeError(f"{role} target map aliases a signal date")
        tickers = [_ticker(value) for value in raw_tickers]
        if len(tickers) != len(set(tickers)):
            raise MinuteScopeError(f"{role} target map contains duplicate tickers")
        partial[signal] = set(tickers)
    extra = set(partial) - set(signals)
    if extra:
        raise MinuteScopeError(f"{role} target map contains an extra signal")
    return {signal: set(partial.get(signal, set())) for signal in signals}


def _delist_map(values: Mapping[Any, Any]) -> dict[str, pd.Timestamp | None]:
    result: dict[str, pd.Timestamp | None] = {}
    for raw_ticker, raw_date in values.items():
        ticker = _ticker(raw_ticker)
        if ticker in result:
            raise MinuteScopeError("delist map aliases a ticker")
        result[ticker] = (
            None
            if raw_date is None or pd.isna(raw_date)
            else _date(raw_date, field="delist_date")
        )
    return result


def _active_on(
    ticker: str,
    execution_date: pd.Timestamp,
    delist_dates: Mapping[str, pd.Timestamp | None],
) -> bool:
    effective = delist_dates.get(ticker)
    return effective is None or execution_date < effective


def canonical_scope_sha256(
    records: Sequence[Mapping[str, Any]],
    *,
    record_fields: Sequence[str],
) -> str:
    """Hash ordered records with the protocol's exact JSON encoding."""

    expected = tuple(record_fields)
    for record in records:
        if tuple(record.keys()) != expected:
            raise MinuteScopeError("scope record fields or field order differ")
    encoded = json.dumps(
        list(records),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _finish_scope(
    *,
    stage: str,
    fields: tuple[str, ...],
    records: list[dict[str, Any]],
    signal_count: int,
) -> MinutePairScope:
    records.sort(key=lambda value: tuple(value[field] for field in PAIR_SORT_FIELDS))
    identities = [
        (value["signal_date"], value["execution_date"], value["ticker"])
        for value in records
    ]
    if len(identities) != len(set(identities)):
        raise MinuteScopeError("scope contains duplicate pair identities")
    if any(tuple(value.keys()) != fields for value in records):
        raise MinuteScopeError("generated scope record schema differs")
    payload = canonical_scope_sha256(records, record_fields=fields)
    return MinutePairScope(
        stage=stage,
        record_fields=fields,
        records=tuple(records),
        payload_sha256=payload,
        signal_count=signal_count,
        unique_ticker_count=len({value["ticker"] for value in records}),
    )


def generate_stage1_candidate_scope(
    *,
    signal_dates: Iterable[Any],
    execution_dates: Mapping[Any, Any],
    candidate_targets: Mapping[Any, Iterable[Any]],
    delist_dates: Mapping[Any, Any],
) -> MinutePairScope:
    """Generate the cumulative candidate scope through the mark-only sentinel."""

    signals = _signals(signal_dates)
    executions = _execution_map(signals, execution_dates)
    candidates = _target_map(signals, candidate_targets, role="candidate")
    delists = _delist_map(delist_dates)
    all_target_tickers = set().union(*candidates.values())
    missing_master = all_target_tickers - set(delists)
    if missing_master:
        raise MinuteScopeError("candidate target is absent from the delist map")

    previous: set[str] = set()
    cumulative: set[str] = set()
    records: list[dict[str, Any]] = []
    for signal in signals:
        current = candidates[signal]
        cumulative.update(current)
        execution = executions[signal]
        mark_only = signal == signals[-1]
        for ticker in sorted(cumulative):
            if not _active_on(ticker, execution, delists):
                continue
            records.append(
                {
                    "signal_date": signal.date().isoformat(),
                    "execution_date": execution.date().isoformat(),
                    "ticker": ticker,
                    "in_previous_target": ticker in previous,
                    "in_current_target": ticker in current,
                    "in_cumulative_target": ticker in cumulative,
                    "mark_only": mark_only,
                }
            )
        previous = set(current)
    return _finish_scope(
        stage="stage_1_candidate_only",
        fields=STAGE1_RECORD_FIELDS,
        records=records,
        signal_count=len(signals),
    )


def generate_stage2_all_roles_scope(
    *,
    signal_dates: Iterable[Any],
    execution_dates: Mapping[Any, Any],
    candidate_targets: Mapping[Any, Iterable[Any]],
    adv500_targets: Mapping[Any, Iterable[Any]],
    delist_dates: Mapping[Any, Any],
) -> MinutePairScope:
    """Generate the frozen cumulative candidate+ADV500 scope."""

    signals = _signals(signal_dates)
    executions = _execution_map(signals, execution_dates)
    candidates = _target_map(signals, candidate_targets, role="candidate")
    benchmarks = _target_map(signals, adv500_targets, role="ADV500")
    delists = _delist_map(delist_dates)
    all_target_tickers = set().union(*candidates.values(), *benchmarks.values())
    missing_master = all_target_tickers - set(delists)
    if missing_master:
        raise MinuteScopeError("candidate/ADV500 target is absent from the delist map")

    cumulative: set[str] = set()
    records: list[dict[str, Any]] = []
    for signal in signals:
        candidate = candidates[signal]
        benchmark = benchmarks[signal]
        cumulative.update(candidate | benchmark)
        execution = executions[signal]
        mark_only = signal == signals[-1]
        for ticker in sorted(cumulative):
            if not _active_on(ticker, execution, delists):
                continue
            records.append(
                {
                    "signal_date": signal.date().isoformat(),
                    "execution_date": execution.date().isoformat(),
                    "ticker": ticker,
                    "in_candidate_target": ticker in candidate,
                    "in_adv500_target": ticker in benchmark,
                    "in_cumulative_all_role_scope": ticker in cumulative,
                    "mark_only": mark_only,
                }
            )
    return _finish_scope(
        stage="stage_2_all_roles_only_after_stage_1_pass",
        fields=STAGE2_RECORD_FIELDS,
        records=records,
        signal_count=len(signals),
    )


def verify_stage1_overlap(stage1: MinutePairScope, stage2: MinutePairScope) -> None:
    """Prove every stage-1 pair is reused unchanged in stage 2."""

    if stage1.record_fields != STAGE1_RECORD_FIELDS:
        raise MinuteScopeError("stage-1 overlap schema differs")
    if stage2.record_fields != STAGE2_RECORD_FIELDS:
        raise MinuteScopeError("stage-2 overlap schema differs")
    stage2_index = {
        (row["signal_date"], row["execution_date"], row["ticker"]): row
        for row in stage2.records
    }
    if len(stage2_index) != len(stage2.records):
        raise MinuteScopeError("stage-2 overlap identities are duplicate")
    for left in stage1.records:
        identity = (
            left["signal_date"],
            left["execution_date"],
            left["ticker"],
        )
        right = stage2_index.get(identity)
        if right is None:
            raise MinuteScopeError("stage 2 omits a stage-1 pair")
        if (
            right["in_candidate_target"] != left["in_current_target"]
            or right["mark_only"] != left["mark_only"]
            or left["in_cumulative_target"] is not True
            or right["in_cumulative_all_role_scope"] is not True
        ):
            raise MinuteScopeError("stage-1/stage-2 overlap semantics differ")


def _target_payload(targets: pd.DataFrame) -> str:
    required = {"strategy_id", "signal_date", "ticker", "target_weight"}
    if not required.issubset(targets.columns):
        raise MinuteScopeError("12.0 targets lack identity columns")
    identity = targets.loc[:, sorted(required)].copy()
    identity = identity[
        ["strategy_id", "signal_date", "ticker", "target_weight"]
    ]
    identity["signal_date"] = pd.to_datetime(
        identity["signal_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    if identity["signal_date"].isna().any():
        raise MinuteScopeError("12.0 targets contain invalid signal dates")
    identity["ticker"] = identity["ticker"].astype(str)
    identity = identity.sort_values(["signal_date", "ticker"], kind="mergesort")
    return sha256(
        json.dumps(
            identity.to_dict("records"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def build_formal_development_scopes(project_root: Path) -> DevelopmentMinuteScopes:
    """Rebuild and verify both formal scopes from exact 12.0 artifacts."""

    root = Path(project_root).resolve()
    panel_path = root / PANEL_RELATIVE_PATH
    targets_path = root / TARGETS_RELATIVE_PATH
    manifest_path = root / DEVELOPMENT_MANIFEST_RELATIVE_PATH
    if (
        not panel_path.is_file()
        or _file_sha256(panel_path) != PANEL_FILE_SHA256
        or not targets_path.is_file()
        or _file_sha256(targets_path) != TARGETS_FILE_SHA256
        or not manifest_path.is_file()
        or _file_sha256(manifest_path) != DEVELOPMENT_MANIFEST_FILE_SHA256
    ):
        raise MinuteScopeError("exact 12.0 development file binding differs")
    development_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (
        not isinstance(development_manifest, dict)
        or development_manifest.get("payload_sha256")
        != DEVELOPMENT_MANIFEST_PAYLOAD_SHA256
        or _mapping_payload_sha256(development_manifest)
        != DEVELOPMENT_MANIFEST_PAYLOAD_SHA256
        or development_manifest.get("panel", {}).get("file_sha256")
        != PANEL_FILE_SHA256
        or development_manifest.get("targets", {}).get("file_sha256")
        != TARGETS_FILE_SHA256
        or development_manifest.get("targets", {}).get("payload_sha256")
        != TARGETS_PAYLOAD_SHA256
    ):
        raise MinuteScopeError("exact 12.0 development manifest differs")
    panel = pd.read_parquet(panel_path)
    targets = pd.read_parquet(targets_path)
    if _target_payload(targets) != TARGETS_PAYLOAD_SHA256:
        raise MinuteScopeError("exact 12.0 target payload differs")
    required_panel = {"signal_date", "ticker", "universe_member", "adv20"}
    if not required_panel.issubset(panel.columns):
        raise MinuteScopeError("12.0 panel lacks scope columns")
    panel = panel.copy()
    targets = targets.copy()
    panel["signal_date"] = pd.to_datetime(
        panel["signal_date"], errors="coerce"
    ).dt.normalize()
    targets["signal_date"] = pd.to_datetime(
        targets["signal_date"], errors="coerce"
    ).dt.normalize()
    if panel["signal_date"].isna().any() or targets["signal_date"].isna().any():
        raise MinuteScopeError("12.0 scope inputs contain invalid signal dates")
    signals = tuple(
        sorted(pd.Timestamp(value) for value in panel["signal_date"].unique())
    )
    if len(signals) != FORMAL_SIGNAL_COUNT:
        raise MinuteScopeError("formal development signal count differs")
    if set(targets["signal_date"]) - set(signals):
        raise MinuteScopeError("12.0 targets contain a signal outside the panel")
    if targets.duplicated(["signal_date", "ticker"]).any():
        raise MinuteScopeError("12.0 targets contain duplicate identities")

    source = development_manifest.get("source_receipt", {})
    calendar_entries = source.get("calendar_artifacts")
    if not isinstance(calendar_entries, list) or not calendar_entries:
        raise MinuteScopeError("12.0 lineage lacks official calendar artifacts")
    session_values: set[pd.Timestamp] = set()
    for entry in calendar_entries:
        if not isinstance(entry, dict):
            raise MinuteScopeError("12.0 calendar lineage entry differs")
        calendar_path = Path(str(entry.get("parquet_path")))
        if not calendar_path.is_absolute():
            calendar_path = root / calendar_path
        if (
            not calendar_path.is_file()
            or _file_sha256(calendar_path) != entry.get("parquet_sha256")
        ):
            raise MinuteScopeError("12.0 calendar lineage artifact differs")
        calendar = pd.read_parquet(calendar_path)
        if set(calendar.columns) != {
            "exchange",
            "cal_date",
            "is_open",
            "pretrade_date",
        } or calendar["exchange"].ne("SSE").any():
            raise MinuteScopeError("12.0 calendar lineage schema differs")
        dates = pd.to_datetime(calendar["cal_date"], errors="coerce").dt.normalize()
        if dates.isna().any() or dates.duplicated().any():
            raise MinuteScopeError("12.0 calendar lineage dates differ")
        if pd.api.types.is_bool_dtype(calendar["is_open"].dtype):
            open_flags = calendar["is_open"].astype(bool)
        else:
            numeric = pd.to_numeric(calendar["is_open"], errors="coerce")
            if numeric.isna().any() or not numeric.isin([0, 1]).all():
                raise MinuteScopeError("12.0 calendar open flags differ")
            open_flags = numeric.astype(bool)
        session_values.update(pd.Timestamp(value) for value in dates.loc[open_flags])
    sessions = tuple(sorted(session_values))
    session_index = {date: index for index, date in enumerate(sessions)}
    execution_dates: dict[pd.Timestamp, pd.Timestamp] = {}
    for signal in signals:
        index = session_index.get(signal)
        if index is None or index + 1 >= len(sessions):
            raise MinuteScopeError("signal lacks an exact next official session")
        execution_dates[signal] = sessions[index + 1]

    master_entry = source.get("security_master")
    if not isinstance(master_entry, dict):
        raise MinuteScopeError("12.0 lineage lacks a security master")
    master_path = Path(str(master_entry.get("parquet_path")))
    if not master_path.is_absolute():
        master_path = root / master_path
    if (
        not master_path.is_file()
        or _file_sha256(master_path) != master_entry.get("parquet_sha256")
    ):
        raise MinuteScopeError("12.0 security-master lineage artifact differs")
    master = pd.read_parquet(master_path)
    if not {"ts_code", "delist_date"}.issubset(master.columns):
        raise MinuteScopeError("security master lacks delist identity fields")
    master["ts_code"] = master["ts_code"].astype(str)
    master = master.loc[
        master["ts_code"].str.fullmatch(r"\d{6}\.(?:SH|SZ)", na=False)
    ].copy()
    if master["ts_code"].duplicated().any():
        raise MinuteScopeError("security master contains duplicate tickers")
    master["delist_date"] = pd.to_datetime(
        master["delist_date"], errors="coerce"
    ).dt.normalize()
    delist_dates = dict(
        master[["ts_code", "delist_date"]].itertuples(index=False, name=None)
    )

    candidate_targets = {
        signal: tuple(
            targets.loc[targets["signal_date"].eq(signal), "ticker"].astype(str)
        )
        for signal in signals
    }
    adv500_targets: dict[pd.Timestamp, tuple[str, ...]] = {}
    for signal, snapshot in panel.groupby("signal_date", sort=True):
        members = snapshot.loc[snapshot["universe_member"]].assign(
            _ticker=snapshot.loc[snapshot["universe_member"], "ticker"].astype(str)
        )
        selected = members.sort_values(
            ["adv20", "_ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(500)
        if len(selected) != 500 or selected["_ticker"].duplicated().any():
            raise MinuteScopeError("formal ADV500 target is incomplete")
        adv500_targets[pd.Timestamp(signal)] = tuple(selected["_ticker"])

    stage1 = generate_stage1_candidate_scope(
        signal_dates=signals,
        execution_dates=execution_dates,
        candidate_targets=candidate_targets,
        delist_dates=delist_dates,
    )
    stage2 = generate_stage2_all_roles_scope(
        signal_dates=signals,
        execution_dates=execution_dates,
        candidate_targets=candidate_targets,
        adv500_targets=adv500_targets,
        delist_dates=delist_dates,
    )
    if (
        stage1.pair_count != FORMAL_STAGE1_PAIR_COUNT
        or stage1.unique_ticker_count != FORMAL_STAGE1_UNIQUE_TICKER_COUNT
        or stage1.payload_sha256 != FORMAL_STAGE1_PAYLOAD_SHA256
    ):
        raise MinuteScopeError("formal stage-1 minute scope differs")
    if (
        stage2.pair_count != FORMAL_STAGE2_PAIR_COUNT
        or stage2.unique_ticker_count != FORMAL_STAGE2_UNIQUE_TICKER_COUNT
        or stage2.payload_sha256 != FORMAL_STAGE2_PAYLOAD_SHA256
    ):
        raise MinuteScopeError("formal stage-2 minute scope differs")
    verify_stage1_overlap(stage1, stage2)
    return DevelopmentMinuteScopes(stage1=stage1, stage2=stage2)


__all__ = [
    "DevelopmentMinuteScopes",
    "FORMAL_SIGNAL_COUNT",
    "FORMAL_STAGE1_PAIR_COUNT",
    "FORMAL_STAGE1_PAYLOAD_SHA256",
    "FORMAL_STAGE1_UNIQUE_TICKER_COUNT",
    "FORMAL_STAGE2_PAIR_COUNT",
    "FORMAL_STAGE2_PAYLOAD_SHA256",
    "FORMAL_STAGE2_UNIQUE_TICKER_COUNT",
    "MinutePairScope",
    "MinuteScopeError",
    "PAIR_SORT_FIELDS",
    "STAGE1_RECORD_FIELDS",
    "STAGE2_RECORD_FIELDS",
    "build_formal_development_scopes",
    "canonical_scope_sha256",
    "generate_stage1_candidate_scope",
    "generate_stage2_all_roles_scope",
    "verify_stage1_overlap",
]
