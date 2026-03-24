from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from backtestforecast.billing.entitlements import ExportFormat
from backtestforecast.schemas.backtests import BacktestSummaryResponse, BacktestTradeResponse, EquityCurvePointResponse

_LOOKS_NUMERIC = re.compile(
    r"^-?"
    r"("
    r"0"
    r"|[1-9]\d{0,17}"
    r"|[1-9]\d{0,17}\.\d+"
    r"|0\.\d+"
    r"|\.\d+"
    r"|[1-9][\d,]*\d"
    r"|[1-9][\d,]*\d\.\d+"
    r")"
    r"([eE][+-]?\d{1,4})?$"
)


@dataclass(frozen=True, slots=True)
class ExportBacktestSnapshot:
    symbol: str
    strategy_type: str
    status: str
    start_date: Any
    end_date: Any
    created_at: datetime
    summary: BacktestSummaryResponse
    trades: list[BacktestTradeResponse]
    equity_curve: list[EquityCurvePointResponse]
    warnings: list[dict[str, Any]]
    risk_free_rate: float | None
    risk_free_rate_source: str | None
    risk_free_rate_model: str | None
    risk_free_rate_curve_points: list[dict[str, Any]]
    risk_free_rate_curve_warning: str | None


def build_export_file_name(symbol: str, strategy_type: str, export_format: ExportFormat) -> str:
    safe_symbol = re.sub(r'[<>:"/\\|?*\s\x00]', "-", symbol).strip("-").lower() or "unknown"
    safe_strategy = re.sub(r'[<>:"/\\|?*\s\x00]', "-", strategy_type).strip("-").lower() or "strategy"
    extension = "csv" if export_format == ExportFormat.CSV else "pdf"
    return f"{safe_symbol}-{safe_strategy}-backtest.{extension}"


def export_mime_type(export_format: ExportFormat) -> str:
    if export_format == ExportFormat.CSV:
        return "text/csv; charset=utf-8"
    return "application/pdf"


def sanitize_csv_cell(value: object) -> object:
    if isinstance(value, str):
        value = value.replace("\x00", "")
    if not isinstance(value, str):
        return value
    original_first = value[:1]
    if original_first in {"\t", "\r", "\n"}:
        return "'" + value.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    sanitized = value.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    stripped = sanitized.strip()
    first = stripped[:1]
    if first in {"=", "+", "@", "|", ";"}:
        return "'" + sanitized
    if first == "-" and not _LOOKS_NUMERIC.match(stripped):
        return "'" + sanitized
    return sanitized


def format_metric_value(val: object, *, percent: bool = False, usd: bool = False) -> str:
    if val is None:
        return "N/A"
    if isinstance(val, str) and val in {"Infinity", "-Infinity"}:
        return val
    try:
        numeric = float(val)
    except (TypeError, ValueError):
        return str(val)
    if numeric == float("inf"):
        return "Infinity"
    if numeric == float("-inf"):
        return "-Infinity"
    if usd:
        return f"${numeric:,.2f}"
    if percent:
        return f"{numeric:.2f}%"
    return f"{numeric:,.2f}"


def normalize_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt
