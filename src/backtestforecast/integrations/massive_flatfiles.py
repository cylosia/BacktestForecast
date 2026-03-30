from __future__ import annotations

import csv
import gzip
import io
import threading
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Iterable, Iterator, TypeVar
from uuid import uuid4

import httpx
import structlog

from backtestforecast.config import get_settings
from backtestforecast.errors import ConfigurationError, ExternalServiceError
from backtestforecast.market_data.historical_store import parse_option_ticker_metadata
from backtestforecast.models import (
    HistoricalOptionDayBar,
    HistoricalUnderlyingDayBar,
)

logger = structlog.get_logger("massive_flatfiles")

_STOCK_DAY_DATASET = "us_stocks_sip/day_aggs_v1"
_OPTION_DAY_DATASET = "us_options_opra/day_aggs_v1"
_T = TypeVar("_T")


def _day_key(dataset: str, trade_date: date) -> str:
    day = trade_date.isoformat()
    return f"{dataset}/{trade_date.year:04d}/{trade_date.month:02d}/{day}.csv.gz"


def _first(row: dict[str, str], *candidates: str) -> str | None:
    for candidate in candidates:
        value = row.get(candidate)
        if value not in (None, ""):
            return value
    return None


class _IteratorBytesIO(io.RawIOBase):
    """Adapt an iterator of byte chunks into a file-like object for gzip streaming."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        super().__init__()
        self._chunks = iter(chunks)
        self._buffer = bytearray()

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:  # type: ignore[no-untyped-def]
        if self.closed:
            return 0
        view = memoryview(b).cast("B")
        if not view:
            return 0
        while len(self._buffer) < len(view):
            try:
                chunk = next(self._chunks)
            except StopIteration:
                break
            if chunk:
                self._buffer.extend(chunk)
        count = min(len(view), len(self._buffer))
        if count <= 0:
            return 0
        view[:count] = self._buffer[:count]
        del self._buffer[:count]
        return count


@dataclass(slots=True)
class MassiveFlatFilesClient:
    base_url: str
    api_key: str
    bucket: str | None = None
    use_s3: bool = False
    _http_client: httpx.Client | None = field(init=False, default=None, repr=False)
    _s3_client: Any | None = field(init=False, default=None, repr=False)
    _client_lock: threading.Lock = field(init=False, default_factory=threading.Lock, repr=False)

    @classmethod
    def from_settings(cls) -> MassiveFlatFilesClient:
        settings = get_settings()
        if not settings.massive_api_key:
            raise ConfigurationError("MASSIVE_API_KEY is required for flat-file sync.")
        base_url = settings.massive_flatfiles_base_url.rstrip("/")
        if settings.massive_flatfiles_use_s3 and settings.s3_endpoint_url:
            base_url = settings.s3_endpoint_url.rstrip("/")
        return cls(
            base_url=base_url,
            api_key=settings.massive_api_key,
            bucket=settings.massive_flatfiles_bucket,
            use_s3=settings.massive_flatfiles_use_s3,
        )

    def download_csv_rows(self, dataset: str, trade_date: date) -> list[dict[str, str]]:
        return list(self.iter_csv_rows(dataset, trade_date))

    def iter_csv_rows(self, dataset: str, trade_date: date) -> Iterator[dict[str, str]]:
        if self.use_s3:
            yield from self._iter_csv_rows_s3(dataset, trade_date)
            return
        yield from self._iter_csv_rows_http(dataset, trade_date)

    def close(self) -> None:
        if self._http_client is not None:
            self._http_client.close()
            self._http_client = None
        if self._s3_client is not None:
            close = getattr(self._s3_client, "close", None)
            if callable(close):
                close()
            self._s3_client = None

    def __enter__(self) -> MassiveFlatFilesClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _download_gzip(self, dataset: str, trade_date: date) -> bytes:
        if self.use_s3:
            return self._download_gzip_s3(dataset, trade_date)
        return self._download_gzip_http(dataset, trade_date)

    def _download_gzip_http(self, dataset: str, trade_date: date) -> bytes:
        key = _day_key(dataset, trade_date)
        url = f"{self.base_url}/{key}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = self._get_http_client().get(url, headers=headers)
        if response.status_code == 404:
            raise ExternalServiceError(f"Massive flat file not found for {trade_date.isoformat()}: {dataset}")
        if response.status_code >= 400:
            raise ExternalServiceError(f"Massive flat file download failed with {response.status_code}.")
        return response.content

    def _iter_csv_rows_http(self, dataset: str, trade_date: date) -> Iterator[dict[str, str]]:
        key = _day_key(dataset, trade_date)
        url = f"{self.base_url}/{key}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        with self._get_http_client().stream("GET", url, headers=headers) as response:
            if response.status_code == 404:
                raise ExternalServiceError(f"Massive flat file not found for {trade_date.isoformat()}: {dataset}")
            if response.status_code >= 400:
                raise ExternalServiceError(f"Massive flat file download failed with {response.status_code}.")
            raw = io.BufferedReader(_IteratorBytesIO(response.iter_bytes()))
            with gzip.GzipFile(fileobj=raw) as gz, io.TextIOWrapper(gz, encoding="utf-8", newline="") as text_stream:
                yield from csv.DictReader(text_stream)

    def _download_gzip_s3(self, dataset: str, trade_date: date) -> bytes:
        from botocore.exceptions import ClientError

        client = self._get_s3_client()
        key = _day_key(dataset, trade_date)
        try:
            response = client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"NoSuchKey", "404", "NotFound"}:
                raise ExternalServiceError(f"Massive flat file not found for {trade_date.isoformat()}: {dataset}") from exc
            raise
        return response["Body"].read()

    def _iter_csv_rows_s3(self, dataset: str, trade_date: date) -> Iterator[dict[str, str]]:
        from botocore.exceptions import ClientError

        client = self._get_s3_client()
        key = _day_key(dataset, trade_date)
        try:
            response = client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"NoSuchKey", "404", "NotFound"}:
                raise ExternalServiceError(f"Massive flat file not found for {trade_date.isoformat()}: {dataset}") from exc
            raise
        body = response["Body"]
        try:
            with gzip.GzipFile(fileobj=body) as gz:
                text_stream = io.TextIOWrapper(gz, encoding="utf-8", newline="")
                yield from csv.DictReader(text_stream)
        finally:
            body.close()

    def _get_http_client(self) -> httpx.Client:
        if self._http_client is None:
            with self._client_lock:
                if self._http_client is None:
                    self._http_client = httpx.Client(
                        timeout=httpx.Timeout(connect=10.0, read=120.0, write=60.0, pool=10.0),
                        trust_env=False,
                    )
        return self._http_client

    def _get_s3_client(self):
        if not self.bucket:
            raise ConfigurationError("massive_flatfiles_bucket is required when using S3 mode.")
        if self._s3_client is None:
            with self._client_lock:
                if self._s3_client is None:
                    try:
                        import boto3  # type: ignore[import-untyped]
                        from botocore.config import Config as BotoConfig
                    except ImportError as exc:
                        raise ConfigurationError("boto3 is required for S3 flat-file sync.") from exc
                    settings = get_settings()
                    self._s3_client = boto3.client(
                        "s3",
                        endpoint_url=self.base_url,
                        aws_access_key_id=settings.aws_access_key_id,
                        aws_secret_access_key=settings.aws_secret_access_key,
                        region_name=settings.s3_region,
                        config=BotoConfig(
                            signature_version="s3v4",
                            s3={"addressing_style": "path"},
                            connect_timeout=10,
                            read_timeout=120,
                            retries={"max_attempts": 2, "mode": "standard"},
                            proxies={},
                        ),
                    )
        return self._s3_client


def parse_stock_day_rows(rows: list[dict[str, str]], trade_date: date, *, symbols: set[str] | None = None) -> list[HistoricalUnderlyingDayBar]:
    parsed: list[HistoricalUnderlyingDayBar] = []
    for row in rows:
        symbol = (_first(row, "ticker", "symbol") or "").strip().upper()
        if not symbol or (symbols is not None and symbol not in symbols):
            continue
        try:
            parsed.append(
                HistoricalUnderlyingDayBar(
                    id=uuid4(),
                    symbol=symbol,
                    trade_date=trade_date,
                    open_price=Decimal(_first(row, "open", "o") or "0"),
                    high_price=Decimal(_first(row, "high", "h") or "0"),
                    low_price=Decimal(_first(row, "low", "l") or "0"),
                    close_price=Decimal(_first(row, "close", "c") or "0"),
                    volume=Decimal(_first(row, "volume", "v") or "0"),
                    source_file_date=trade_date,
                )
            )
        except Exception:
            logger.debug("massive_flatfiles.stock_row_skipped", row=row)
    return parsed


def parse_option_day_rows(rows: list[dict[str, str]], trade_date: date, *, symbols: set[str] | None = None) -> list[HistoricalOptionDayBar]:
    parsed: list[HistoricalOptionDayBar] = []
    for row in rows:
        option_ticker = (_first(row, "ticker", "sym", "option_ticker") or "").strip().upper()
        metadata = parse_option_ticker_metadata(option_ticker)
        if metadata is None:
            logger.debug("massive_flatfiles.option_row_skipped", ticker=option_ticker)
            continue
        underlying, expiration, contract_type, strike = metadata
        if symbols is not None and underlying not in symbols:
            continue
        try:
            parsed.append(
                HistoricalOptionDayBar(
                    id=uuid4(),
                    option_ticker=option_ticker,
                    underlying_symbol=underlying,
                    trade_date=trade_date,
                    expiration_date=expiration,
                    contract_type=contract_type,
                    strike_price=Decimal(f"{strike:.4f}"),
                    open_price=Decimal(_first(row, "open", "o") or "0"),
                    high_price=Decimal(_first(row, "high", "h") or "0"),
                    low_price=Decimal(_first(row, "low", "l") or "0"),
                    close_price=Decimal(_first(row, "close", "c") or "0"),
                    volume=Decimal(_first(row, "volume", "v") or "0"),
                    source_file_date=trade_date,
                )
            )
        except Exception:
            logger.debug("massive_flatfiles.option_row_parse_failed", ticker=option_ticker, row=row)
    return parsed


def stock_day_bar_payload(row: dict[str, str], trade_date: date, *, symbols: set[str] | None = None) -> dict[str, object] | None:
    symbol = (_first(row, "ticker", "symbol") or "").strip().upper()
    if not symbol or (symbols is not None and symbol not in symbols):
        return None
    try:
        return {
            "id": uuid4(),
            "symbol": symbol,
            "trade_date": trade_date,
            "open_price": Decimal(_first(row, "open", "o") or "0"),
            "high_price": Decimal(_first(row, "high", "h") or "0"),
            "low_price": Decimal(_first(row, "low", "l") or "0"),
            "close_price": Decimal(_first(row, "close", "c") or "0"),
            "volume": Decimal(_first(row, "volume", "v") or "0"),
            "source_file_date": trade_date,
        }
    except Exception:
        logger.debug("massive_flatfiles.stock_row_skipped", row=row)
        return None


def option_day_bar_payload(row: dict[str, str], trade_date: date, *, symbols: set[str] | None = None) -> dict[str, object] | None:
    option_ticker = (_first(row, "ticker", "sym", "option_ticker") or "").strip().upper()
    metadata = parse_option_ticker_metadata(option_ticker)
    if metadata is None:
        logger.debug("massive_flatfiles.option_row_skipped", ticker=option_ticker)
        return None
    underlying, expiration, contract_type, strike = metadata
    if symbols is not None and underlying not in symbols:
        return None
    try:
        return {
            "id": uuid4(),
            "option_ticker": option_ticker,
            "underlying_symbol": underlying,
            "trade_date": trade_date,
            "expiration_date": expiration,
            "contract_type": contract_type,
            "strike_price": Decimal(f"{strike:.4f}"),
            "open_price": Decimal(_first(row, "open", "o") or "0"),
            "high_price": Decimal(_first(row, "high", "h") or "0"),
            "low_price": Decimal(_first(row, "low", "l") or "0"),
            "close_price": Decimal(_first(row, "close", "c") or "0"),
            "volume": Decimal(_first(row, "volume", "v") or "0"),
            "source_file_date": trade_date,
        }
    except Exception:
        logger.debug("massive_flatfiles.option_row_parse_failed", ticker=option_ticker, row=row)
        return None


def option_day_bar_record(row: dict[str, str], trade_date: date, *, symbols: set[str] | None = None) -> tuple[object, ...] | None:
    option_ticker = (_first(row, "ticker", "sym", "option_ticker") or "").strip().upper()
    metadata = parse_option_ticker_metadata(option_ticker)
    if metadata is None:
        logger.debug("massive_flatfiles.option_row_skipped", ticker=option_ticker)
        return None
    underlying, expiration, contract_type, strike = metadata
    if symbols is not None and underlying not in symbols:
        return None
    try:
        return (
            uuid4(),
            option_ticker,
            underlying,
            trade_date,
            expiration,
            contract_type,
            Decimal(f"{strike:.4f}"),
            Decimal(_first(row, "open", "o") or "0"),
            Decimal(_first(row, "high", "h") or "0"),
            Decimal(_first(row, "low", "l") or "0"),
            Decimal(_first(row, "close", "c") or "0"),
            Decimal(_first(row, "volume", "v") or "0"),
            trade_date,
        )
    except Exception:
        logger.debug("massive_flatfiles.option_row_parse_failed", ticker=option_ticker, row=row)
        return None


def iter_stock_day_bar_payloads(
    rows: Iterable[dict[str, str]],
    trade_date: date,
    *,
    symbols: set[str] | None = None,
) -> Iterator[dict[str, object]]:
    for row in rows:
        payload = stock_day_bar_payload(row, trade_date, symbols=symbols)
        if payload is not None:
            yield payload


def iter_option_day_bar_payloads(
    rows: Iterable[dict[str, str]],
    trade_date: date,
    *,
    symbols: set[str] | None = None,
) -> Iterator[dict[str, object]]:
    for row in rows:
        payload = option_day_bar_payload(row, trade_date, symbols=symbols)
        if payload is not None:
            yield payload


def iter_option_day_bar_records(
    rows: Iterable[dict[str, str]],
    trade_date: date,
    *,
    symbols: set[str] | None = None,
) -> Iterator[tuple[object, ...]]:
    for row in rows:
        record = option_day_bar_record(row, trade_date, symbols=symbols)
        if record is not None:
            yield record


def chunked(iterable: Iterable[_T], size: int) -> Iterator[list[_T]]:
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    batch: list[_T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def stock_day_dataset() -> str:
    return _STOCK_DAY_DATASET


def option_day_dataset() -> str:
    return _OPTION_DAY_DATASET
