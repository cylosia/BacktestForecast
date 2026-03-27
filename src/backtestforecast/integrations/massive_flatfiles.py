from __future__ import annotations

import csv
import gzip
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
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


def _day_key(dataset: str, trade_date: date) -> str:
    day = trade_date.isoformat()
    return f"{dataset}/{trade_date.year:04d}/{trade_date.month:02d}/{day}.csv.gz"


def _first(row: dict[str, str], *candidates: str) -> str | None:
    for candidate in candidates:
        value = row.get(candidate)
        if value not in (None, ""):
            return value
    return None


@dataclass(slots=True)
class MassiveFlatFilesClient:
    base_url: str
    api_key: str
    bucket: str | None = None
    use_s3: bool = False

    @classmethod
    def from_settings(cls) -> MassiveFlatFilesClient:
        settings = get_settings()
        if not settings.massive_api_key:
            raise ConfigurationError("MASSIVE_API_KEY is required for flat-file sync.")
        return cls(
            base_url=settings.massive_flatfiles_base_url.rstrip("/"),
            api_key=settings.massive_api_key,
            bucket=settings.massive_flatfiles_bucket,
            use_s3=settings.massive_flatfiles_use_s3,
        )

    def download_csv_rows(self, dataset: str, trade_date: date) -> list[dict[str, str]]:
        payload = self._download_gzip(dataset, trade_date)
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as fh:
            text = fh.read().decode("utf-8")
        return list(csv.DictReader(io.StringIO(text)))

    def _download_gzip(self, dataset: str, trade_date: date) -> bytes:
        if self.use_s3:
            return self._download_gzip_s3(dataset, trade_date)
        return self._download_gzip_http(dataset, trade_date)

    def _download_gzip_http(self, dataset: str, trade_date: date) -> bytes:
        key = _day_key(dataset, trade_date)
        url = f"{self.base_url}/{key}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        with httpx.Client(
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=60.0, pool=10.0),
            trust_env=False,
        ) as client:
            response = client.get(url, headers=headers)
        if response.status_code == 404:
            raise ExternalServiceError(f"Massive flat file not found for {trade_date.isoformat()}: {dataset}")
        if response.status_code >= 400:
            raise ExternalServiceError(f"Massive flat file download failed with {response.status_code}.")
        return response.content

    def _download_gzip_s3(self, dataset: str, trade_date: date) -> bytes:
        if not self.bucket:
            raise ConfigurationError("massive_flatfiles_bucket is required when using S3 mode.")
        try:
            import boto3  # type: ignore[import-untyped]
            from botocore.config import Config as BotoConfig
        except ImportError as exc:
            raise ConfigurationError("boto3 is required for S3 flat-file sync.") from exc
        settings = get_settings()
        client = boto3.client(
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
        key = _day_key(dataset, trade_date)
        response = client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()


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


def stock_day_dataset() -> str:
    return _STOCK_DAY_DATASET


def option_day_dataset() -> str:
    return _OPTION_DAY_DATASET
