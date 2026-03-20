# TODO: Implement cross-strategy caching for scan workloads. During scans,
# the same symbol's market data (daily bars, option contracts, quotes) is
# re-fetched for each strategy combination. A shared per-symbol cache keyed
# by (symbol, date_range, dte_params) would deduplicate these calls, cutting
# Massive API usage roughly proportional to the number of strategy types per
# symbol. The cache should be scoped to a single scan execution lifetime to
# avoid stale data across runs.

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date

import structlog

from backtestforecast.market_data.types import DailyBar, OptionContractRecord

logger = structlog.get_logger("market_data.prefetch")

_DEFAULT_API_CONCURRENCY = 10


@dataclass(slots=True)
class PrefetchSummary:
    dates_processed: int = 0
    contracts_fetched: int = 0
    quotes_fetched: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "dates_processed": self.dates_processed,
            "contracts_fetched": self.contracts_fetched,
            "quotes_fetched": self.quotes_fetched,
            "errors": self.errors[:20],
        }


@dataclass(slots=True)
class _DateResult:
    """Accumulator returned by a single-date prefetch worker."""
    contracts_fetched: int = 0
    quotes_fetched: int = 0
    errors: list[str] = field(default_factory=list)


class OptionDataPrefetcher:
    """Eagerly fetches all option contracts and quotes for a symbol across a
    date range, populating the gateway's in-memory and Redis caches.

    Uses a thread pool to fetch multiple dates concurrently.  The gateway's
    in-memory LRU caches are protected by ``threading.Lock`` so concurrent
    writes are safe.
    """

    def __init__(self, max_workers: int | None = None, api_concurrency: int = _DEFAULT_API_CONCURRENCY, timeout_seconds: int = 300) -> None:
        if max_workers is None:
            from backtestforecast.config import get_settings
            max_workers = get_settings().prefetch_max_workers
        self._max_workers = max_workers
        self._api_concurrency = threading.Semaphore(api_concurrency)
        self._timeout = timeout_seconds

    def prefetch_for_symbol(
        self,
        symbol: str,
        bars: list[DailyBar],
        start_date: date,
        end_date: date,
        target_dte: int,
        dte_tolerance_days: int,
        option_gateway: object,
    ) -> PrefetchSummary:
        from backtestforecast.market_data.service import MassiveOptionGateway

        if not isinstance(option_gateway, MassiveOptionGateway):
            raise TypeError("option_gateway must be a MassiveOptionGateway instance")

        trade_dates = [
            bar.trade_date
            for bar in bars
            if start_date <= bar.trade_date <= end_date
        ]

        if not trade_dates:
            return PrefetchSummary()

        summary = PrefetchSummary()
        counter_lock = threading.Lock()
        workers = min(self._max_workers, len(trade_dates))

        logger.info(
            "prefetch.starting",
            symbol=symbol,
            dates=len(trade_dates),
            workers=workers,
        )

        def _fetch_date(trade_date: date) -> _DateResult:
            result = _DateResult()
            for contract_type in ("put", "call"):
                try:
                    with self._api_concurrency:
                        contracts = option_gateway.list_contracts(
                            entry_date=trade_date,
                            contract_type=contract_type,
                            target_dte=target_dte,
                            dte_tolerance_days=dte_tolerance_days,
                        )
                    result.contracts_fetched += len(contracts)
                    for contract in contracts:
                        with self._api_concurrency:
                            option_gateway.get_quote(contract.ticker, trade_date)
                        result.quotes_fetched += 1
                except Exception as exc:
                    import re
                    sanitized = re.sub(
                        r"(api[_-]?key|token|password|secret|auth)[=:\s]\S+",
                        r"\1=<REDACTED>",
                        str(exc),
                        flags=re.IGNORECASE,
                    )
                    if len(sanitized) > 200:
                        sanitized = sanitized[:200] + "..."
                    msg = f"{symbol} {trade_date} {contract_type}: {sanitized}"
                    result.errors.append(msg)
                    logger.debug(
                        "prefetch.date_failed",
                        symbol=symbol,
                        date=str(trade_date),
                        contract_type=contract_type,
                        exc_info=True,
                    )
            return result

        pool = ThreadPoolExecutor(max_workers=workers)
        timed_out = False
        try:
            futures = {
                pool.submit(_fetch_date, td): td for td in trade_dates
            }
            try:
                for future in as_completed(futures, timeout=self._timeout):
                    td = futures[future]
                    try:
                        date_result = future.result()
                    except Exception:
                        logger.warning("prefetch.worker_error", date=str(td), exc_info=True)
                        date_result = _DateResult(errors=[f"{symbol} {td}: worker exception"])

                    with counter_lock:
                        summary.dates_processed += 1
                        summary.contracts_fetched += date_result.contracts_fetched
                        summary.quotes_fetched += date_result.quotes_fetched
                        summary.errors.extend(date_result.errors)

                        if summary.dates_processed % 50 == 0:
                            logger.info(
                                "prefetch.progress",
                                symbol=symbol,
                                dates_done=summary.dates_processed,
                                dates_total=len(trade_dates),
                                contracts=summary.contracts_fetched,
                                quotes=summary.quotes_fetched,
                            )
            except TimeoutError:
                timed_out = True
                logger.warning(
                    "prefetch.timeout",
                    symbol=symbol,
                    dates_done=summary.dates_processed,
                    dates_total=len(trade_dates),
                )
                summary.errors.append(f"{symbol}: prefetch timed out after {self._timeout}s")
        finally:
            pool.shutdown(wait=not timed_out, cancel_futures=timed_out)

        logger.info(
            "prefetch.completed",
            symbol=symbol,
            dates_processed=summary.dates_processed,
            contracts_fetched=summary.contracts_fetched,
            quotes_fetched=summary.quotes_fetched,
            error_count=len(summary.errors),
            workers=workers,
        )
        return summary
