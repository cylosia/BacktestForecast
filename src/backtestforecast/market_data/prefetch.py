from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date

import structlog

from backtestforecast.market_data.types import DailyBar, OptionContractRecord

logger = structlog.get_logger("market_data.prefetch")


@dataclass(slots=True)
class PrefetchSummary:
    dates_processed: int = 0
    contracts_fetched: int = 0
    quotes_fetched: int = 0
    quote_cache_hits: int = 0
    contract_cache_hits: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "dates_processed": self.dates_processed,
            "contracts_fetched": self.contracts_fetched,
            "quotes_fetched": self.quotes_fetched,
            "quote_cache_hits": self.quote_cache_hits,
            "contract_cache_hits": self.contract_cache_hits,
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

    def __init__(self, max_workers: int | None = None) -> None:
        if max_workers is None:
            from backtestforecast.config import get_settings
            max_workers = get_settings().prefetch_max_workers
        self._max_workers = max_workers

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
                    contracts = option_gateway.list_contracts(
                        entry_date=trade_date,
                        contract_type=contract_type,
                        target_dte=target_dte,
                        dte_tolerance_days=dte_tolerance_days,
                    )
                    result.contracts_fetched += len(contracts)
                    for contract in contracts:
                        option_gateway.get_quote(contract.ticker, trade_date)
                        result.quotes_fetched += 1
                except Exception as exc:
                    msg = f"{symbol} {trade_date} {contract_type}: {exc}"
                    result.errors.append(msg)
                    logger.debug(
                        "prefetch.date_failed",
                        symbol=symbol,
                        date=str(trade_date),
                        contract_type=contract_type,
                        exc_info=True,
                    )
            return result

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_fetch_date, td): td for td in trade_dates
            }
            for future in as_completed(futures):
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
