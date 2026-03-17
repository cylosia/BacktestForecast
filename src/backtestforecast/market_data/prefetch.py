from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import structlog

from backtestforecast.market_data.types import DailyBar

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


class OptionDataPrefetcher:
    """Eagerly fetches all option contracts and quotes for a symbol across a
    date range, populating the gateway's in-memory and Redis caches.

    After prefetching, subsequent backtest runs for the same symbol and dates
    will resolve entirely from cache with zero Massive API calls.
    """

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

        summary = PrefetchSummary()
        trade_dates = [
            bar.trade_date
            for bar in bars
            if start_date <= bar.trade_date <= end_date
        ]

        for trade_date in trade_dates:
            summary.dates_processed += 1
            for contract_type in ("put", "call"):
                try:
                    contracts = option_gateway.list_contracts(
                        entry_date=trade_date,
                        contract_type=contract_type,
                        target_dte=target_dte,
                        dte_tolerance_days=dte_tolerance_days,
                    )
                    summary.contracts_fetched += len(contracts)

                    for contract in contracts:
                        option_gateway.get_quote(contract.ticker, trade_date)
                        summary.quotes_fetched += 1
                except Exception as exc:
                    msg = f"{symbol} {trade_date} {contract_type}: {exc}"
                    summary.errors.append(msg)
                    logger.debug("prefetch.date_failed", symbol=symbol, date=str(trade_date), contract_type=contract_type, exc_info=True)

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
        )
        return summary
