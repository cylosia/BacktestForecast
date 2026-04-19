from __future__ import annotations

from datetime import date

from backtestforecast.market_data.types import DailyBar
from backtestforecast.market_data import vix_regime


class _FakeStore:
    def __init__(self, bars_by_symbol: dict[str, list[DailyBar]]) -> None:
        self._bars_by_symbol = bars_by_symbol

    def get_underlying_day_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBar]:
        return [
            bar
            for bar in self._bars_by_symbol.get(symbol, [])
            if start_date <= bar.trade_date <= end_date
        ]


def test_parse_vix_csv_rows_skips_missing_and_parses_dates() -> None:
    text = "\n".join(
        [
            "DATE,VIXCLS",
            "2026-03-06,29.49",
            "2026-03-09,.",
            "2026-03-13,27.19",
        ]
    )

    result = vix_regime.parse_vix_csv_rows(text)

    assert result == {
        date(2026, 3, 6): 29.49,
        date(2026, 3, 13): 27.19,
    }


def test_parse_vix_csv_rows_accepts_fred_observation_date_header() -> None:
    text = "\n".join(
        [
            "observation_date,VIXCLS",
            "2026-03-06,29.49",
            "2026-03-13,27.19",
        ]
    )

    result = vix_regime.parse_vix_csv_rows(text)

    assert result == {
        date(2026, 3, 6): 29.49,
        date(2026, 3, 13): 27.19,
    }


def test_build_weekly_change_snapshots_uses_previous_available_close() -> None:
    close_by_date = {
        date(2026, 3, 5): 28.0,
        date(2026, 3, 12): 27.0,
        date(2026, 3, 19): 26.0,
    }

    snapshots = vix_regime.build_weekly_change_snapshots(
        entry_dates=[date(2026, 3, 6), date(2026, 3, 13), date(2026, 3, 20)],
        close_by_date=close_by_date,
    )

    first = snapshots[date(2026, 3, 6)]
    assert first.effective_trade_date == date(2026, 3, 5)
    assert first.close_price == 28.0
    assert first.weekly_change_pct is None

    second = snapshots[date(2026, 3, 13)]
    assert second.effective_trade_date == date(2026, 3, 12)
    assert second.prior_effective_trade_date == date(2026, 3, 5)
    assert second.prior_close_price == 28.0
    assert round(float(second.weekly_change_pct or 0.0), 6) == -3.571429

    third = snapshots[date(2026, 3, 20)]
    assert third.effective_trade_date == date(2026, 3, 19)
    assert third.prior_effective_trade_date == date(2026, 3, 12)
    assert third.prior_close_price == 27.0
    assert round(float(third.weekly_change_pct or 0.0), 6) == -3.703704


def test_load_vix_close_series_prefers_db_bars_and_backfills_from_cache(tmp_path) -> None:
    store = _FakeStore(
        {
            "VIX": [
                DailyBar(
                    trade_date=date(2026, 3, 6),
                    open_price=29.0,
                    high_price=30.0,
                    low_price=28.0,
                    close_price=29.49,
                    volume=0.0,
                )
            ]
        }
    )
    cache_csv = tmp_path / "vixcls_fred.csv"
    cache_csv.write_text(
        "\n".join(
            [
                "DATE,VIXCLS",
                "2026-03-06,29.60",
                "2026-03-13,27.19",
            ]
        ),
        encoding="utf-8",
    )

    result = vix_regime.load_vix_close_series(
        start_date=date(2026, 3, 6),
        end_date=date(2026, 3, 13),
        store=store,  # type: ignore[arg-type]
        cache_csv=cache_csv,
        allow_cache_refresh=False,
    )

    assert result == {
        date(2026, 3, 6): 29.49,
        date(2026, 3, 13): 27.19,
    }


def test_load_vix_close_series_refreshes_when_cache_does_not_cover_start_date(
    monkeypatch,
    tmp_path,
) -> None:
    cache_csv = tmp_path / "vixcls_fred.csv"
    cache_csv.write_text(
        "\n".join(
            [
                "DATE,VIXCLS",
                "2026-03-06,29.60",
                "2026-03-13,27.19",
            ]
        ),
        encoding="utf-8",
    )
    refreshed = {
        date(2026, 2, 28): 31.12,
        date(2026, 3, 6): 29.60,
        date(2026, 3, 13): 27.19,
    }

    def _fake_refresh(path, *, timeout_seconds: float = 30.0):
        assert path == cache_csv
        assert timeout_seconds == 30.0
        return refreshed

    monkeypatch.setattr(vix_regime, "refresh_vix_cache_csv", _fake_refresh)

    result = vix_regime.load_vix_close_series(
        start_date=date(2026, 2, 28),
        end_date=date(2026, 3, 13),
        cache_csv=cache_csv,
        allow_cache_refresh=True,
    )

    assert result == refreshed
