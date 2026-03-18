"""Backward-compatibility shim — use DailyPicksRepository instead."""
from __future__ import annotations

from backtestforecast.repositories.daily_picks import DailyPicksRepository as NightlyPipelineRunRepository

__all__ = ["NightlyPipelineRunRepository"]
