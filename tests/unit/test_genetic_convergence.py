"""Verify genetic optimizer convergence detection."""
from __future__ import annotations


def test_genetic_config_has_max_stale():
    from backtestforecast.schemas.sweeps import GeneticSweepConfig
    config = GeneticSweepConfig()
    assert config.max_stale_generations >= 2
