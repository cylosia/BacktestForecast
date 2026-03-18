"""Tests for the genetic optimizer constraint module and GA engine."""
from __future__ import annotations

import random
from decimal import Decimal
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Constraints: is_valid
# ---------------------------------------------------------------------------

class TestIsValid:
    def test_valid_two_leg_spread(self):
        from backtestforecast.sweeps.constraints import is_valid

        individual = [
            {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "put", "side": "long", "strike_offset": -2, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        assert is_valid(individual) is True

    def test_all_same_side_rejected(self):
        from backtestforecast.sweeps.constraints import is_valid

        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "put", "side": "long", "strike_offset": -1, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        assert is_valid(individual) is False

    def test_cancelling_legs_rejected(self):
        from backtestforecast.sweeps.constraints import is_valid

        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "call", "side": "short", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        assert is_valid(individual) is False

    def test_duplicate_legs_with_different_ratios_allowed(self):
        """Ratio spreads have duplicate strikes with different quantities."""
        from backtestforecast.sweeps.constraints import is_valid

        individual = [
            {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "put", "side": "long", "strike_offset": -2, "expiration_offset": 0, "quantity_ratio": Decimal("2")},
        ]
        assert is_valid(individual) is True

    def test_duplicate_legs_same_side_same_strike_allowed(self):
        """Two identical legs (same side, strike, type) with different ratios are valid."""
        from backtestforecast.sweeps.constraints import is_valid

        individual = [
            {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("2")},
            {"asset_type": "option", "contract_type": "put", "side": "long", "strike_offset": -2, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        assert is_valid(individual) is True

    def test_empty_individual_rejected(self):
        from backtestforecast.sweeps.constraints import is_valid
        assert is_valid([]) is False

    def test_invalid_strike_offset_rejected(self):
        from backtestforecast.sweeps.constraints import is_valid

        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 25, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "call", "side": "short", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        assert is_valid(individual) is False


# ---------------------------------------------------------------------------
# Constraints: repair
# ---------------------------------------------------------------------------

class TestRepair:
    def test_repair_fixes_all_same_side(self):
        from backtestforecast.sweeps.constraints import is_valid, repair

        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "put", "side": "long", "strike_offset": -1, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        assert is_valid(individual) is False
        repaired = repair(individual)
        sides = {leg["side"] for leg in repaired}
        assert len(sides) == 2

    def test_repair_clamps_strike_offset(self):
        from backtestforecast.sweeps.constraints import repair

        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 50, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "call", "side": "short", "strike_offset": -50, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        repaired = repair(individual)
        assert repaired[0]["strike_offset"] == 20
        assert repaired[1]["strike_offset"] == -20

    def test_repair_clamps_quantity_ratio(self):
        from backtestforecast.sweeps.constraints import repair

        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("0.01")},
            {"asset_type": "option", "contract_type": "call", "side": "short", "strike_offset": 1, "expiration_offset": 0, "quantity_ratio": Decimal("100")},
        ]
        repaired = repair(individual)
        assert repaired[0]["quantity_ratio"] >= Decimal("0.1")
        assert repaired[1]["quantity_ratio"] <= Decimal("10")


# ---------------------------------------------------------------------------
# Constraints: canonicalize
# ---------------------------------------------------------------------------

class TestCanonicalize:
    def test_permutations_produce_same_canonical_form(self):
        from backtestforecast.sweeps.constraints import canonicalize

        leg_a = {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 2, "expiration_offset": 0, "quantity_ratio": Decimal("1")}
        leg_b = {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": -1, "expiration_offset": 0, "quantity_ratio": Decimal("1")}

        order1 = canonicalize([leg_a, leg_b])
        order2 = canonicalize([leg_b, leg_a])

        assert order1[0]["contract_type"] == order2[0]["contract_type"]
        assert order1[1]["contract_type"] == order2[1]["contract_type"]

    def test_individual_to_key_deduplicates(self):
        from backtestforecast.sweeps.constraints import individual_to_key

        leg_a = {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")}
        leg_b = {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": -2, "expiration_offset": 0, "quantity_ratio": Decimal("1")}

        key1 = individual_to_key([leg_a, leg_b])
        key2 = individual_to_key([leg_b, leg_a])
        assert key1 == key2


# ---------------------------------------------------------------------------
# Constraints: random_individual
# ---------------------------------------------------------------------------

class TestRandomIndividual:
    def test_random_individual_is_valid(self):
        from backtestforecast.sweeps.constraints import is_valid, random_individual

        random.seed(42)
        for _ in range(50):
            ind = random_individual(3)
            assert len(ind) == 3
            assert is_valid(ind), f"Generated invalid individual: {ind}"

    def test_random_individual_leg_count_matches(self):
        from backtestforecast.sweeps.constraints import random_individual

        for n in (2, 4, 6, 8):
            ind = random_individual(n)
            assert len(ind) == n


# ---------------------------------------------------------------------------
# Genetic engine: crossover
# ---------------------------------------------------------------------------

class TestCrossover:
    def test_crossover_produces_correct_length(self):
        from backtestforecast.sweeps.genetic import GeneticOptimizer

        parent_a = [
            {"contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"contract_type": "put", "side": "short", "strike_offset": -2, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"contract_type": "call", "side": "short", "strike_offset": 3, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        parent_b = [
            {"contract_type": "put", "side": "long", "strike_offset": -1, "expiration_offset": 0, "quantity_ratio": Decimal("2")},
            {"contract_type": "call", "side": "short", "strike_offset": 1, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"contract_type": "put", "side": "long", "strike_offset": -3, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]

        child = GeneticOptimizer._crossover(parent_a, parent_b)
        assert len(child) == 3

    def test_crossover_does_not_modify_parents(self):
        from backtestforecast.sweeps.genetic import GeneticOptimizer

        parent_a = [{"contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")}]
        parent_b = [{"contract_type": "put", "side": "short", "strike_offset": -1, "expiration_offset": 0, "quantity_ratio": Decimal("1")}]

        original_a = [dict(leg) for leg in parent_a]
        GeneticOptimizer._crossover(parent_a, parent_b)
        assert parent_a[0]["contract_type"] == original_a[0]["contract_type"]


# ---------------------------------------------------------------------------
# Genetic engine: mutation
# ---------------------------------------------------------------------------

class TestMutation:
    def test_mutation_changes_at_least_one_gene_at_high_rate(self):
        from backtestforecast.sweeps.genetic import GeneticOptimizer

        random.seed(42)
        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": -2, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]

        changed_count = 0
        for _ in range(100):
            mutated = GeneticOptimizer._mutate(individual, mutation_rate=1.0)
            for orig, mut in zip(individual, mutated):
                if any(orig.get(k) != mut.get(k) for k in ("contract_type", "side", "strike_offset", "expiration_offset", "quantity_ratio")):
                    changed_count += 1
                    break

        assert changed_count > 50, "High mutation rate should produce changes most of the time"

    def test_mutation_preserves_length(self):
        from backtestforecast.sweeps.genetic import GeneticOptimizer

        individual = [
            {"asset_type": "option", "contract_type": "call", "side": "long", "strike_offset": 0, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "put", "side": "short", "strike_offset": -1, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
            {"asset_type": "option", "contract_type": "call", "side": "short", "strike_offset": 3, "expiration_offset": 0, "quantity_ratio": Decimal("1")},
        ]
        mutated = GeneticOptimizer._mutate(individual, mutation_rate=0.5)
        assert len(mutated) == 3


# ---------------------------------------------------------------------------
# Genetic engine: tournament selection
# ---------------------------------------------------------------------------

class TestTournamentSelection:
    def test_selects_best_from_tournament(self):
        from backtestforecast.sweeps.genetic import GeneticOptimizer

        scored = [
            ([{"side": "long"}], 10.0),
            ([{"side": "short"}], 50.0),
            ([{"side": "long"}], 30.0),
        ]

        random.seed(1)
        winners = set()
        for _ in range(100):
            winner = GeneticOptimizer._tournament_select(scored, tournament_size=3)
            winners.add(winner[0]["side"])

        assert "short" in winners


# ---------------------------------------------------------------------------
# Genetic engine: convergence on a simple problem
# ---------------------------------------------------------------------------

class TestGAConvergence:
    def test_ga_finds_known_optimal(self):
        """The GA should converge toward individuals with high positive strike offsets
        on the long side when the fitness function rewards that."""
        from backtestforecast.sweeps.genetic import GeneticConfig, GeneticOptimizer

        def fitness_fn(individual):
            score = 0.0
            for leg in individual:
                if leg["side"] == "long" and leg.get("strike_offset", 0) > 0:
                    score += leg["strike_offset"] * 10
                if leg["side"] == "short" and leg.get("strike_offset", 0) < 0:
                    score += abs(leg["strike_offset"]) * 5
            return score

        config = GeneticConfig(
            num_legs=2,
            population_size=50,
            max_generations=20,
            tournament_size=3,
            crossover_rate=0.7,
            mutation_rate=0.3,
            elitism_count=3,
            max_workers=2,
            max_stale_generations=10,
        )
        optimizer = GeneticOptimizer(config)
        result = optimizer.run(fitness_fn)

        assert result.best_fitness >= 150, (
            f"GA should reach at least 50% of theoretical max (~300), got {result.best_fitness}"
        )
        assert result.generations_run >= 1
        assert result.total_evaluations > 0


# ---------------------------------------------------------------------------
# Schema: GeneticSweepConfig validation
# ---------------------------------------------------------------------------

class TestGeneticSweepConfigSchema:
    def test_valid_config(self):
        from backtestforecast.schemas.sweeps import GeneticSweepConfig

        config = GeneticSweepConfig(num_legs=4, population_size=100, max_generations=30)
        assert config.num_legs == 4

    def test_invalid_num_legs(self):
        from backtestforecast.schemas.sweeps import GeneticSweepConfig

        with pytest.raises(Exception, match="num_legs"):
            GeneticSweepConfig(num_legs=7)

    def test_elitism_must_be_less_than_population(self):
        from backtestforecast.schemas.sweeps import GeneticSweepConfig

        with pytest.raises(Exception, match="elitism_count"):
            GeneticSweepConfig(num_legs=2, population_size=20, elitism_count=20)

    def test_ga_terminates_early_on_flat_fitness(self):
        """When fitness never improves, the GA should stop after max_stale_generations."""
        from backtestforecast.sweeps.genetic import GeneticConfig, GeneticOptimizer

        def flat_fitness(individual):
            return 42.0

        config = GeneticConfig(
            num_legs=2,
            population_size=20,
            max_generations=50,
            tournament_size=3,
            crossover_rate=0.7,
            mutation_rate=0.3,
            elitism_count=2,
            max_workers=1,
            max_stale_generations=3,
        )
        optimizer = GeneticOptimizer(config)
        result = optimizer.run(flat_fitness)

        assert result.generations_run <= 5, (
            f"Expected early termination within ~5 generations (3 stale + buffer), "
            f"but ran {result.generations_run}"
        )


class TestGeneticSweepConfigSchema:
    def test_genetic_config_required_for_genetic_mode(self):
        from backtestforecast.schemas.sweeps import CreateSweepRequest

        with patch("backtestforecast.schemas.sweeps.get_settings") as mock_settings, \
             patch("backtestforecast.utils.dates.market_date_today") as mock_today:
            mock_settings.return_value.max_backtest_window_days = 1825
            from datetime import date
            mock_today.return_value = date(2025, 12, 31)

            with pytest.raises(Exception, match="genetic_config"):
                CreateSweepRequest(
                    mode="genetic",
                    symbol="TSLA",
                    strategy_types=["custom_2_leg"],
                    start_date=date(2025, 1, 1),
                    end_date=date(2025, 6, 30),
                    target_dte=8,
                    max_holding_days=8,
                    account_size=Decimal("10000"),
                    risk_per_trade_pct=Decimal("5"),
                    commission_per_contract=Decimal("0.65"),
                    entry_rule_sets=[{"name": "no_rules", "entry_rules": []}],
                )
