"""Fix 62: Genetic optimizer assigns -inf fitness to failed evaluations.

When a fitness function raises an exception for certain individuals, those
individuals must receive ``float('-inf')`` fitness and be ranked last.
"""
from __future__ import annotations

from decimal import Decimal

from backtestforecast.sweeps.genetic import GeneticConfig, GeneticOptimizer


def _make_individual(strike_offset: int) -> list[dict]:
    return [
        {
            "asset_type": "option",
            "contract_type": "put",
            "side": "short",
            "strike_offset": 0,
            "expiration_offset": 0,
            "quantity_ratio": Decimal("1"),
        },
        {
            "asset_type": "option",
            "contract_type": "put",
            "side": "long",
            "strike_offset": strike_offset,
            "expiration_offset": 0,
            "quantity_ratio": Decimal("1"),
        },
    ]


class TestFitnessErrorHandling:
    def test_failed_evaluation_gets_neg_inf(self):
        """_evaluate_population assigns -inf to individuals whose fitness raises."""
        failing_offsets = {-5, -10}

        def fitness_fn(individual):
            offset = individual[1]["strike_offset"]
            if offset in failing_offsets:
                raise RuntimeError("simulated failure")
            return float(offset) * -1

        population = [_make_individual(o) for o in [-1, -5, -3, -10, -2]]
        results = GeneticOptimizer._evaluate_population(
            population, fitness_fn, fitness_cache={}, max_workers=1,
        )

        result_map = {}
        for ind, fit in results:
            offset = ind[1]["strike_offset"]
            result_map[offset] = fit

        assert result_map[-5] == float("-inf"), "Failed individual should get -inf"
        assert result_map[-10] == float("-inf"), "Failed individual should get -inf"
        assert result_map[-1] > float("-inf"), "Successful individual should not be -inf"
        assert result_map[-3] > float("-inf"), "Successful individual should not be -inf"
        assert result_map[-2] > float("-inf"), "Successful individual should not be -inf"

    def test_failed_individuals_ranked_last(self):
        """After scoring, failed individuals must sort to the bottom."""
        def fitness_fn(individual):
            offset = individual[1]["strike_offset"]
            if offset == -5:
                raise ValueError("boom")
            return 100.0 + float(offset)

        population = [_make_individual(o) for o in [-1, -5, -3]]
        results = GeneticOptimizer._evaluate_population(
            population, fitness_fn, fitness_cache={}, max_workers=1,
        )

        results.sort(key=lambda x: x[1], reverse=True)
        last_ind, last_fit = results[-1]
        assert last_ind[1]["strike_offset"] == -5
        assert last_fit == float("-inf")

    def test_ga_run_tolerates_partial_failures(self):
        """A full GA run should complete even if some evaluations raise."""
        call_count = 0

        def fitness_fn(individual):
            nonlocal call_count
            call_count += 1
            if call_count % 3 == 0:
                raise RuntimeError("intermittent failure")
            return sum(leg.get("strike_offset", 0) for leg in individual) * -1.0

        config = GeneticConfig(
            num_legs=2,
            population_size=20,
            max_generations=5,
            tournament_size=3,
            crossover_rate=0.7,
            mutation_rate=0.3,
            elitism_count=2,
            max_workers=1,
            max_stale_generations=4,
        )
        optimizer = GeneticOptimizer(config)
        result = optimizer.run(fitness_fn)

        assert result.generations_run >= 1
        assert result.best_fitness > float("-inf"), "At least one individual should succeed"
