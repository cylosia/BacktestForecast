"""Genetic optimizer for custom N-leg option strategy configurations.

Uses tournament selection, single-point crossover, per-gene mutation,
and constraint repair to search the combinatorial space of leg
definitions efficiently.  Fitness evaluation is parallelized via
``ThreadPoolExecutor``.
"""
from __future__ import annotations

import random
from concurrent.futures import Executor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

import structlog

from backtestforecast.sweeps.constraints import (
    CONTRACT_TYPES,
    EXPIRATION_OFFSETS,
    QUANTITY_RATIOS,
    SIDES,
    STRIKE_OFFSETS,
    Individual,
    canonicalize,
    individual_to_key,
    is_valid,
    random_individual,
    repair,
)

logger = structlog.get_logger("sweeps.genetic")


@dataclass(slots=True)
class GeneticConfig:
    num_legs: int = 2
    population_size: int = 100
    max_generations: int = 30
    tournament_size: int = 3
    crossover_rate: float = 0.7
    mutation_rate: float = 0.3
    elitism_count: int = 5
    max_workers: int = 10
    max_stale_generations: int = 8
    top_n: int = 20


@dataclass(slots=True)
class GAResult:
    best_individual: Individual
    best_fitness: float
    generations_run: int
    total_evaluations: int
    top_individuals: list[tuple[Individual, float]] = field(default_factory=list)


# fitness_fn must be thread-safe: it is called from multiple threads
# concurrently via ThreadPoolExecutor in _evaluate_population.
FitnessFunc = Callable[[Individual], float]


class GeneticOptimizer:
    """Genetic algorithm optimizer for custom leg configurations."""

    def __init__(self, config: GeneticConfig) -> None:
        self.config = config

    def run(self, fitness_fn: FitnessFunc) -> GAResult:
        cfg = self.config
        population = self._seed_population(cfg.num_legs, cfg.population_size)
        _MAX_FITNESS_CACHE_SIZE = 10_000
        fitness_cache: dict[tuple, float] = {}
        total_evals = 0
        best_fitness = float("-inf")
        best_individual: Individual = population[0]
        stale_count = 0

        generations_run = 0

        for gen in range(cfg.max_generations):
            scored = self._evaluate_population(population, fitness_fn, fitness_cache, cfg.max_workers)
            total_evals += sum(1 for ind, _ in scored if individual_to_key(ind) not in fitness_cache)

            for ind, fit_val in scored:
                fitness_cache[individual_to_key(ind)] = fit_val

            if len(fitness_cache) > _MAX_FITNESS_CACHE_SIZE:
                sorted_keys = sorted(fitness_cache, key=fitness_cache.get)
                for k in sorted_keys[:len(fitness_cache) - _MAX_FITNESS_CACHE_SIZE]:
                    del fitness_cache[k]

            scored.sort(key=lambda x: x[1], reverse=True)
            gen_best = scored[0][1]

            if gen_best > best_fitness:
                best_fitness = gen_best
                best_individual = list(scored[0][0])
                stale_count = 0
            else:
                stale_count += 1

            logger.info(
                "ga.generation",
                generation=gen,
                best_fitness=round(gen_best, 4),
                overall_best=round(best_fitness, 4),
                population=len(scored),
                cache_size=len(fitness_cache),
            )

            generations_run = gen + 1

            if stale_count >= cfg.max_stale_generations:
                logger.info("ga.early_stop", generation=gen, reason="stale_generations", stale_count=stale_count)
                break

            # Build next generation
            next_pop: list[Individual] = []

            # Elitism: carry top N unchanged
            for ind, _ in scored[:cfg.elitism_count]:
                next_pop.append(list(ind))

            _fill_attempts = 0
            _max_fill_attempts = cfg.population_size * 50
            while len(next_pop) < cfg.population_size and _fill_attempts < _max_fill_attempts:
                _fill_attempts += 1
                parent_a = self._tournament_select(scored, cfg.tournament_size)
                parent_b = self._tournament_select(scored, cfg.tournament_size)

                if random.random() < cfg.crossover_rate:
                    child = self._crossover(parent_a, parent_b)
                else:
                    child = list(random.choice([parent_a, parent_b]))

                child = self._mutate(child, cfg.mutation_rate)
                child = repair(child)
                child = canonicalize(child)

                if is_valid(child):
                    next_pop.append(child)

            if _fill_attempts >= _max_fill_attempts:
                logger.warning(
                    "ga.fill_exhausted",
                    generation=gen,
                    filled=len(next_pop),
                    target=cfg.population_size,
                )

            population = next_pop

        top_n = cfg.top_n
        all_scored = sorted(fitness_cache.items(), key=lambda x: x[1], reverse=True)
        top_individuals: list[tuple[Individual, float]] = []
        for key, fit_val in all_scored[:top_n]:
            ind = [
                {
                    "asset_type": gene[0],
                    "contract_type": gene[1],
                    "side": gene[2],
                    "strike_offset": gene[3],
                    "expiration_offset": gene[4],
                    "quantity_ratio": Decimal(gene[5]),
                }
                for gene in key
            ]
            top_individuals.append((ind, fit_val))

        return GAResult(
            best_individual=best_individual,
            best_fitness=best_fitness,
            generations_run=generations_run,
            total_evaluations=total_evals,
            top_individuals=top_individuals,
        )

    @staticmethod
    def _seed_population(num_legs: int, size: int) -> list[Individual]:
        seen: set[tuple] = set()
        population: list[Individual] = []
        attempts = 0
        max_attempts = size * 20
        while len(population) < size and attempts < max_attempts:
            ind = random_individual(num_legs)
            key = individual_to_key(ind)
            if key not in seen and is_valid(ind):
                seen.add(key)
                population.append(ind)
            attempts += 1
        if not population:
            for _ in range(50):
                fallback = random_individual(num_legs)
                if is_valid(fallback):
                    population.append(fallback)
                    break
            else:
                logger.warning("genetic.fallback_exhausted", num_legs=num_legs)
                population.append(random_individual(num_legs))
        if len(population) < size:
            logger.warning(
                "genetic.seed_population_undersized",
                requested=size,
                actual=len(population),
                num_legs=num_legs,
            )
        return population

    @staticmethod
    def _evaluate_population(
        population: list[Individual],
        fitness_fn: FitnessFunc,
        fitness_cache: dict[tuple, float],
        max_workers: int,
    ) -> list[tuple[Individual, float]]:
        results: list[tuple[Individual, float]] = []
        to_evaluate: list[tuple[int, Individual]] = []

        for i, ind in enumerate(population):
            key = individual_to_key(ind)
            cached = fitness_cache.get(key)
            if cached is not None:
                results.append((ind, cached))
            else:
                to_evaluate.append((i, ind))

        if not to_evaluate:
            return results

        # TODO: ThreadPoolExecutor is suboptimal for CPU-bound fitness evaluation
        # due to the GIL. ProcessPoolExecutor would be preferable but requires
        # fitness_fn to be pickle-serializable. Since fitness_fn is typically a
        # closure constructed in the sweep service (capturing the DB session,
        # backtest engine, and config), it cannot be pickled without major
        # refactoring to extract it into a top-level function with explicit
        # serializable arguments. Revisit if sweep latency becomes a bottleneck.
        workers = min(max_workers, len(to_evaluate))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(fitness_fn, ind): (i, ind)
                for i, ind in to_evaluate
            }
            for future in as_completed(futures):
                _, ind = futures[future]
                try:
                    fit_val = future.result()
                except Exception:
                    logger.debug("ga.fitness_error", exc_info=True)
                    fit_val = float("-inf")
                results.append((ind, fit_val))

        return results

    @staticmethod
    def _tournament_select(
        scored: list[tuple[Individual, float]],
        tournament_size: int,
    ) -> Individual:
        competitors = random.sample(scored, min(tournament_size, len(scored)))
        winner = max(competitors, key=lambda x: x[1])
        return list(winner[0])

    @staticmethod
    def _crossover(parent_a: Individual, parent_b: Individual) -> Individual:
        """Single-point crossover: take prefix from A, suffix from B."""
        n = min(len(parent_a), len(parent_b))
        if n <= 1:
            return list(parent_a)
        point = random.randint(1, n - 1)
        child = [dict(leg) for leg in parent_a[:point]] + [dict(leg) for leg in parent_b[point:]]
        return child

    @staticmethod
    def _mutate(individual: Individual, mutation_rate: float) -> Individual:
        """Per-gene mutation: each leg parameter has a chance to be randomized."""
        mutated = [dict(leg) for leg in individual]
        for leg in mutated:
            if leg.get("asset_type") == "stock":
                continue
            if random.random() < mutation_rate:
                gene = random.choice(["contract_type", "side", "strike_offset", "expiration_offset", "quantity_ratio"])
                if gene == "contract_type":
                    leg["contract_type"] = random.choice(CONTRACT_TYPES)
                elif gene == "side":
                    leg["side"] = random.choice(SIDES)
                elif gene == "strike_offset":
                    leg["strike_offset"] = random.choice(STRIKE_OFFSETS)
                elif gene == "expiration_offset":
                    leg["expiration_offset"] = random.choice(EXPIRATION_OFFSETS)
                elif gene == "quantity_ratio":
                    leg["quantity_ratio"] = random.choice(QUANTITY_RATIOS)
        return mutated
