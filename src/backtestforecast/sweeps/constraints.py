"""Constraint validation, repair, and random generation for custom leg individuals.

An *individual* is a list of leg dicts, each with the same fields as
``CustomLegDefinition``: contract_type, side, strike_offset,
expiration_offset, quantity_ratio.  We operate on plain dicts rather
than Pydantic models for speed during GA mutation/crossover.
"""
from __future__ import annotations

import random
from decimal import Decimal
from typing import Any

LegDict = dict[str, Any]
Individual = list[LegDict]

CONTRACT_TYPES = ("call", "put")
SIDES = ("long", "short")
STRIKE_OFFSETS = list(range(-10, 11))
EXPIRATION_OFFSETS = (0, 1, 2)
QUANTITY_RATIOS = [Decimal("1"), Decimal("2"), Decimal("3")]


def random_leg() -> LegDict:
    return {
        "asset_type": "option",
        "contract_type": random.choice(CONTRACT_TYPES),
        "side": random.choice(SIDES),
        "strike_offset": random.choice(STRIKE_OFFSETS),
        "expiration_offset": random.choice(EXPIRATION_OFFSETS),
        "quantity_ratio": random.choice(QUANTITY_RATIOS),
    }


def random_individual(num_legs: int) -> Individual:
    """Generate a random individual with *num_legs* legs, then repair and canonicalize."""
    ind = [random_leg() for _ in range(num_legs)]
    ind = repair(ind)
    return canonicalize(ind)


def is_valid(individual: Individual) -> bool:
    """Return True if the individual satisfies all structural constraints."""
    if not individual:
        return False

    option_legs = [leg for leg in individual if leg.get("asset_type", "option") == "option"]
    if not option_legs:
        return False

    sides = {leg["side"] for leg in option_legs}
    if len(sides) < 2:
        return False

    for leg in individual:
        if leg.get("asset_type", "option") == "option":
            if leg.get("contract_type") not in CONTRACT_TYPES:
                return False
        if leg["side"] not in SIDES:
            return False
        if not (-20 <= leg.get("strike_offset", 0) <= 20):
            return False
        if leg.get("expiration_offset", 0) not in (0, 1, 2):
            return False
        ratio = leg.get("quantity_ratio", Decimal("1"))
        if not (Decimal("0.1") <= Decimal(str(ratio)) <= Decimal("10")):
            return False

    if _has_cancelling_legs(option_legs):
        return False

    return True


def _has_cancelling_legs(option_legs: list[LegDict]) -> bool:
    """Detect legs that perfectly cancel (same type, strike, expiration, opposite sides, same qty)."""
    for i, a in enumerate(option_legs):
        for b in option_legs[i + 1:]:
            if (
                a["contract_type"] == b["contract_type"]
                and a.get("strike_offset", 0) == b.get("strike_offset", 0)
                and a.get("expiration_offset", 0) == b.get("expiration_offset", 0)
                and a["side"] != b["side"]
                and Decimal(str(a.get("quantity_ratio", 1))) == Decimal(str(b.get("quantity_ratio", 1)))
            ):
                return True
    return False


def repair(individual: Individual) -> Individual:
    """Fix constraint violations in-place and return the individual."""
    for leg in individual:
        if leg.get("asset_type", "option") == "option":
            if leg.get("contract_type") not in CONTRACT_TYPES:
                leg["contract_type"] = random.choice(CONTRACT_TYPES)

        if leg["side"] not in SIDES:
            leg["side"] = random.choice(SIDES)

        strike = leg.get("strike_offset", 0)
        leg["strike_offset"] = max(-20, min(20, strike))

        exp = leg.get("expiration_offset", 0)
        leg["expiration_offset"] = max(0, min(2, exp))

        ratio = Decimal(str(leg.get("quantity_ratio", 1)))
        ratio = max(Decimal("0.1"), min(Decimal("10"), ratio))
        leg["quantity_ratio"] = ratio

    option_legs = [leg for leg in individual if leg.get("asset_type", "option") == "option"]
    sides = {leg["side"] for leg in option_legs}
    if len(sides) < 2 and len(option_legs) >= 2:
        idx = random.randrange(len(option_legs))
        option_legs[idx]["side"] = "long" if option_legs[idx]["side"] == "short" else "short"

    max_repair_iterations = 200
    _repair_iter = 0
    while _has_cancelling_legs(option_legs):
        _repair_iter += 1
        if _repair_iter > max_repair_iterations:
            import structlog
            structlog.get_logger("sweeps.constraints").warning(
                "repair.iteration_limit_reached",
                max_iterations=max_repair_iterations,
            )
            break
        for leg in option_legs:
            if leg["side"] == "long":
                leg["strike_offset"] = leg.get("strike_offset", 0) + random.choice([-1, 1])
                leg["strike_offset"] = max(-20, min(20, leg["strike_offset"]))
                break
        else:
            break

    return individual


def canonicalize(individual: Individual) -> Individual:
    """Sort legs into a canonical order to eliminate permutation duplicates.

    Order: option legs first (sorted by contract_type, then strike_offset asc,
    then side, then expiration_offset), stock legs last.
    """
    options = [leg for leg in individual if leg.get("asset_type", "option") == "option"]
    stocks = [leg for leg in individual if leg.get("asset_type") == "stock"]

    options.sort(key=lambda leg: (
        leg.get("contract_type", ""),
        leg.get("strike_offset", 0),
        leg.get("side", ""),
        leg.get("expiration_offset", 0),
        float(leg.get("quantity_ratio", 1)),
    ))

    return options + stocks


def individual_to_key(individual: Individual) -> tuple:
    """Convert an individual to a hashable key for deduplication."""
    canon = canonicalize(list(individual))
    return tuple(
        (
            leg.get("asset_type", "option"),
            leg.get("contract_type"),
            leg.get("side"),
            leg.get("strike_offset", 0),
            leg.get("expiration_offset", 0),
            str(leg.get("quantity_ratio", 1)),
        )
        for leg in canon
    )
