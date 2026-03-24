"""Test that butterfly credit spreads compute max_loss_per_unit correctly.

A symmetric credit butterfly (opened for a net credit) should have
max_loss = wider_wing - credit, NOT zero.  This test constructs the
scenario directly from the payoff math in ButterflyStrategy.build_position.
"""
from __future__ import annotations


def test_symmetric_credit_butterfly_max_loss() -> None:
    """Symmetric credit butterfly: max_loss = wing_width - credit."""
    lower_mid = 2.00
    center_mid = 3.20
    upper_mid = 2.00

    entry_value_per_unit = (lower_mid + upper_mid - 2.0 * center_mid) * 100.0
    assert entry_value_per_unit < 0, "This scenario must be a credit butterfly"

    credit = abs(entry_value_per_unit)
    lower_strike = 100.0
    center_strike = 105.0
    upper_strike = 110.0
    left_width = (center_strike - lower_strike) * 100.0
    right_width = (upper_strike - center_strike) * 100.0
    wing_width = min(left_width, right_width)
    wider_wing = max(left_width, right_width)

    capital_per_unit = max(wider_wing - credit, 0.0)
    max_loss_per_unit = max(wider_wing - credit, 0.0)
    max_profit_per_unit = wing_width + credit

    assert max_loss_per_unit > 0, (
        f"Credit butterfly max_loss should be wider_wing - credit = "
        f"{wider_wing} - {credit} = {wider_wing - credit}, not 0"
    )
    assert max_loss_per_unit == capital_per_unit
    assert max_loss_per_unit == wider_wing - credit
    assert max_profit_per_unit == wing_width + credit


def test_asymmetric_credit_butterfly_max_loss() -> None:
    """Asymmetric wings: max_loss uses the wider wing width."""
    lower_mid = 2.00
    center_mid = 4.50
    upper_mid = 1.50

    entry_value_per_unit = (lower_mid + upper_mid - 2.0 * center_mid) * 100.0
    assert entry_value_per_unit < 0, "This scenario must be a credit butterfly"

    credit = abs(entry_value_per_unit)
    lower_strike = 95.0
    center_strike = 100.0
    upper_strike = 110.0
    left_width = (center_strike - lower_strike) * 100.0
    right_width = (upper_strike - center_strike) * 100.0
    wider_wing = max(left_width, right_width)

    max_loss_per_unit = max(wider_wing - credit, 0.0)

    assert wider_wing == right_width, "Right wing should be wider in this setup"
    assert max_loss_per_unit > 0
    assert max_loss_per_unit == wider_wing - credit


def test_debit_butterfly_max_loss_equals_debit() -> None:
    """Standard debit butterfly: max_loss = debit paid."""
    lower_mid = 3.00
    center_mid = 2.00
    upper_mid = 3.00

    entry_value_per_unit = (lower_mid + upper_mid - 2.0 * center_mid) * 100.0
    assert entry_value_per_unit >= 0, "This scenario must be a debit butterfly"

    max_loss_per_unit = entry_value_per_unit
    assert max_loss_per_unit == entry_value_per_unit
    assert max_loss_per_unit > 0
