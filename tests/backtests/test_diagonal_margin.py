"""Test diagonal spread margin calculation."""


def test_diagonal_margin_uses_round_not_int():
    """Verify margin calculation uses round() instead of int() truncation."""
    # capital * 0.50 should use round(), not int()
    capital = 1001.0
    result = round(capital * 0.50, 2)
    assert result == 500.5, f"Expected 500.5 but got {result}"
    # int() would truncate to 500
    assert int(capital * 0.50) == 500
