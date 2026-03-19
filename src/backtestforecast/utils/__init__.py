from __future__ import annotations

import math
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

DECIMAL_QUANT = Decimal("0.0001")


def to_decimal(value: float | Decimal | None, *, allow_infinite: bool = False) -> Decimal | None:
    """Convert a float or Decimal to a quantized Decimal.

    NaN returns ``None`` so that scan/sweep serialization does not crash
    on unexpected NaN values from the backtest engine.  Infinite values
    return ``None`` when *allow_infinite* is True (appropriate for
    metrics like profit_factor where infinity is a valid result meaning
    "no losses"), or raise ``ValueError`` when False (the default, for
    fields that must be finite).
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value.is_nan():
            return None
        if value.is_infinite():
            if allow_infinite:
                return None
            raise ValueError(f"Non-finite Decimal value: {value}")
        return value.quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(fval):
        return None
    if math.isinf(fval):
        if allow_infinite:
            return None
        raise ValueError(f"Non-finite value: {value}")
    return Decimal(str(fval)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
