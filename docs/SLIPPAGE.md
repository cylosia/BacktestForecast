# Slippage Computation Semantics

## Overview

Slippage in BacktestForecast is computed **per-leg** rather than on the net position value.

## Formula

```
slippage = sum(|leg_value_i| × slippage_pct) for each leg i
```

Each leg's absolute value is multiplied by the slippage percentage, then summed across all legs.

## Rationale

Multi-leg strategies (e.g., iron condors, spreads, straddles) often have:

- **Low net position value** — long and short legs partially offset each other
- **High gross transacted value** — each leg is traded individually at meaningful size

If slippage were applied to the net position value, it would underestimate the true cost. For example, an iron condor with near-zero net premium would show negligible slippage, even though each of the four legs incurs real execution cost.

By computing slippage per-leg, we model the actual trading friction: every leg that is bought or sold experiences slippage proportional to its absolute value.
