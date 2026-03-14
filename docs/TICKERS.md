# Ticker Format Requirements

## Regex

Tickers must match:

```
[A-Z0-9./^]{1,16}
```

- **Length:** 1–16 characters
- **Allowed:** Uppercase letters (`A-Z`), digits (`0-9`), dot (`.`), slash (`/`), caret (`^`)

## Supported Formats

| Format | Example | Notes |
|--------|---------|-------|
| Simple symbol | `SPY` | Common equities |
| Class B / fractional | `BRK.B` | Dot for share classes |
| Index | `^VIX` | Caret prefix for indices |
| Option symbol | `SPY240315C500` | OCC format: underlying + expiry + type + strike |

## Enforcement

Both **frontend** and **backend** enforce the same regex. Invalid tickers are rejected with a 422 validation error before reaching market data or backtest logic.
