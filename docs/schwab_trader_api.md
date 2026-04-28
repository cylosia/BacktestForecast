# Schwab Trader API / thinkorswim Integration

This integration targets the Schwab Trader API used by thinkorswim-enabled Schwab accounts.

## Setup

1. Create a Schwab developer app and configure its callback URL.
2. Set the Schwab env vars in `.env` or `apps/api/.env`.
3. Generate the OAuth URL:

```powershell
python scripts\schwab_trader_cli.py --kind live auth-url --state local-test
```

4. After approving the app, exchange the callback code:

```powershell
python scripts\schwab_trader_cli.py --kind live exchange-code --code "<callback-code>"
```

5. Fetch account hashes:

```powershell
python scripts\schwab_trader_cli.py --kind live account-numbers
```

Use separate `SCHWAB_TOKEN_PATH_LIVE` and `SCHWAB_TOKEN_PATH_PAPER` files. The public API host is the same; live vs paper is represented by the token/account profile you authenticate.

## Examples

Fetch quotes:

```powershell
python scripts\schwab_trader_cli.py --kind live quotes --symbols AAPL,MSFT --fields quote,reference
```

Fetch an option chain:

```powershell
python scripts\schwab_trader_cli.py --kind live option-chain --symbol AAPL --contract-type CALL --from-date 2026-05-01 --to-date 2026-05-08
```

Preview an option order:

```powershell
python scripts\schwab_trader_cli.py --kind paper preview-option-order --option-symbol "AAPL  260501C00100000" --quantity 1 --instruction BUY_TO_OPEN --limit-price 1.25
```

Preview a calendar spread:

```powershell
python scripts\schwab_trader_cli.py --kind paper preview-option-spread --price 0.75 --legs-json "[{\"symbol\":\"AAPL  260501C00100000\",\"quantity\":1,\"instruction\":\"SELL_TO_OPEN\"},{\"symbol\":\"AAPL  260508C00100000\",\"quantity\":1,\"instruction\":\"BUY_TO_OPEN\"}]"
```

## Live Order Safety

Order placement defaults to preview mode. A live order can only be sent when both conditions are true:

- `SCHWAB_TRADING_ENABLED=true`
- The caller passes `confirm_order_placement=True`, or the CLI uses `--confirm-order-placement`

Keep token files out of git. The example env paths use `.secrets/...`; that directory should remain local-only.

Official docs live at the Schwab developer portal: https://developer.schwab.com/products/trader-api--individual
