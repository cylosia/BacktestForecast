from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections.abc import Callable
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from backtestforecast.backtests import native_kernels as nk
from backtestforecast.backtests import strategies as strategies_pkg
from backtestforecast.backtests.strategies import common
from backtestforecast.market_data.types import OptionContractRecord
from backtestforecast.schemas.backtests import StrikeSelection, StrikeSelectionMode


def _timed(fn: Callable[[], Any], *, rounds: int) -> dict[str, object]:
    samples: list[float] = []
    last: Any = None
    for _ in range(rounds):
        start = time.perf_counter()
        last = fn()
        samples.append(time.perf_counter() - start)
    return {
        "samples_s": samples,
        "mean_s": statistics.mean(samples),
        "last": last,
    }


def _loader_for_mode(mode: str) -> Callable[[], object | None]:
    if mode == "python":
        return lambda: None
    if mode == "ctypes":
        native = nk._load_ctypes_native_kernel()
        if native is None:
            raise RuntimeError("ctypes kernel did not load")
        return lambda native=native: native
    if mode == "pyo3":
        nk.reset_native_kernel_module_cache()
        native = nk._load_native_kernel_module()
        if native is None or getattr(native, "__file__", None) is None:
            raise RuntimeError("pyo3 kernel did not load")
        return lambda native=native: native
    raise ValueError(f"Unsupported mode: {mode}")


def _with_loader(loader: Callable[[], object | None], fn: Callable[[], Any]) -> Any:
    original_loader = nk._load_native_kernel_module
    try:
        nk._load_native_kernel_module = loader
        return fn()
    finally:
        nk._load_native_kernel_module = original_loader


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--direct-calls", type=int, default=300_000)
    parser.add_argument("--batch-iters", type=int, default=50_000)
    parser.add_argument("--resolve-realized-iters", type=int, default=8_000)
    parser.add_argument("--resolve-lookup-iters", type=int, default=40_000)
    args = parser.parse_args()

    os.environ.pop("BFF_NATIVE_KERNELS_DLL", None)

    selection = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=Decimal("30"))
    strikes = [float(300 + i) for i in range(60)]
    contracts = [
        OptionContractRecord(
            f"O:SPY250404C{int(strike * 1000):08d}",
            "call",
            date(2025, 4, 4),
            strike,
            100.0,
        )
        for strike in strikes
    ]
    delta_lookup = {(strike, date(2025, 4, 4)): 0.1 + (idx / 200.0) for idx, strike in enumerate(strikes)}
    vols = [0.18 + ((idx % 9) * 0.01) for idx, _ in enumerate(strikes)]
    direct_inputs = [
        (
            100.0 + (idx % 23),
            80.0 + (idx % 41),
            7 + (idx % 45),
            "call" if idx % 2 == 0 else "put",
            0.18 + ((idx % 9) * 0.01),
            0.045,
            0.0,
        )
        for idx in range(args.direct_calls)
    ]

    class _Gateway:
        def get_quote(self, option_ticker: str, trade_date: date):
            raise AssertionError(f"benchmark unexpectedly fetched quote for {option_ticker} on {trade_date}")

    gateway = _Gateway()

    def _bench_direct() -> float:
        acc = 0.0
        for direct_input in direct_inputs:
            acc += nk.approx_bsm_delta(*direct_input)
        return round(acc, 6)

    def _bench_batch() -> float:
        last = []
        for _ in range(args.batch_iters):
            last = nk.approx_bsm_delta_many(329.5, strikes, 3, "call", vols, 0.045, 0.0)
        return round(sum(last), 6)

    def _bench_resolve_realized() -> float:
        last = 0.0
        for _ in range(args.resolve_realized_iters):
            last = common.resolve_strike(
                strikes,
                329.5,
                "call",
                selection,
                dte_days=3,
                contracts=contracts,
                option_gateway=None,
                trade_date=date(2025, 4, 1),
                realized_vol=0.30,
                risk_free_rate=0.045,
            )
        return last

    def _bench_resolve_lookup() -> float:
        last = 0.0
        for _ in range(args.resolve_lookup_iters):
            last = common.resolve_strike(
                strikes,
                329.5,
                "call",
                selection,
                dte_days=3,
                contracts=contracts,
                option_gateway=gateway,
                trade_date=date(2025, 4, 1),
                delta_lookup=delta_lookup,
                risk_free_rate=0.045,
            )
        return last

    results: dict[str, object] = {
        "meta": {
            "direct_calls": args.direct_calls,
            "batch_iters": args.batch_iters,
            "resolve_realized_iters": args.resolve_realized_iters,
            "resolve_lookup_iters": args.resolve_lookup_iters,
            "contracts_per_iter": len(contracts),
            "strategies_module": strategies_pkg.__name__,
        }
    }

    for mode in ("python", "ctypes", "pyo3"):
        loader = _loader_for_mode(mode)
        results[f"direct_{mode}"] = _timed(lambda loader=loader: _with_loader(loader, _bench_direct), rounds=args.rounds)
        results[f"batch_{mode}"] = _timed(lambda loader=loader: _with_loader(loader, _bench_batch), rounds=args.rounds)
        results[f"resolve_realized_{mode}"] = _timed(
            lambda loader=loader: _with_loader(loader, _bench_resolve_realized),
            rounds=args.rounds,
        )
        results[f"resolve_lookup_{mode}"] = _timed(
            lambda loader=loader: _with_loader(loader, _bench_resolve_lookup),
            rounds=args.rounds,
        )

    python_baseline = {
        "direct": float(results["direct_python"]["mean_s"]),  # type: ignore[index]
        "batch": float(results["batch_python"]["mean_s"]),  # type: ignore[index]
        "resolve_realized": float(results["resolve_realized_python"]["mean_s"]),  # type: ignore[index]
        "resolve_lookup": float(results["resolve_lookup_python"]["mean_s"]),  # type: ignore[index]
    }
    results["speedups_vs_python"] = {
        "ctypes_direct": python_baseline["direct"] / float(results["direct_ctypes"]["mean_s"]),  # type: ignore[index]
        "pyo3_direct": python_baseline["direct"] / float(results["direct_pyo3"]["mean_s"]),  # type: ignore[index]
        "ctypes_batch": python_baseline["batch"] / float(results["batch_ctypes"]["mean_s"]),  # type: ignore[index]
        "pyo3_batch": python_baseline["batch"] / float(results["batch_pyo3"]["mean_s"]),  # type: ignore[index]
        "ctypes_resolve_realized": python_baseline["resolve_realized"] / float(results["resolve_realized_ctypes"]["mean_s"]),  # type: ignore[index]
        "pyo3_resolve_realized": python_baseline["resolve_realized"] / float(results["resolve_realized_pyo3"]["mean_s"]),  # type: ignore[index]
        "ctypes_resolve_lookup": python_baseline["resolve_lookup"] / float(results["resolve_lookup_ctypes"]["mean_s"]),  # type: ignore[index]
        "pyo3_resolve_lookup": python_baseline["resolve_lookup"] / float(results["resolve_lookup_pyo3"]["mean_s"]),  # type: ignore[index]
    }

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
