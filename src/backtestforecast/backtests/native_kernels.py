from __future__ import annotations

import ctypes
import importlib
import math
import os
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace


_NATIVE_KERNEL_MODULE: object = None
_MISSING = object()
_CALL_CONTRACT_KIND = 1
_PUT_CONTRACT_KIND = 2
_MIN_NATIVE_CHOOSE_DELTA_TARGET_STRIKE_LEN = 128


def _contract_kind(contract_type: str) -> int:
    lowered = contract_type.lower()
    if lowered == "call":
        return _CALL_CONTRACT_KIND
    if lowered == "put":
        return _PUT_CONTRACT_KIND
    raise ValueError(f"Unsupported contract type for native kernel: {contract_type!r}")


def _native_library_candidates() -> list[Path]:
    env_override = os.environ.get("BFF_NATIVE_KERNELS_DLL")
    candidates: list[Path] = []
    if env_override:
        candidates.append(Path(env_override))
    package_root = Path(__file__).resolve().parents[1]
    if os.name == "nt":
        candidates.append(package_root / "_backtest_kernels_native.dll")
    elif os.name == "posix":
        candidates.append(package_root / "libbacktest_kernels_native.dylib")
        candidates.append(package_root / "libbacktest_kernels_native.so")
    else:
        candidates.append(package_root / "libbacktest_kernels_native.so")
    return candidates


def _load_ctypes_native_kernel() -> object | None:
    for candidate in _native_library_candidates():
        if not candidate.exists():
            continue
        native_lib = ctypes.CDLL(str(candidate))
        approx_impl = native_lib.bff_approx_bsm_delta
        approx_impl.argtypes = [
            ctypes.c_double,
            ctypes.c_double,
            ctypes.c_int32,
            ctypes.c_uint8,
            ctypes.c_double,
            ctypes.c_double,
            ctypes.c_double,
        ]
        approx_impl.restype = ctypes.c_double

        try:
            approx_many_impl = native_lib.bff_approx_bsm_delta_many
        except AttributeError:
            approx_many_impl = None
        else:
            approx_many_impl.argtypes = [
                ctypes.POINTER(ctypes.c_double),
                ctypes.POINTER(ctypes.c_uint8),
                ctypes.POINTER(ctypes.c_double),
                ctypes.c_size_t,
                ctypes.c_double,
                ctypes.c_int32,
                ctypes.c_double,
                ctypes.c_double,
                ctypes.POINTER(ctypes.c_double),
            ]
            approx_many_impl.restype = ctypes.c_size_t

        choose_impl = native_lib.bff_choose_delta_target_strike
        choose_impl.argtypes = [
            ctypes.POINTER(ctypes.c_double),
            ctypes.POINTER(ctypes.c_double),
            ctypes.c_size_t,
            ctypes.c_double,
        ]
        choose_impl.restype = ctypes.c_double

        try:
            resolve_impl = native_lib.bff_resolve_delta_target_strike_from_vols
        except AttributeError:
            resolve_impl = None
        else:
            resolve_impl.argtypes = [
                ctypes.POINTER(ctypes.c_double),
                ctypes.POINTER(ctypes.c_uint8),
                ctypes.POINTER(ctypes.c_double),
                ctypes.c_size_t,
                ctypes.c_double,
                ctypes.c_int32,
                ctypes.c_double,
                ctypes.c_double,
                ctypes.c_double,
            ]
            resolve_impl.restype = ctypes.c_double

        def _approx_bsm_delta(
            spot: float,
            strike: float,
            dte_days: int,
            contract_type: str,
            vol: float,
            risk_free_rate: float,
            dividend_yield: float,
        ) -> float:
            return float(
                approx_impl(
                    spot,
                    strike,
                    dte_days,
                    _contract_kind(contract_type),
                    vol,
                    risk_free_rate,
                    dividend_yield,
                )
            )

        def _approx_bsm_delta_many(
            spot: float,
            strikes: Sequence[float],
            dte_days: int,
            contract_types: Sequence[str] | str,
            vols: Sequence[float],
            risk_free_rate: float,
            dividend_yield: float,
        ) -> list[float]:
            strike_values = list(strikes)
            vol_values = list(vols)
            if len(strike_values) != len(vol_values):
                raise ValueError("strikes and vols must have the same length")
            if not strike_values:
                return []
            if isinstance(contract_types, str):
                if approx_many_impl is None:
                    return [
                        _approx_bsm_delta(
                            spot,
                            strike,
                            dte_days,
                            contract_types,
                            vol,
                            risk_free_rate,
                            dividend_yield,
                        )
                        for strike, vol in zip(strike_values, vol_values, strict=True)
                    ]
                contract_kind_values = [_contract_kind(contract_types)] * len(strike_values)
            else:
                contract_type_values = list(contract_types)
                if len(strike_values) != len(contract_type_values):
                    raise ValueError("strikes and contract_types must have the same length")
                if approx_many_impl is None:
                    return [
                        _approx_bsm_delta(
                            spot,
                            strike,
                            dte_days,
                            contract_type,
                            vol,
                            risk_free_rate,
                            dividend_yield,
                        )
                        for strike, contract_type, vol in zip(
                            strike_values,
                            contract_type_values,
                            vol_values,
                            strict=True,
                        )
                    ]
                contract_kind_values = [_contract_kind(contract_type) for contract_type in contract_type_values]
            strike_buffer = (ctypes.c_double * len(strike_values))(*strike_values)
            contract_kind_buffer = (ctypes.c_uint8 * len(contract_kind_values))(*contract_kind_values)
            vol_buffer = (ctypes.c_double * len(vol_values))(*vol_values)
            output_buffer = (ctypes.c_double * len(strike_values))()
            processed = int(
                approx_many_impl(
                    strike_buffer,
                    contract_kind_buffer,
                    vol_buffer,
                    len(strike_values),
                    spot,
                    dte_days,
                    risk_free_rate,
                    dividend_yield,
                    output_buffer,
                )
            )
            if processed != len(strike_values):
                raise ValueError("native kernel did not process the expected number of deltas")
            return [float(value) for value in output_buffer]

        def _choose_delta_target_strike(
            strikes: Sequence[float],
            deltas: Sequence[float],
            target_delta: float,
        ) -> float:
            strike_values = list(strikes)
            delta_values = list(deltas)
            if len(strike_values) != len(delta_values):
                raise ValueError("strikes and deltas must have the same length")
            if not strike_values:
                raise ValueError("strikes must not be empty")
            strike_buffer = (ctypes.c_double * len(strike_values))(*strike_values)
            delta_buffer = (ctypes.c_double * len(delta_values))(*delta_values)
            result = float(
                choose_impl(
                    strike_buffer,
                    delta_buffer,
                    len(strike_values),
                    target_delta,
                )
            )
            if math.isnan(result):
                raise ValueError("native kernel returned NaN for delta target strike")
            return result

        def _resolve_delta_target_strike_from_vols(
            spot: float,
            strikes: Sequence[float],
            dte_days: int,
            contract_types: Sequence[str] | str,
            vols: Sequence[float],
            target_delta: float,
            risk_free_rate: float,
            dividend_yield: float,
        ) -> float:
            strike_values = list(strikes)
            vol_values = list(vols)
            if len(strike_values) != len(vol_values):
                raise ValueError("strikes and vols must have the same length")
            if not strike_values:
                raise ValueError("strikes must not be empty")
            if isinstance(contract_types, str):
                contract_kind_values = [_contract_kind(contract_types)] * len(strike_values)
            else:
                contract_type_values = list(contract_types)
                if len(strike_values) != len(contract_type_values):
                    raise ValueError("strikes and contract_types must have the same length")
                contract_kind_values = [_contract_kind(contract_type) for contract_type in contract_type_values]
            if resolve_impl is None:
                deltas = _approx_bsm_delta_many(
                    spot,
                    strike_values,
                    dte_days,
                    contract_types,
                    vol_values,
                    risk_free_rate,
                    dividend_yield,
                )
                return _choose_delta_target_strike(strike_values, deltas, target_delta)
            strike_buffer = (ctypes.c_double * len(strike_values))(*strike_values)
            contract_kind_buffer = (ctypes.c_uint8 * len(contract_kind_values))(*contract_kind_values)
            vol_buffer = (ctypes.c_double * len(vol_values))(*vol_values)
            result = float(
                resolve_impl(
                    strike_buffer,
                    contract_kind_buffer,
                    vol_buffer,
                    len(strike_values),
                    spot,
                    dte_days,
                    target_delta,
                    risk_free_rate,
                    dividend_yield,
                )
            )
            if math.isnan(result):
                raise ValueError("native kernel returned NaN for delta-target resolver")
            return result

        return SimpleNamespace(
            approx_bsm_delta=_approx_bsm_delta,
            approx_bsm_delta_many=_approx_bsm_delta_many,
            choose_delta_target_strike=_choose_delta_target_strike,
            resolve_delta_target_strike_from_vols=_resolve_delta_target_strike_from_vols,
            source=str(candidate),
        )
    return None


def _load_native_kernel_module() -> object | None:
    global _NATIVE_KERNEL_MODULE
    if _NATIVE_KERNEL_MODULE is _MISSING:
        try:
            _NATIVE_KERNEL_MODULE = importlib.import_module("backtestforecast._backtest_kernels")
        except ImportError:
            _NATIVE_KERNEL_MODULE = _load_ctypes_native_kernel()
    if _NATIVE_KERNEL_MODULE is None or _NATIVE_KERNEL_MODULE is _MISSING:
        return None
    return _NATIVE_KERNEL_MODULE


def reset_native_kernel_module_cache() -> None:
    global _NATIVE_KERNEL_MODULE
    _NATIVE_KERNEL_MODULE = _MISSING


def _python_norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _python_approx_bsm_delta(
    spot: float,
    strike: float,
    dte_days: int,
    contract_type: str,
    vol: float = 0.30,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> float:
    if dte_days <= 0:
        if spot == strike:
            return 0.5 if contract_type == "call" else -0.5
        if contract_type == "call":
            return 1.0 if spot > strike else 0.0
        return -1.0 if spot < strike else 0.0

    t = dte_days / 365.0
    sqrt_t = math.sqrt(t)
    try:
        d1 = (math.log(spot / strike) + (risk_free_rate - dividend_yield + 0.5 * vol * vol) * t) / (vol * sqrt_t)
    except (ValueError, ZeroDivisionError):
        return 0.5 if contract_type == "call" else -0.5

    if contract_type == "call":
        return math.exp(-dividend_yield * t) * _python_norm_cdf(d1)
    return math.exp(-dividend_yield * t) * (_python_norm_cdf(d1) - 1.0)


def approx_bsm_delta(
    spot: float,
    strike: float,
    dte_days: int,
    contract_type: str,
    vol: float = 0.30,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> float:
    native = _load_native_kernel_module()
    if native is not None:
        native_impl = getattr(native, "approx_bsm_delta", None)
        if callable(native_impl):
            return float(
                native_impl(
                    spot,
                    strike,
                    dte_days,
                    contract_type,
                    vol,
                    risk_free_rate,
                    dividend_yield,
                )
            )
    return _python_approx_bsm_delta(
        spot,
        strike,
        dte_days,
        contract_type,
        vol=vol,
        risk_free_rate=risk_free_rate,
        dividend_yield=dividend_yield,
    )


def approx_bsm_delta_many(
    spot: float,
    strikes: Sequence[float],
    dte_days: int,
    contract_types: Sequence[str] | str,
    vols: Sequence[float],
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> list[float]:
    strike_values = list(strikes)
    vol_values = list(vols)
    if len(strike_values) != len(vol_values):
        raise ValueError("strikes and vols must have the same length")
    if isinstance(contract_types, str):
        contract_type_values = [contract_types] * len(strike_values)
    else:
        contract_type_values = list(contract_types)
        if len(strike_values) != len(contract_type_values):
            raise ValueError("strikes and contract_types must have the same length")
    native = _load_native_kernel_module()
    if native is not None:
        native_impl = getattr(native, "approx_bsm_delta_many", None)
        if callable(native_impl):
            return [
                float(delta)
                for delta in native_impl(
                    spot,
                    strike_values,
                    dte_days,
                    contract_type_values,
                    vol_values,
                    risk_free_rate,
                    dividend_yield,
                )
            ]
    return [
        _python_approx_bsm_delta(
            spot,
            strike,
            dte_days,
            contract_type,
            vol=vol,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
        )
        for strike, contract_type, vol in zip(strike_values, contract_type_values, vol_values, strict=True)
    ]


def _python_resolve_delta_target_strike_from_vols(
    spot: float,
    strikes: Sequence[float],
    dte_days: int,
    contract_types: Sequence[str] | str,
    vols: Sequence[float],
    target_delta: float,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> float:
    strike_values = list(strikes)
    vol_values = list(vols)
    if isinstance(contract_types, str):
        contract_type_values = [contract_types] * len(strike_values)
    else:
        contract_type_values = list(contract_types)
    deltas = [
        _python_approx_bsm_delta(
            spot,
            strike,
            dte_days,
            contract_type,
            vol=vol,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
        )
        for strike, contract_type, vol in zip(strike_values, contract_type_values, vol_values, strict=True)
    ]
    return _python_choose_delta_target_strike(strikes, deltas, target_delta)


def _python_choose_delta_target_strike(
    strikes: Sequence[float],
    deltas: Sequence[float],
    target_delta: float,
) -> float:
    if len(strikes) != len(deltas):
        raise ValueError("strikes and deltas must have the same length")
    if not strikes:
        raise ValueError("strikes must not be empty")
    best_strike = strikes[0]
    best_diff = float("inf")
    for strike, delta in zip(strikes, deltas, strict=True):
        diff = abs(abs(delta) - target_delta)
        if diff < best_diff:
            best_diff = diff
            best_strike = strike
    return float(best_strike)


def choose_delta_target_strike(
    strikes: Sequence[float],
    deltas: Sequence[float],
    target_delta: float,
) -> float:
    native = _load_native_kernel_module()
    if native is not None:
        native_impl = getattr(native, "choose_delta_target_strike", None)
        if callable(native_impl):
            if getattr(native, "source", None) is not None and len(strikes) < _MIN_NATIVE_CHOOSE_DELTA_TARGET_STRIKE_LEN:
                return _python_choose_delta_target_strike(strikes, deltas, target_delta)
            return float(native_impl(list(strikes), list(deltas), target_delta))
    return _python_choose_delta_target_strike(strikes, deltas, target_delta)


def resolve_delta_target_strike_from_vols(
    spot: float,
    strikes: Sequence[float],
    dte_days: int,
    contract_types: Sequence[str] | str,
    vols: Sequence[float],
    target_delta: float,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> float:
    strike_values = list(strikes)
    vol_values = list(vols)
    if len(strike_values) != len(vol_values):
        raise ValueError("strikes and vols must have the same length")
    if isinstance(contract_types, str):
        contract_type_values = contract_types
    else:
        contract_type_values = list(contract_types)
        if len(strike_values) != len(contract_type_values):
            raise ValueError("strikes and contract_types must have the same length")
    native = _load_native_kernel_module()
    if native is not None:
        native_impl = getattr(native, "resolve_delta_target_strike_from_vols", None)
        if callable(native_impl):
            return float(
                native_impl(
                    spot,
                    strike_values,
                    dte_days,
                    contract_type_values,
                    vol_values,
                    target_delta,
                    risk_free_rate,
                    dividend_yield,
                )
            )
    return _python_resolve_delta_target_strike_from_vols(
        spot,
        strike_values,
        dte_days,
        contract_type_values,
        vol_values,
        target_delta,
        risk_free_rate=risk_free_rate,
        dividend_yield=dividend_yield,
    )


reset_native_kernel_module_cache()
