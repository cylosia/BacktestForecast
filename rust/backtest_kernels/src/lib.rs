use core::ffi::{c_double, c_int, c_uchar};
use core::slice;

pub const CALL_CONTRACT_KIND: c_uchar = 1;
pub const PUT_CONTRACT_KIND: c_uchar = 2;

fn norm_cdf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let z = x.abs() / f64::sqrt(2.0);
    let t = 1.0 / (1.0 + 0.327_591_1 * z);
    let a1 = 0.254_829_592;
    let a2 = -0.284_496_736;
    let a3 = 1.421_413_741;
    let a4 = -1.453_152_027;
    let a5 = 1.061_405_429;
    let erf = sign
        * (1.0
            - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * f64::exp(-(z * z))));
    0.5 * (1.0 + erf)
}

fn contract_kind_is_call(contract_kind: c_uchar) -> Option<bool> {
    match contract_kind {
        CALL_CONTRACT_KIND => Some(true),
        PUT_CONTRACT_KIND => Some(false),
        _ => None,
    }
}

pub fn approx_bsm_delta(
    spot: f64,
    strike: f64,
    dte_days: i32,
    contract_kind: c_uchar,
    vol: f64,
    risk_free_rate: f64,
    dividend_yield: f64,
) -> f64 {
    let Some(is_call) = contract_kind_is_call(contract_kind) else {
        return f64::NAN;
    };

    if dte_days <= 0 {
        if spot == strike {
            return if is_call { 0.5 } else { -0.5 };
        }
        if is_call {
            return if spot > strike { 1.0 } else { 0.0 };
        }
        return if spot < strike { -1.0 } else { 0.0 };
    }

    let t = dte_days as f64 / 365.0;
    let sqrt_t = f64::sqrt(t);
    let d1 = match (f64::ln(spot / strike), vol * sqrt_t) {
        (log_ratio, denom) if denom != 0.0 && log_ratio.is_finite() => {
            (log_ratio + (risk_free_rate - dividend_yield + 0.5 * vol * vol) * t) / denom
        }
        _ => return if contract_kind == CALL_CONTRACT_KIND { 0.5 } else { -0.5 },
    };

    if is_call {
        f64::exp(-dividend_yield * t) * norm_cdf(d1)
    } else {
        f64::exp(-dividend_yield * t) * (norm_cdf(d1) - 1.0)
    }
}

pub fn approx_bsm_delta_many(
    strikes: &[f64],
    contract_kinds: &[c_uchar],
    vols: &[f64],
    spot: f64,
    dte_days: i32,
    risk_free_rate: f64,
    dividend_yield: f64,
) -> Option<Vec<f64>> {
    if strikes.len() != contract_kinds.len() || strikes.len() != vols.len() {
        return None;
    }

    let mut deltas = Vec::with_capacity(strikes.len());
    for idx in 0..strikes.len() {
        deltas.push(approx_bsm_delta(
            spot,
            strikes[idx],
            dte_days,
            contract_kinds[idx],
            vols[idx],
            risk_free_rate,
            dividend_yield,
        ));
    }
    Some(deltas)
}

pub fn choose_delta_target_strike(
    strikes: &[f64],
    deltas: &[f64],
    target_delta: f64,
) -> Option<f64> {
    if strikes.len() != deltas.len() || strikes.is_empty() {
        return None;
    }

    let mut best_strike = strikes[0];
    let mut best_diff = f64::INFINITY;
    for (strike, delta) in strikes.iter().zip(deltas.iter()) {
        let diff = (delta.abs() - target_delta).abs();
        if diff < best_diff {
            best_diff = diff;
            best_strike = *strike;
        }
    }
    Some(best_strike)
}

pub fn resolve_delta_target_strike_from_vols(
    strikes: &[f64],
    contract_kinds: &[c_uchar],
    vols: &[f64],
    spot: f64,
    dte_days: i32,
    target_delta: f64,
    risk_free_rate: f64,
    dividend_yield: f64,
) -> Option<f64> {
    if strikes.len() != contract_kinds.len() || strikes.len() != vols.len() || strikes.is_empty() {
        return None;
    }

    let mut best_strike = strikes[0];
    let mut best_diff = f64::INFINITY;
    for idx in 0..strikes.len() {
        let delta = approx_bsm_delta(
            spot,
            strikes[idx],
            dte_days,
            contract_kinds[idx],
            vols[idx],
            risk_free_rate,
            dividend_yield,
        );
        let diff = (delta.abs() - target_delta).abs();
        if diff < best_diff {
            best_diff = diff;
            best_strike = strikes[idx];
        }
    }
    Some(best_strike)
}

#[no_mangle]
pub extern "C" fn bff_approx_bsm_delta(
    spot: c_double,
    strike: c_double,
    dte_days: c_int,
    contract_kind: c_uchar,
    vol: c_double,
    risk_free_rate: c_double,
    dividend_yield: c_double,
) -> c_double {
    approx_bsm_delta(
        spot,
        strike,
        dte_days,
        contract_kind,
        vol,
        risk_free_rate,
        dividend_yield,
    )
}

#[no_mangle]
pub unsafe extern "C" fn bff_approx_bsm_delta_many(
    strikes_ptr: *const c_double,
    contract_kinds_ptr: *const c_uchar,
    vols_ptr: *const c_double,
    len: usize,
    spot: c_double,
    dte_days: c_int,
    risk_free_rate: c_double,
    dividend_yield: c_double,
    out_ptr: *mut c_double,
) -> usize {
    if strikes_ptr.is_null()
        || contract_kinds_ptr.is_null()
        || vols_ptr.is_null()
        || out_ptr.is_null()
        || len == 0
    {
        return 0;
    }

    let strikes = slice::from_raw_parts(strikes_ptr, len);
    let contract_kinds = slice::from_raw_parts(contract_kinds_ptr, len);
    let vols = slice::from_raw_parts(vols_ptr, len);
    let out = slice::from_raw_parts_mut(out_ptr, len);

    let Some(deltas) = approx_bsm_delta_many(
        strikes,
        contract_kinds,
        vols,
        spot,
        dte_days,
        risk_free_rate,
        dividend_yield,
    ) else {
        return 0;
    };
    for (idx, delta) in deltas.into_iter().enumerate() {
        out[idx] = delta;
    }

    len
}

#[no_mangle]
pub unsafe extern "C" fn bff_choose_delta_target_strike(
    strikes_ptr: *const c_double,
    deltas_ptr: *const c_double,
    len: usize,
    target_delta: c_double,
) -> c_double {
    if strikes_ptr.is_null() || deltas_ptr.is_null() || len == 0 {
        return f64::NAN;
    }
    let strikes = slice::from_raw_parts(strikes_ptr, len);
    let deltas = slice::from_raw_parts(deltas_ptr, len);
    choose_delta_target_strike(strikes, deltas, target_delta).unwrap_or(f64::NAN)
}

#[no_mangle]
pub unsafe extern "C" fn bff_resolve_delta_target_strike_from_vols(
    strikes_ptr: *const c_double,
    contract_kinds_ptr: *const c_uchar,
    vols_ptr: *const c_double,
    len: usize,
    spot: c_double,
    dte_days: c_int,
    target_delta: c_double,
    risk_free_rate: c_double,
    dividend_yield: c_double,
) -> c_double {
    if strikes_ptr.is_null() || contract_kinds_ptr.is_null() || vols_ptr.is_null() || len == 0 {
        return f64::NAN;
    }
    let strikes = slice::from_raw_parts(strikes_ptr, len);
    let contract_kinds = slice::from_raw_parts(contract_kinds_ptr, len);
    let vols = slice::from_raw_parts(vols_ptr, len);
    resolve_delta_target_strike_from_vols(
        strikes,
        contract_kinds,
        vols,
        spot,
        dte_days,
        target_delta,
        risk_free_rate,
        dividend_yield,
    )
    .unwrap_or(f64::NAN)
}
