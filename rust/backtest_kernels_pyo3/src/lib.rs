use backtest_kernels_native::{
    approx_bsm_delta as approx_bsm_delta_core, approx_bsm_delta_many as approx_bsm_delta_many_core,
    choose_delta_target_strike as choose_delta_target_strike_core,
    resolve_delta_target_strike_from_vols as resolve_delta_target_strike_from_vols_core,
    CALL_CONTRACT_KIND, PUT_CONTRACT_KIND,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};

fn contract_kind_from_type(contract_type: &str) -> PyResult<u8> {
    match contract_type.to_ascii_lowercase().as_str() {
        "call" => Ok(CALL_CONTRACT_KIND),
        "put" => Ok(PUT_CONTRACT_KIND),
        _ => Err(PyValueError::new_err(format!(
            "Unsupported contract type for native kernel: {contract_type:?}"
        ))),
    }
}

fn contract_kinds_from_object(contract_types: &Bound<'_, PyAny>, len: usize) -> PyResult<Vec<u8>> {
    if let Ok(contract_type) = contract_types.extract::<String>() {
        return Ok(vec![contract_kind_from_type(&contract_type)?; len]);
    }

    let contract_type_values = contract_types.extract::<Vec<String>>().map_err(|_| {
        PyTypeError::new_err("contract_types must be a string or a sequence of strings")
    })?;
    if contract_type_values.len() != len {
        return Err(PyValueError::new_err(
            "strikes and contract_types must have the same length",
        ));
    }
    contract_type_values
        .iter()
        .map(|contract_type| contract_kind_from_type(contract_type))
        .collect()
}

#[pyfunction(signature = (spot, strike, dte_days, contract_type, vol = 0.30, risk_free_rate = 0.045, dividend_yield = 0.0))]
fn approx_bsm_delta(
    spot: f64,
    strike: f64,
    dte_days: i32,
    contract_type: &str,
    vol: f64,
    risk_free_rate: f64,
    dividend_yield: f64,
) -> PyResult<f64> {
    Ok(approx_bsm_delta_core(
        spot,
        strike,
        dte_days,
        contract_kind_from_type(contract_type)?,
        vol,
        risk_free_rate,
        dividend_yield,
    ))
}

#[pyfunction(signature = (spot, strikes, dte_days, contract_types, vols, risk_free_rate = 0.045, dividend_yield = 0.0))]
fn approx_bsm_delta_many(
    spot: f64,
    strikes: Vec<f64>,
    dte_days: i32,
    contract_types: &Bound<'_, PyAny>,
    vols: Vec<f64>,
    risk_free_rate: f64,
    dividend_yield: f64,
) -> PyResult<Vec<f64>> {
    if strikes.len() != vols.len() {
        return Err(PyValueError::new_err("strikes and vols must have the same length"));
    }
    let contract_kinds = contract_kinds_from_object(contract_types, strikes.len())?;
    approx_bsm_delta_many_core(
        &strikes,
        &contract_kinds,
        &vols,
        spot,
        dte_days,
        risk_free_rate,
        dividend_yield,
    )
    .ok_or_else(|| PyValueError::new_err("invalid batch delta inputs"))
}

#[pyfunction]
fn choose_delta_target_strike(
    strikes: Vec<f64>,
    deltas: Vec<f64>,
    target_delta: f64,
) -> PyResult<f64> {
    choose_delta_target_strike_core(&strikes, &deltas, target_delta).ok_or_else(|| {
        PyValueError::new_err("strikes and deltas must have the same length and not be empty")
    })
}

#[pyfunction(signature = (spot, strikes, dte_days, contract_types, vols, target_delta, risk_free_rate = 0.045, dividend_yield = 0.0))]
fn resolve_delta_target_strike_from_vols(
    spot: f64,
    strikes: Vec<f64>,
    dte_days: i32,
    contract_types: &Bound<'_, PyAny>,
    vols: Vec<f64>,
    target_delta: f64,
    risk_free_rate: f64,
    dividend_yield: f64,
) -> PyResult<f64> {
    if strikes.len() != vols.len() {
        return Err(PyValueError::new_err("strikes and vols must have the same length"));
    }
    let contract_kinds = contract_kinds_from_object(contract_types, strikes.len())?;
    resolve_delta_target_strike_from_vols_core(
        &strikes,
        &contract_kinds,
        &vols,
        spot,
        dte_days,
        target_delta,
        risk_free_rate,
        dividend_yield,
    )
    .ok_or_else(|| PyValueError::new_err("invalid delta-target resolver inputs"))
}

#[pymodule]
fn _backtest_kernels(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(approx_bsm_delta, m)?)?;
    m.add_function(wrap_pyfunction!(approx_bsm_delta_many, m)?)?;
    m.add_function(wrap_pyfunction!(choose_delta_target_strike, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_delta_target_strike_from_vols, m)?)?;
    Ok(())
}
