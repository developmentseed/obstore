use pyo3::prelude::*;

mod attributes;
mod buffered;
mod copy;
mod delete;
mod get;
mod head;
mod list;
mod path;
mod put;
mod rename;
mod runtime;
mod signer;
mod tags;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// Raise RuntimeWarning for debug builds
#[pyfunction]
fn check_debug_build(_py: Python) -> PyResult<()> {
    #[cfg(debug_assertions)]
    {
        use pyo3::exceptions::PyRuntimeWarning;
        use pyo3::intern;
        use pyo3::types::PyTuple;

        let warnings_mod = _py.import(intern!(_py, "warnings"))?;
        let warning = PyRuntimeWarning::new_err(
            "obstore has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(_py, vec![warning])?;
        warnings_mod.call_method1(intern!(_py, "warn"), args)?;
    }

    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _obstore(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;

    m.add_wrapped(wrap_pyfunction!(___version))?;

    pyo3_object_store::register_store_module(py, m, "obstore")?;
    pyo3_object_store::register_exceptions_module(py, m, "obstore")?;

    m.add_wrapped(wrap_pyfunction!(buffered::open))?;
    m.add_wrapped(wrap_pyfunction!(buffered::open_async))?;
    m.add_wrapped(wrap_pyfunction!(copy::copy_async))?;
    m.add_wrapped(wrap_pyfunction!(copy::copy))?;
    m.add_wrapped(wrap_pyfunction!(delete::delete_async))?;
    m.add_wrapped(wrap_pyfunction!(delete::delete))?;
    m.add_wrapped(wrap_pyfunction!(get::get_async))?;
    m.add_wrapped(wrap_pyfunction!(get::get_range_async))?;
    m.add_wrapped(wrap_pyfunction!(get::get_range))?;
    m.add_wrapped(wrap_pyfunction!(get::get_ranges_async))?;
    m.add_wrapped(wrap_pyfunction!(get::get_ranges))?;
    m.add_wrapped(wrap_pyfunction!(get::get))?;
    m.add_wrapped(wrap_pyfunction!(head::head_async))?;
    m.add_wrapped(wrap_pyfunction!(head::head))?;
    m.add_wrapped(wrap_pyfunction!(list::list_with_delimiter_async))?;
    m.add_wrapped(wrap_pyfunction!(list::list_with_delimiter))?;
    m.add_wrapped(wrap_pyfunction!(list::list))?;
    m.add_wrapped(wrap_pyfunction!(put::put_async))?;
    m.add_wrapped(wrap_pyfunction!(put::put))?;
    m.add_wrapped(wrap_pyfunction!(rename::rename_async))?;
    m.add_wrapped(wrap_pyfunction!(rename::rename))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign_async))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign))?;

    Ok(())
}
