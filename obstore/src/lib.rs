use pyo3::prelude::*;

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

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _obstore(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    pyo3_object_store::register_store_module(py, m, "obstore")?;
    pyo3_object_store::register_exceptions_module(py, m, "obstore")?;

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
