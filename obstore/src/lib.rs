// Except for explicit areas where we enable unsafe
#![deny(unsafe_code)]

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
mod scheme;
mod signer;
mod tags;
mod utils;

use pyo3::prelude::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const OBJECT_STORE_VERSION: &str = env!("OBJECT_STORE_VERSION");
const OBJECT_STORE_SOURCE: &str = env!("OBJECT_STORE_SOURCE");

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

    m.add("__version__", VERSION)?;
    m.add("_object_store_version", OBJECT_STORE_VERSION)?;
    m.add("_object_store_source", OBJECT_STORE_SOURCE)?;

    pyo3_object_store::register_store_module(py, m, "obstore", "_store")?;
    pyo3_object_store::register_exceptions_module(py, m, "obstore", "exceptions")?;

    m.add_class::<pyo3_bytes::PyBytes>()?;
    // Set the value of `__module__` correctly on PyBytes
    m.getattr("Bytes")?.setattr("__module__", "obstore")?;

    m.add_wrapped(wrap_pyfunction!(buffered::open_reader))?;
    m.add_wrapped(wrap_pyfunction!(buffered::open_reader_async))?;
    m.add_wrapped(wrap_pyfunction!(buffered::open_writer))?;
    m.add_wrapped(wrap_pyfunction!(buffered::open_writer_async))?;
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
    m.add_wrapped(wrap_pyfunction!(scheme::parse_scheme))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign_async))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign))?;

    Ok(())
}
