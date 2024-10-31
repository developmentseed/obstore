use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use tokio::runtime::Runtime;

static RUNTIME: GILOnceCell<Arc<Runtime>> = GILOnceCell::new();

/// Get the tokio runtime for sync requests
pub(crate) fn get_runtime(py: Python<'_>) -> PyResult<Arc<Runtime>> {
    let runtime = RUNTIME.get_or_try_init(py, || {
        Ok::<_, PyErr>(Arc::new(Runtime::new().map_err(|err| {
            PyValueError::new_err(format!("Could not create tokio runtime. {}", err))
        })?))
    })?;
    Ok(runtime.clone())
}
