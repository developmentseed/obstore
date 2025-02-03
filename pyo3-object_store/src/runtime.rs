use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use tokio::runtime::Runtime;

static RUNTIME: GILOnceCell<Arc<Runtime>> = GILOnceCell::new();

/// Construct a tokio runtime for sync requests
///
/// This constructs a runtime with default tokio settings (e.g. [`Runtime::new`]).
///
/// This runtime can possibly be used in the store creation process (e.g. in the AWS case, for
/// finding shared credentials), and thus any downstream applications may wish to reuse the same
/// runtime.
///
/// Downstream consumers may explicitly want to depend on tokio and add `rt-multi-thread` as a
/// tokio feature flag to opt-in to the multi-threaded tokio runtime.
pub fn get_runtime(py: Python<'_>) -> PyResult<Arc<Runtime>> {
    let runtime = RUNTIME.get_or_try_init(py, || {
        Ok::<_, PyErr>(Arc::new(Runtime::new().map_err(|err| {
            PyValueError::new_err(format!("Could not create tokio runtime. {}", err))
        })?))
    })?;
    Ok(runtime.clone())
}
