use object_store::ObjectStore;
use pyo3::prelude::*;
use pyo3_object_store::PyObjectStore;

use crate::error::{PyObstoreError, PyObstoreResult};
use crate::runtime::get_runtime;
use crate::utils::PyNone;

#[pyfunction]
#[pyo3(signature = (store, from_, to, *, overwrite=true))]
pub(crate) fn copy(
    py: Python,
    store: PyObjectStore,
    from_: String,
    to: String,
    overwrite: bool,
) -> PyObstoreResult<()> {
    let runtime = get_runtime(py)?;
    let from_ = from_.into();
    let to = to.into();
    py.allow_threads(|| {
        let fut = if overwrite {
            store.as_ref().copy(&from_, &to)
        } else {
            store.as_ref().copy_if_not_exists(&from_, &to)
        };
        runtime.block_on(fut)?;
        Ok::<_, PyObstoreError>(())
    })
}

#[pyfunction]
#[pyo3(signature = (store, from_, to, *, overwrite=true))]
pub(crate) fn copy_async(
    py: Python,
    store: PyObjectStore,
    from_: String,
    to: String,
    overwrite: bool,
) -> PyResult<Bound<PyAny>> {
    let from_ = from_.into();
    let to = to.into();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fut = if overwrite {
            store.as_ref().copy(&from_, &to)
        } else {
            store.as_ref().copy_if_not_exists(&from_, &to)
        };
        fut.await.map_err(PyObstoreError::from)?;
        Ok(PyNone)
    })
}
