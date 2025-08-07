use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_object_store::{PyObjectStore, PyObjectStoreError, PyObjectStoreResult, PyPath};

use crate::list::PyObjectMeta;

#[pyfunction]
pub fn head(py: Python, store: PyObjectStore, path: PyPath) -> PyObjectStoreResult<PyObjectMeta> {
    let runtime = get_runtime();
    let store = store.into_inner();

    py.allow_threads(|| {
        let meta = runtime.block_on(store.head(path.as_ref()))?;
        Ok::<_, PyObjectStoreError>(PyObjectMeta::new(meta))
    })
}

#[pyfunction]
pub fn head_async(py: Python, store: PyObjectStore, path: PyPath) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner().clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let meta = store
            .head(path.as_ref())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyObjectMeta::new(meta))
    })
}
