use pyo3::prelude::*;
use pyo3_object_store::{get_runtime, PyObjectStore, PyObjectStoreError, PyObjectStoreResult};

use crate::list::PyObjectMeta;

#[pyfunction]
pub fn head(py: Python, store: PyObjectStore, path: String) -> PyObjectStoreResult<PyObjectMeta> {
    let runtime = get_runtime(py)?;
    let store = store.into_inner();

    py.allow_threads(|| {
        let meta = runtime.block_on(store.head(&path.into()))?;
        Ok::<_, PyObjectStoreError>(PyObjectMeta::new(meta))
    })
}

#[pyfunction]
pub fn head_async(py: Python, store: PyObjectStore, path: String) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner().clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let meta = store
            .head(&path.into())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyObjectMeta::new(meta))
    })
}
