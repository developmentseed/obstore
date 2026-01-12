use std::{future::Future, pin::Pin};

use object_store::ObjectStoreExt;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_object_store::{PyObjectStore, PyObjectStoreError, PyObjectStoreResult};

use crate::utils::PyNone;

#[pyfunction]
#[pyo3(signature = (store, from_, to, *, overwrite=true))]
pub(crate) fn copy(
    py: Python,
    store: PyObjectStore,
    from_: String,
    to: String,
    overwrite: bool,
) -> PyObjectStoreResult<()> {
    let runtime = get_runtime();
    let from_ = from_.into();
    let to = to.into();
    py.detach(|| {
        let fut: Pin<Box<dyn Future<Output = _> + Send>> = if overwrite {
            Box::pin(store.as_ref().copy(&from_, &to))
        } else {
            Box::pin(store.as_ref().copy_if_not_exists(&from_, &to))
        };
        runtime.block_on(fut)?;
        Ok::<_, PyObjectStoreError>(())
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
        let fut: Pin<Box<dyn Future<Output = _> + Send>> = if overwrite {
            Box::pin(store.as_ref().copy(&from_, &to))
        } else {
            Box::pin(store.as_ref().copy_if_not_exists(&from_, &to))
        };
        fut.await.map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyNone)
    })
}
