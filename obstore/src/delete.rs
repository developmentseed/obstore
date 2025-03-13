use futures::{StreamExt, TryStreamExt};
use pyo3::prelude::*;
use pyo3_object_store::{PyObjectStore, PyObjectStoreError, PyObjectStoreResult};

use crate::path::PyPaths;
use crate::runtime::get_runtime;
use crate::utils::PyNone;

#[pyfunction]
pub(crate) fn delete(py: Python, store: PyObjectStore, paths: PyPaths) -> PyObjectStoreResult<()> {
    let runtime = get_runtime(py)?;
    let store = store.into_inner();
    py.allow_threads(|| {
        match paths {
            PyPaths::One(path) => {
                runtime.block_on(store.delete(&path))?;
            }
            PyPaths::Many(paths) => {
                // TODO: add option to allow some errors here?
                let stream =
                    store.delete_stream(futures::stream::iter(paths.into_iter().map(Ok)).boxed());
                runtime.block_on(stream.try_collect::<Vec<_>>())?;
            }
        };
        Ok::<_, PyObjectStoreError>(())
    })
}

#[pyfunction]
pub(crate) fn delete_async(
    py: Python,
    store: PyObjectStore,
    paths: PyPaths,
) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        match paths {
            PyPaths::One(path) => {
                store
                    .delete(&path)
                    .await
                    .map_err(PyObjectStoreError::ObjectStoreError)?;
            }
            PyPaths::Many(paths) => {
                // TODO: add option to allow some errors here?
                let stream =
                    store.delete_stream(futures::stream::iter(paths.into_iter().map(Ok)).boxed());
                stream
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(PyObjectStoreError::ObjectStoreError)?;
            }
        }
        Ok(PyNone)
    })
}
