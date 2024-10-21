use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;

use crate::runtime::get_runtime;

pub(crate) enum PyLocations {
    One(Path),
    // TODO: also support an Arrow String Array here.
    Many(Vec<Path>),
}

impl<'py> FromPyObject<'py> for PyLocations {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path) = ob.extract::<String>() {
            Ok(Self::One(path.into()))
        } else if let Ok(paths) = ob.extract::<Vec<String>>() {
            Ok(Self::Many(
                paths.into_iter().map(|path| path.into()).collect(),
            ))
        } else {
            Err(PyTypeError::new_err(
                "Expected string path or sequence of string paths.",
            ))
        }
    }
}

#[pyfunction]
pub(crate) fn delete(
    py: Python,
    store: PyObjectStore,
    locations: PyLocations,
) -> PyObjectStoreResult<()> {
    let runtime = get_runtime(py)?;
    let store = store.into_inner();
    py.allow_threads(|| {
        match locations {
            PyLocations::One(path) => {
                runtime.block_on(store.delete(&path))?;
            }
            PyLocations::Many(paths) => {
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
    locations: PyLocations,
) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        match locations {
            PyLocations::One(path) => {
                store
                    .delete(&path)
                    .await
                    .map_err(PyObjectStoreError::ObjectStoreError)?;
            }
            PyLocations::Many(paths) => {
                // TODO: add option to allow some errors here?
                let stream =
                    store.delete_stream(futures::stream::iter(paths.into_iter().map(Ok)).boxed());
                stream
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(PyObjectStoreError::ObjectStoreError)?;
            }
        }
        Ok(())
    })
}
