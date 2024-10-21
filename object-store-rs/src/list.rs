use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ListResult, ObjectMeta, ObjectStore};
use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;

use crate::runtime::get_runtime;

pub(crate) struct PyObjectMeta(ObjectMeta);

impl PyObjectMeta {
    pub(crate) fn new(meta: ObjectMeta) -> Self {
        Self(meta)
    }
}

impl IntoPy<PyObject> for PyObjectMeta {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = HashMap::new();
        dict.insert("location", self.0.location.as_ref().into_py(py));
        dict.insert("last_modified", self.0.last_modified.into_py(py));
        dict.insert("size", self.0.size.into_py(py));
        dict.insert("e_tag", self.0.e_tag.into_py(py));
        dict.insert("version", self.0.version.into_py(py));
        dict.into_py(py)
    }
}

pub(crate) struct PyListResult(ListResult);

impl IntoPy<PyObject> for PyListResult {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = HashMap::new();
        dict.insert(
            "common_prefixes",
            self.0
                .common_prefixes
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
                .into_py(py),
        );
        dict.insert(
            "objects",
            self.0
                .objects
                .into_iter()
                .map(PyObjectMeta)
                .collect::<Vec<_>>()
                .into_py(py),
        );
        dict.into_py(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None, *, offset = None, max_items = 2000))]
pub(crate) fn list(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
    offset: Option<String>,
    max_items: Option<usize>,
) -> PyObjectStoreResult<Vec<PyObjectMeta>> {
    let store = store.into_inner();
    let prefix = prefix.map(|s| s.into());
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        let stream = if let Some(offset) = offset {
            store.list_with_offset(prefix.as_ref(), &offset.into())
        } else {
            store.list(prefix.as_ref())
        };
        let out = runtime.block_on(materialize_list_stream(stream, max_items))?;
        Ok::<_, PyObjectStoreError>(out)
    })
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None, *, offset = None, max_items = 2000))]
pub(crate) fn list_async(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
    offset: Option<String>,
    max_items: Option<usize>,
) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner();
    let prefix = prefix.map(|s| s.into());

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let stream = if let Some(offset) = offset {
            store.list_with_offset(prefix.as_ref(), &offset.into())
        } else {
            store.list(prefix.as_ref())
        };
        Ok(materialize_list_stream(stream, max_items).await?)
    })
}

async fn materialize_list_stream(
    mut stream: BoxStream<'_, object_store::Result<ObjectMeta>>,
    max_items: Option<usize>,
) -> PyObjectStoreResult<Vec<PyObjectMeta>> {
    let mut result = vec![];
    while let Some(object) = stream.next().await {
        result.push(PyObjectMeta(object?));
        if let Some(max_items) = max_items {
            if result.len() >= max_items {
                return Ok(result);
            }
        }
    }

    Ok(result)
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None))]
pub(crate) fn list_with_delimiter(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
) -> PyObjectStoreResult<PyListResult> {
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        let out = runtime.block_on(list_with_delimiter_materialize(
            store.into_inner(),
            prefix.map(|s| s.into()).as_ref(),
        ))?;
        Ok::<_, PyObjectStoreError>(out)
    })
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None))]
pub(crate) fn list_with_delimiter_async(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out =
            list_with_delimiter_materialize(store.into_inner(), prefix.map(|s| s.into()).as_ref())
                .await?;
        Ok(out)
    })
}

async fn list_with_delimiter_materialize(
    store: Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
) -> PyObjectStoreResult<PyListResult> {
    let list_result = store.list_with_delimiter(prefix).await?;
    Ok(PyListResult(list_result))
}
