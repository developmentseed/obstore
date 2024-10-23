use std::sync::Arc;

use futures::stream::{BoxStream, Fuse};
use futures::StreamExt;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::{ListResult, ObjectMeta, ObjectStore};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;
use tokio::sync::Mutex;

use crate::runtime::get_runtime;

pub(crate) struct PyObjectMeta(ObjectMeta);

impl PyObjectMeta {
    pub(crate) fn new(meta: ObjectMeta) -> Self {
        Self(meta)
    }
}

impl IntoPy<PyObject> for PyObjectMeta {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = IndexMap::with_capacity(5);
        // Note, this uses "path" instead of "location" because we standardize the API to accept
        // the keyword "path" everywhere.
        dict.insert("path", self.0.location.as_ref().into_py(py));
        dict.insert("last_modified", self.0.last_modified.into_py(py));
        dict.insert("size", self.0.size.into_py(py));
        dict.insert("e_tag", self.0.e_tag.into_py(py));
        dict.insert("version", self.0.version.into_py(py));
        dict.into_py(py)
    }
}

// Note: we fuse the underlying stream so that we can get `None` multiple times.
//
// In general, you can't poll an iterator after it's already emitted None. But the issue here is
// that we need _two_ states for the Python async iterator. It needs to first get all returned
// results, and then it needs its **own** PyStopAsyncIteration/PyStopIteration. But these are _two_
// results to be returned from the Rust call, and we can't return them both at the same time. The
// easiest way to fix this is to safely return `None` from the stream multiple times. The first
// time we see `None` we return any batched results, the second time we see `None`, there are no
// batched results and we return PyStopAsyncIteration/PyStopIteration.
//
// Note: another way we could solve this is by removing any batching from the stream, but batching
// should improve the performance of the Rust/Python bridge.
//
// Ref:
// - https://stackoverflow.com/a/66964599
// - https://docs.rs/futures/latest/futures/prelude/stream/trait.StreamExt.html#method.fuse
#[pyclass(name = "ListStream")]
pub(crate) struct PyListStream {
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<ObjectMeta>>>>>,
    chunk_size: usize,
}

impl PyListStream {
    fn new(
        stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
        chunk_size: usize,
    ) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream.fuse())),
            chunk_size,
        }
    }
}

#[pymethods]
impl PyListStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn collect(&self, py: Python) -> PyResult<Vec<PyObjectMeta>> {
        let runtime = get_runtime(py)?;
        let stream = self.stream.clone();
        runtime.block_on(collect_stream(stream))
    }

    fn collect_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, collect_stream(stream))
    }

    fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, next_stream(stream, self.chunk_size, false))
    }

    fn __next__<'py>(&'py self, py: Python<'py>) -> PyResult<Vec<PyObjectMeta>> {
        let runtime = get_runtime(py)?;
        let stream = self.stream.clone();
        runtime.block_on(next_stream(stream, self.chunk_size, true))
    }
}

async fn next_stream(
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<ObjectMeta>>>>>,
    chunk_size: usize,
    sync: bool,
) -> PyResult<Vec<PyObjectMeta>> {
    let mut stream = stream.lock().await;
    let mut metas: Vec<PyObjectMeta> = vec![];
    loop {
        match stream.next().await {
            Some(Ok(meta)) => {
                metas.push(PyObjectMeta(meta));
                if metas.len() >= chunk_size {
                    return Ok(metas);
                }
            }
            Some(Err(e)) => return Err(PyObjectStoreError::from(e).into()),
            None => {
                if metas.is_empty() {
                    // Depending on whether the iteration is sync or not, we raise either a
                    // StopIteration or a StopAsyncIteration
                    if sync {
                        return Err(PyStopIteration::new_err("stream exhausted"));
                    } else {
                        return Err(PyStopAsyncIteration::new_err("stream exhausted"));
                    }
                } else {
                    return Ok(metas);
                }
            }
        };
    }
}

async fn collect_stream(
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<ObjectMeta>>>>>,
) -> PyResult<Vec<PyObjectMeta>> {
    let mut stream = stream.lock().await;
    let mut metas: Vec<PyObjectMeta> = vec![];
    loop {
        match stream.next().await {
            Some(Ok(meta)) => {
                metas.push(PyObjectMeta(meta));
            }
            Some(Err(e)) => return Err(PyObjectStoreError::from(e).into()),
            None => {
                return Ok(metas);
            }
        };
    }
}

pub(crate) struct PyListResult(ListResult);

impl IntoPy<PyObject> for PyListResult {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = IndexMap::with_capacity(2);
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
#[pyo3(signature = (store, prefix = None, *, offset = None, chunk_size = 50))]
pub(crate) fn list(
    store: PyObjectStore,
    prefix: Option<String>,
    offset: Option<String>,
    chunk_size: usize,
) -> PyObjectStoreResult<PyListStream> {
    let store = store.into_inner().clone();
    let prefix = prefix.map(|s| s.into());
    let stream = if let Some(offset) = offset {
        store.list_with_offset(prefix.as_ref(), &offset.into())
    } else {
        store.list(prefix.as_ref())
    };
    Ok(PyListStream::new(stream, chunk_size))
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
