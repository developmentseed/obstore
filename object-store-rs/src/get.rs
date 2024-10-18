use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{GetOptions, GetResult, ObjectStore};
use pyo3::exceptions::{PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;
use tokio::sync::Mutex;

use crate::list::PyObjectMeta;
use crate::runtime::get_runtime;

#[derive(FromPyObject)]
pub(crate) struct PyGetOptions {
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<DateTime<Utc>>,
    if_unmodified_since: Option<DateTime<Utc>>,
    // TODO:
    // range: Option<Range<usize>>,
    version: Option<String>,
    head: bool,
}

impl From<PyGetOptions> for GetOptions {
    fn from(value: PyGetOptions) -> Self {
        Self {
            if_match: value.if_match,
            if_none_match: value.if_none_match,
            if_modified_since: value.if_modified_since,
            if_unmodified_since: value.if_unmodified_since,
            range: None,
            version: value.version,
            head: value.head,
        }
    }
}

#[pyclass(name = "GetResult")]
pub(crate) struct PyGetResult(Option<GetResult>);

impl PyGetResult {
    fn new(result: GetResult) -> Self {
        Self(Some(result))
    }
}

#[pymethods]
impl PyGetResult {
    fn bytes(&mut self, py: Python) -> PyObjectStoreResult<PyBytesWrapper> {
        let get_result = self
            .0
            .take()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        let runtime = get_runtime(py)?;
        py.allow_threads(|| {
            let bytes = runtime.block_on(get_result.bytes())?;
            Ok::<_, PyObjectStoreError>(PyBytesWrapper(bytes))
        })
    }

    fn bytes_async<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let get_result = self
            .0
            .take()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = get_result
                .bytes()
                .await
                .map_err(PyObjectStoreError::ObjectStoreError)?;
            Ok(PyBytesWrapper(bytes))
        })
    }

    #[getter]
    fn meta(&self) -> PyResult<PyObjectMeta> {
        let inner = self
            .0
            .as_ref()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        Ok(PyObjectMeta::new(inner.meta.clone()))
    }

    fn stream(&mut self) -> PyResult<PyBytesStream> {
        let get_result = self
            .0
            .take()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        Ok(PyBytesStream::new(get_result.into_stream()))
    }

    fn __aiter__(&mut self) -> PyResult<PyBytesStream> {
        self.stream()
    }

    fn __iter__(&mut self) -> PyResult<PyBytesStream> {
        self.stream()
    }
}

#[pyclass(name = "BytesStream")]
pub struct PyBytesStream {
    stream: Arc<Mutex<BoxStream<'static, object_store::Result<Bytes>>>>,
}

impl PyBytesStream {
    fn new(stream: BoxStream<'static, object_store::Result<Bytes>>) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

async fn next_stream(
    stream: Arc<Mutex<BoxStream<'static, object_store::Result<Bytes>>>>,
) -> PyResult<PyBytesWrapper> {
    match stream.lock().await.next().await {
        Some(Ok(bytes)) => Ok(PyBytesWrapper(bytes)),
        Some(Err(e)) => Err(PyObjectStoreError::from(e).into()),
        None => Err(PyStopAsyncIteration::new_err("stream exhausted")),
    }
}

#[pymethods]
impl PyBytesStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, next_stream(stream))
    }

    fn __next__<'py>(&'py self, py: Python<'py>) -> PyResult<PyBytesWrapper> {
        let runtime = get_runtime(py)?;
        let stream = self.stream.clone();
        runtime.block_on(next_stream(stream))
    }
}

pub(crate) struct PyBytesWrapper(bytes::Bytes);

// TODO: return buffer protocol object
impl IntoPy<PyObject> for PyBytesWrapper {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyBytes::new_bound(py, &self.0).into_py(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, location, *, options = None))]
pub(crate) fn get(
    py: Python,
    store: PyObjectStore,
    location: String,
    options: Option<PyGetOptions>,
) -> PyObjectStoreResult<PyGetResult> {
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        let path = &location.into();
        let fut = if let Some(options) = options {
            store.as_ref().get_opts(path, options.into())
        } else {
            store.as_ref().get(path)
        };
        let out = runtime.block_on(fut)?;
        Ok::<_, PyObjectStoreError>(PyGetResult::new(out))
    })
}

#[pyfunction]
#[pyo3(signature = (store, location, *, options = None))]
pub(crate) fn get_async(
    py: Python,
    store: PyObjectStore,
    location: String,
    options: Option<PyGetOptions>,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let path = &location.into();
        let fut = if let Some(options) = options {
            store.as_ref().get_opts(path, options.into())
        } else {
            store.as_ref().get(path)
        };
        let out = fut.await.map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyGetResult::new(out))
    })
}

#[pyfunction]
pub(crate) fn get_range(
    py: Python,
    store: PyObjectStore,
    location: String,
    offset: usize,
    length: usize,
) -> PyObjectStoreResult<PyBytesWrapper> {
    let runtime = get_runtime(py)?;
    let range = offset..offset + length;
    py.allow_threads(|| {
        let out = runtime.block_on(store.as_ref().get_range(&location.into(), range))?;
        Ok::<_, PyObjectStoreError>(PyBytesWrapper(out))
    })
}

#[pyfunction]
pub(crate) fn get_range_async(
    py: Python,
    store: PyObjectStore,
    location: String,
    offset: usize,
    length: usize,
) -> PyResult<Bound<PyAny>> {
    let range = offset..offset + length;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = store
            .as_ref()
            .get_range(&location.into(), range)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyBytesWrapper(out))
    })
}

#[pyfunction]
pub(crate) fn get_ranges(
    py: Python,
    store: PyObjectStore,
    location: String,
    offsets: Vec<usize>,
    lengths: Vec<usize>,
) -> PyObjectStoreResult<Vec<PyBytesWrapper>> {
    let runtime = get_runtime(py)?;
    let ranges = offsets
        .into_iter()
        .zip(lengths)
        .map(|(offset, length)| offset..offset + length)
        .collect::<Vec<_>>();
    py.allow_threads(|| {
        let out = runtime.block_on(store.as_ref().get_ranges(&location.into(), &ranges))?;
        Ok::<_, PyObjectStoreError>(out.into_iter().map(PyBytesWrapper).collect())
    })
}

#[pyfunction]
pub(crate) fn get_ranges_async(
    py: Python,
    store: PyObjectStore,
    location: String,
    offsets: Vec<usize>,
    lengths: Vec<usize>,
) -> PyResult<Bound<PyAny>> {
    let ranges = offsets
        .into_iter()
        .zip(lengths)
        .map(|(offset, length)| offset..offset + length)
        .collect::<Vec<_>>();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = store
            .as_ref()
            .get_ranges(&location.into(), &ranges)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(out.into_iter().map(PyBytesWrapper).collect::<Vec<_>>())
    })
}
