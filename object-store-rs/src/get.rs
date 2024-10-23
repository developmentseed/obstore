use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, Fuse};
use futures::StreamExt;
use object_store::{GetOptions, GetResult, ObjectStore};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;
use tokio::sync::Mutex;

use crate::list::PyObjectMeta;
use crate::runtime::get_runtime;

/// 10MB default chunk size
const DEFAULT_BYTES_CHUNK_SIZE: usize = 10 * 1024 * 1024;

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
            Ok::<_, PyObjectStoreError>(PyBytesWrapper::new(bytes))
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
            Ok(PyBytesWrapper::new(bytes))
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

    #[pyo3(signature = (min_chunk_size = DEFAULT_BYTES_CHUNK_SIZE))]
    fn stream(&mut self, min_chunk_size: usize) -> PyResult<PyBytesStream> {
        let get_result = self
            .0
            .take()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        Ok(PyBytesStream::new(get_result.into_stream(), min_chunk_size))
    }

    fn __aiter__(&mut self) -> PyResult<PyBytesStream> {
        self.stream(DEFAULT_BYTES_CHUNK_SIZE)
    }

    fn __iter__(&mut self) -> PyResult<PyBytesStream> {
        self.stream(DEFAULT_BYTES_CHUNK_SIZE)
    }
}

// Note: we fuse the underlying stream so that we can get `None` multiple times.
// See the note on PyListStream for more background.
#[pyclass(name = "BytesStream")]
pub struct PyBytesStream {
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<Bytes>>>>>,
    min_chunk_size: usize,
}

impl PyBytesStream {
    fn new(stream: BoxStream<'static, object_store::Result<Bytes>>, min_chunk_size: usize) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream.fuse())),
            min_chunk_size,
        }
    }
}

async fn next_stream(
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<Bytes>>>>>,
    min_chunk_size: usize,
    sync: bool,
) -> PyResult<PyBytesWrapper> {
    let mut stream = stream.lock().await;
    let mut buffers: Vec<Bytes> = vec![];
    loop {
        match stream.next().await {
            Some(Ok(bytes)) => {
                buffers.push(bytes);
                let total_buffer_len = buffers.iter().fold(0, |acc, buf| acc + buf.len());
                if total_buffer_len >= min_chunk_size {
                    return Ok(PyBytesWrapper::new_multiple(buffers));
                }
            }
            Some(Err(e)) => return Err(PyObjectStoreError::from(e).into()),
            None => {
                if buffers.is_empty() {
                    // Depending on whether the iteration is sync or not, we raise either a
                    // StopIteration or a StopAsyncIteration
                    if sync {
                        return Err(PyStopIteration::new_err("stream exhausted"));
                    } else {
                        return Err(PyStopAsyncIteration::new_err("stream exhausted"));
                    }
                } else {
                    return Ok(PyBytesWrapper::new_multiple(buffers));
                }
            }
        };
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
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            next_stream(stream, self.min_chunk_size, false),
        )
    }

    fn __next__<'py>(&'py self, py: Python<'py>) -> PyResult<PyBytesWrapper> {
        let runtime = get_runtime(py)?;
        let stream = self.stream.clone();
        runtime.block_on(next_stream(stream, self.min_chunk_size, true))
    }
}

pub(crate) struct PyBytesWrapper(Vec<Bytes>);

impl PyBytesWrapper {
    pub fn new(buf: Bytes) -> Self {
        Self(vec![buf])
    }

    pub fn new_multiple(buffers: Vec<Bytes>) -> Self {
        Self(buffers)
    }
}

// TODO: return buffer protocol object? This isn't possible on an array of Bytes, so if you want to
// support the buffer protocol in the future (e.g. for get_range) you may need to have a separate
// wrapper of Bytes
impl IntoPy<PyObject> for PyBytesWrapper {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let total_len = self.0.iter().fold(0, |acc, buf| acc + buf.len());
        // Copy all internal Bytes objects into a single PyBytes
        // Since our inner callback is infallible, this will only panic on out of memory
        PyBytes::new_bound_with(py, total_len, |target| {
            let mut offset = 0;
            for buf in self.0.iter() {
                target[offset..offset + buf.len()].copy_from_slice(buf);
                offset += buf.len();
            }
            Ok(())
        })
        .unwrap()
        .into_py(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, *, options = None))]
pub(crate) fn get(
    py: Python,
    store: PyObjectStore,
    path: String,
    options: Option<PyGetOptions>,
) -> PyObjectStoreResult<PyGetResult> {
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        let path = &path.into();
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
#[pyo3(signature = (store, path, *, options = None))]
pub(crate) fn get_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    options: Option<PyGetOptions>,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let path = &path.into();
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
    path: String,
    offset: usize,
    length: usize,
) -> PyObjectStoreResult<PyBytesWrapper> {
    let runtime = get_runtime(py)?;
    let range = offset..offset + length;
    py.allow_threads(|| {
        let out = runtime.block_on(store.as_ref().get_range(&path.into(), range))?;
        Ok::<_, PyObjectStoreError>(PyBytesWrapper::new(out))
    })
}

#[pyfunction]
pub(crate) fn get_range_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    offset: usize,
    length: usize,
) -> PyResult<Bound<PyAny>> {
    let range = offset..offset + length;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = store
            .as_ref()
            .get_range(&path.into(), range)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyBytesWrapper::new(out))
    })
}

#[pyfunction]
pub(crate) fn get_ranges(
    py: Python,
    store: PyObjectStore,
    path: String,
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
        let out = runtime.block_on(store.as_ref().get_ranges(&path.into(), &ranges))?;
        Ok::<_, PyObjectStoreError>(out.into_iter().map(PyBytesWrapper::new).collect())
    })
}

#[pyfunction]
pub(crate) fn get_ranges_async(
    py: Python,
    store: PyObjectStore,
    path: String,
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
            .get_ranges(&path.into(), &ranges)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(out.into_iter().map(PyBytesWrapper::new).collect::<Vec<_>>())
    })
}
