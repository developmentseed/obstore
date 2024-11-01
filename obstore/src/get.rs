use std::collections::HashMap;
use std::sync::Arc;

use arrow::buffer::Buffer;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, Fuse};
use futures::StreamExt;
use object_store::{GetOptions, GetRange, GetResult, ObjectStore};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_arrow::buffer::PyArrowBuffer;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;
use tokio::sync::Mutex;

use crate::attributes::PyAttributes;
use crate::list::PyObjectMeta;
use crate::runtime::get_runtime;

/// 10MB default chunk size
const DEFAULT_BYTES_CHUNK_SIZE: usize = 10 * 1024 * 1024;

pub(crate) struct PyGetOptions {
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<DateTime<Utc>>,
    if_unmodified_since: Option<DateTime<Utc>>,
    range: Option<PyGetRange>,
    version: Option<String>,
    head: bool,
}

impl<'py> FromPyObject<'py> for PyGetOptions {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        // Update to use derive(FromPyObject) when default is implemented:
        // https://github.com/PyO3/pyo3/issues/4643
        let dict = ob.extract::<HashMap<String, Bound<PyAny>>>()?;
        Ok(Self {
            if_match: dict.get("if_match").map(|x| x.extract()).transpose()?,
            if_none_match: dict.get("if_none_match").map(|x| x.extract()).transpose()?,
            if_modified_since: dict
                .get("if_modified_since")
                .map(|x| x.extract())
                .transpose()?,
            if_unmodified_since: dict
                .get("if_unmodified_since")
                .map(|x| x.extract())
                .transpose()?,
            range: dict.get("range").map(|x| x.extract()).transpose()?,
            version: dict.get("version").map(|x| x.extract()).transpose()?,
            head: dict
                .get("head")
                .map(|x| x.extract())
                .transpose()?
                .unwrap_or(false),
        })
    }
}

impl From<PyGetOptions> for GetOptions {
    fn from(value: PyGetOptions) -> Self {
        Self {
            if_match: value.if_match,
            if_none_match: value.if_none_match,
            if_modified_since: value.if_modified_since,
            if_unmodified_since: value.if_unmodified_since,
            range: value.range.map(|inner| inner.0),
            version: value.version,
            head: value.head,
        }
    }
}

#[derive(FromPyObject)]
pub(crate) struct PyOffsetRange {
    #[pyo3(item)]
    offset: usize,
}

impl From<PyOffsetRange> for GetRange {
    fn from(value: PyOffsetRange) -> Self {
        GetRange::Offset(value.offset)
    }
}

#[derive(FromPyObject)]
pub(crate) struct PySuffixRange {
    #[pyo3(item)]
    suffix: usize,
}

impl From<PySuffixRange> for GetRange {
    fn from(value: PySuffixRange) -> Self {
        GetRange::Suffix(value.suffix)
    }
}

pub(crate) struct PyGetRange(GetRange);

// TODO: think of a better API here so that the distinction between each of these is easy to
// understand.
// Allowed input:
// - [usize, usize] to refer to a bounded range from start to end (exclusive)
// - {"offset": usize} to request all bytes starting from a given byte offset
// - {"suffix": usize} to request the last `n` bytes
impl<'py> FromPyObject<'py> for PyGetRange {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(bounded) = ob.extract::<[usize; 2]>() {
            Ok(Self(GetRange::Bounded(bounded[0]..bounded[1])))
        } else if let Ok(offset_range) = ob.extract::<PyOffsetRange>() {
            Ok(Self(offset_range.into()))
        } else if let Ok(suffix_range) = ob.extract::<PySuffixRange>() {
            Ok(Self(suffix_range.into()))
        } else {
            // dbg!(ob);
            // let x = ob.extract::<PyOffsetRange>()?;
            // dbg!(x.offset);
            Err(PyValueError::new_err("Unexpected input for byte range.\nExpected two-integer tuple or list, or dict with 'offset' or 'suffix' key." ))
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
    fn attributes(&self) -> PyResult<PyAttributes> {
        let inner = self
            .0
            .as_ref()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        Ok(PyAttributes::new(inner.attributes.clone()))
    }

    #[getter]
    fn meta(&self) -> PyResult<PyObjectMeta> {
        let inner = self
            .0
            .as_ref()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        Ok(PyObjectMeta::new(inner.meta.clone()))
    }

    #[getter]
    fn range(&self) -> PyResult<(usize, usize)> {
        let inner = self
            .0
            .as_ref()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        Ok((inner.range.start, inner.range.end))
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
    start: usize,
    end: usize,
) -> PyObjectStoreResult<PyArrowBuffer> {
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        let out = runtime.block_on(store.as_ref().get_range(&path.into(), start..end))?;
        Ok::<_, PyObjectStoreError>(PyArrowBuffer::new(Buffer::from_bytes(out.into())))
    })
}

#[pyfunction]
pub(crate) fn get_range_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    start: usize,
    end: usize,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = store
            .as_ref()
            .get_range(&path.into(), start..end)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyArrowBuffer::new(Buffer::from_bytes(out.into())))
    })
}

#[pyfunction]
pub(crate) fn get_ranges(
    py: Python,
    store: PyObjectStore,
    path: String,
    starts: Vec<usize>,
    ends: Vec<usize>,
) -> PyObjectStoreResult<Vec<PyArrowBuffer>> {
    let runtime = get_runtime(py)?;
    let ranges = starts
        .into_iter()
        .zip(ends)
        .map(|(start, end)| start..end)
        .collect::<Vec<_>>();
    py.allow_threads(|| {
        let out = runtime.block_on(store.as_ref().get_ranges(&path.into(), &ranges))?;
        Ok::<_, PyObjectStoreError>(
            out.into_iter()
                .map(|buf| PyArrowBuffer::new(Buffer::from_bytes(buf.into())))
                .collect(),
        )
    })
}

#[pyfunction]
pub(crate) fn get_ranges_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    starts: Vec<usize>,
    ends: Vec<usize>,
) -> PyResult<Bound<PyAny>> {
    let ranges = starts
        .into_iter()
        .zip(ends)
        .map(|(start, end)| start..end)
        .collect::<Vec<_>>();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = store
            .as_ref()
            .get_ranges(&path.into(), &ranges)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(out
            .into_iter()
            .map(|buf| PyArrowBuffer::new(Buffer::from_bytes(buf.into())))
            .collect::<Vec<_>>())
    })
}
