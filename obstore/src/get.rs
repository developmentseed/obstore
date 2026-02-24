use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, Fuse};
use futures::StreamExt;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, ObjectMeta, ObjectStore, ObjectStoreExt,
    coalesce_ranges, OBJECT_STORE_COALESCE_DEFAULT,
};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_bytes::PyBytes;
use pyo3_object_store::{PyObjectStore, PyObjectStoreError, PyObjectStoreResult, PyPath};
use tokio::sync::Mutex;

use crate::attributes::PyAttributes;
use crate::list::PyObjectMeta;

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

impl<'py> FromPyObject<'_, 'py> for PyGetOptions {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        // Update to use derive(FromPyObject) when default is implemented:
        // https://github.com/PyO3/pyo3/issues/4643
        let dict = obj.extract::<HashMap<String, Bound<PyAny>>>()?;
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
            extensions: Default::default(),
        }
    }
}

#[derive(FromPyObject)]
pub(crate) struct PyOffsetRange {
    #[pyo3(item)]
    offset: u64,
}

impl From<PyOffsetRange> for GetRange {
    fn from(value: PyOffsetRange) -> Self {
        GetRange::Offset(value.offset)
    }
}

#[derive(FromPyObject)]
pub(crate) struct PySuffixRange {
    #[pyo3(item)]
    suffix: u64,
}

impl From<PySuffixRange> for GetRange {
    fn from(value: PySuffixRange) -> Self {
        GetRange::Suffix(value.suffix)
    }
}

pub(crate) struct PyGetRange(GetRange);

// Allowed input:
// - [usize, usize] to refer to a bounded range from start to end (exclusive)
// - {"offset": usize} to request all bytes starting from a given byte offset
// - {"suffix": usize} to request the last `n` bytes
impl<'py> FromPyObject<'_, 'py> for PyGetRange {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(bounded) = obj.extract::<[u64; 2]>() {
            Ok(Self(GetRange::Bounded(bounded[0]..bounded[1])))
        } else if let Ok(offset_range) = obj.extract::<PyOffsetRange>() {
            Ok(Self(offset_range.into()))
        } else if let Ok(suffix_range) = obj.extract::<PySuffixRange>() {
            Ok(Self(suffix_range.into()))
        } else {
            Err(PyValueError::new_err("Unexpected input for byte range.\nExpected two-integer tuple or list, or dict with 'offset' or 'suffix' key." ))
        }
    }
}

/// Note that we clone the [ObjectMeta], byte range, and [Attributes] from the [`GetResult`] so
/// that we can access them from Python even after the result has been disposed.
#[pyclass(name = "GetResult", frozen)]
pub(crate) struct PyGetResult {
    /// The [`ObjectMeta`] for this object
    meta: ObjectMeta,
    /// The range of bytes returned by this request
    range: Range<u64>,
    /// Additional object attributes
    attributes: Attributes,
    /// The underlying result
    result: std::sync::Mutex<Option<GetResult>>,
}

impl PyGetResult {
    fn new(result: GetResult) -> Self {
        // Note: in the future we could avoid these clones if we stored GetResultPayload directly,
        // but it's useful to use the upstream helper methods defined on `GetResult`, like
        // `GetResult::bytes` and `GetResult::into_stream`.
        // Once https://github.com/apache/arrow-rs-object-store/pull/353 is merged it'll be easier.
        Self {
            meta: result.meta.clone(),
            range: result.range.clone(),
            attributes: result.attributes.clone(),
            result: std::sync::Mutex::new(Some(result)),
        }
    }
}

#[pymethods]
impl PyGetResult {
    fn bytes(&self, py: Python) -> PyObjectStoreResult<PyBytes> {
        let get_result = self
            .result
            .lock()
            .unwrap()
            .take()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        let runtime = get_runtime();
        py.detach(|| {
            let bytes = runtime.block_on(get_result.bytes())?;
            Ok::<_, PyObjectStoreError>(PyBytes::new(bytes))
        })
    }

    fn bytes_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let get_result = self
            .result
            .lock()
            .unwrap()
            .take()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = get_result
                .bytes()
                .await
                .map_err(PyObjectStoreError::ObjectStoreError)?;
            Ok(PyBytes::new(bytes))
        })
    }

    fn buffer(&self, py: Python) -> PyObjectStoreResult<PyBytes> {
        self.bytes(py)
    }

    fn buffer_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.bytes_async(py)
    }

    #[getter]
    fn attributes(&self) -> PyResult<PyAttributes> {
        Ok(PyAttributes::new(self.attributes.clone()))
    }

    #[getter]
    fn meta(&self) -> PyResult<PyObjectMeta> {
        Ok(PyObjectMeta::new(self.meta.clone()))
    }

    #[getter]
    fn range(&self) -> PyResult<(u64, u64)> {
        Ok((self.range.start, self.range.end))
    }

    #[pyo3(signature = (min_chunk_size = DEFAULT_BYTES_CHUNK_SIZE))]
    fn stream(&self, min_chunk_size: usize) -> PyResult<PyBytesStream> {
        let get_result = self
            .result
            .lock()
            .unwrap()
            .take()
            .ok_or(PyValueError::new_err("Result has already been disposed."))?;
        Ok(PyBytesStream::new(get_result.into_stream(), min_chunk_size))
    }

    fn __aiter__(&self) -> PyResult<PyBytesStream> {
        self.stream(DEFAULT_BYTES_CHUNK_SIZE)
    }

    fn __iter__(&self) -> PyResult<PyBytesStream> {
        self.stream(DEFAULT_BYTES_CHUNK_SIZE)
    }
}

// Note: we fuse the underlying stream so that we can get `None` multiple times.
// See the note on PyListStream for more background.
#[pyclass(name = "BytesStream", frozen)]
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
    let mut total_buffer_len = 0;
    loop {
        match stream.next().await {
            Some(Ok(bytes)) => {
                total_buffer_len += bytes.len();
                buffers.push(bytes);
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

    fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            next_stream(stream, self.min_chunk_size, false),
        )
    }

    fn __next__(&self, py: Python) -> PyResult<PyBytesWrapper> {
        let runtime = get_runtime();
        let stream = self.stream.clone();
        py.detach(|| runtime.block_on(next_stream(stream, self.min_chunk_size, true)))
    }
}

struct PyBytesWrapper(Vec<Bytes>);

impl PyBytesWrapper {
    fn new_multiple(buffers: Vec<Bytes>) -> Self {
        Self(buffers)
    }
}

// TODO: return buffer protocol object? This isn't possible on an array of Bytes, so if you want to
// support the buffer protocol in the future (e.g. for get_range) you may need to have a separate
// wrapper of Bytes
impl<'py> IntoPyObject<'py> for PyBytesWrapper {
    type Target = pyo3::types::PyBytes;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let total_len = self.0.iter().fold(0, |acc, buf| acc + buf.len());

        // Copy all internal Bytes objects into a single PyBytes
        // Since our inner callback is infallible, this will only panic on out of memory
        pyo3::types::PyBytes::new_with(py, total_len, |target| {
            let mut offset = 0;
            for buf in self.0.iter() {
                target[offset..offset + buf.len()].copy_from_slice(buf);
                offset += buf.len();
            }
            Ok(())
        })
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, *, options=None))]
pub(crate) fn get(
    py: Python,
    store: PyObjectStore,
    path: PyPath,
    options: Option<PyGetOptions>,
) -> PyObjectStoreResult<PyGetResult> {
    let runtime = get_runtime();
    py.detach(|| {
        let path = &path.as_ref();
        let fut = if let Some(options) = options {
            store.as_ref().get_opts(path, options.into())
        } else {
            Box::pin(store.as_ref().get(path))
        };
        let out = runtime.block_on(fut)?;
        Ok::<_, PyObjectStoreError>(PyGetResult::new(out))
    })
}

#[pyfunction]
#[pyo3(signature = (store, path, *, options=None))]
pub(crate) fn get_async(
    py: Python,
    store: PyObjectStore,
    path: PyPath,
    options: Option<PyGetOptions>,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let path = &path.as_ref();
        let fut = if let Some(options) = options {
            store.as_ref().get_opts(path, options.into())
        } else {
            Box::pin(store.as_ref().get(path))
        };
        let out = fut.await.map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyGetResult::new(out))
    })
}

#[pyfunction]
#[pyo3(signature = (store, path, *, start, end=None, length=None))]
pub(crate) fn get_range(
    py: Python,
    store: PyObjectStore,
    path: PyPath,
    start: u64,
    end: Option<u64>,
    length: Option<u64>,
) -> PyObjectStoreResult<pyo3_bytes::PyBytes> {
    let runtime = get_runtime();
    let range = params_to_range(start, end, length)?;
    py.detach(|| {
        let out = runtime.block_on(store.as_ref().get_range(path.as_ref(), range))?;
        Ok::<_, PyObjectStoreError>(pyo3_bytes::PyBytes::new(out))
    })
}

#[pyfunction]
#[pyo3(signature = (store, path, *, start, end=None, length=None))]
pub(crate) fn get_range_async(
    py: Python,
    store: PyObjectStore,
    path: PyPath,
    start: u64,
    end: Option<u64>,
    length: Option<u64>,
) -> PyResult<Bound<PyAny>> {
    let range = params_to_range(start, end, length)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = store
            .as_ref()
            .get_range(path.as_ref(), range)
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(pyo3_bytes::PyBytes::new(out))
    })
}

fn params_to_range(
    start: u64,
    end: Option<u64>,
    length: Option<u64>,
) -> PyObjectStoreResult<Range<u64>> {
    match (end, length) {
        (Some(_), Some(_)) => {
            Err(PyValueError::new_err("end and length cannot both be non-None.").into())
        }
        (None, None) => Err(PyValueError::new_err("Either end or length must be non-None.").into()),
        (Some(end), None) => validate_range(start..end),
        (None, Some(length)) => validate_range(start..start + length),
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, *, starts, ends=None, lengths=None, coalesce=OBJECT_STORE_COALESCE_DEFAULT))]
pub(crate) fn get_ranges(
    py: Python,
    store: PyObjectStore,
    path: PyPath,
    starts: Vec<u64>,
    ends: Option<Vec<u64>>,
    lengths: Option<Vec<u64>>,
    coalesce: u64,
) -> PyObjectStoreResult<Vec<pyo3_bytes::PyBytes>> {
    let runtime = get_runtime();
    let ranges = params_to_ranges(starts, ends, lengths)?;
    py.detach(|| {
        let out = runtime.block_on(coalesce_ranges(
            &ranges,
            |range| store.as_ref().get_range(path.as_ref(), range),
            coalesce,
        ))?;
        Ok::<_, PyObjectStoreError>(out.into_iter().map(|buf| buf.into()).collect())
    })
}

#[pyfunction]
#[pyo3(signature = (store, path, *, starts, ends=None, lengths=None, coalesce=OBJECT_STORE_COALESCE_DEFAULT))]
pub(crate) fn get_ranges_async(
    py: Python,
    store: PyObjectStore,
    path: PyPath,
    starts: Vec<u64>,
    ends: Option<Vec<u64>>,
    lengths: Option<Vec<u64>>,
    coalesce: u64,
) -> PyResult<Bound<PyAny>> {
    let ranges = params_to_ranges(starts, ends, lengths)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = coalesce_ranges(
            &ranges,
            |range| store.as_ref().get_range(path.as_ref(), range),
            coalesce,
        )
        .await
        .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(out
            .into_iter()
            .map(pyo3_bytes::PyBytes::new)
            .collect::<Vec<_>>())
    })
}

fn params_to_ranges(
    starts: Vec<u64>,
    ends: Option<Vec<u64>>,
    lengths: Option<Vec<u64>>,
) -> PyObjectStoreResult<Vec<Range<u64>>> {
    match (ends, lengths) {
        (Some(_), Some(_)) => {
            Err(PyValueError::new_err("ends and lengths cannot both be non-None.").into())
        }
        (None, None) => {
            Err(PyValueError::new_err("Either ends or lengths must be non-None.").into())
        }
        (Some(ends), None) => starts
            .into_iter()
            .zip(ends)
            .map(|(start, end)| start..end)
            .map(validate_range)
            .collect(),
        (None, Some(lengths)) => starts
            .into_iter()
            .zip(lengths)
            .map(|(start, length)| start..start + length)
            .map(validate_range)
            .collect(),
    }
}

fn validate_range(r: Range<u64>) -> PyObjectStoreResult<Range<u64>> {
    if r.end <= r.start {
        return Err(PyValueError::new_err(format!(
            "Invalid range requested, start: {} end: {}",
            r.start, r.end
        ))
        .into());
    }

    if (r.end - r.start) > usize::MAX as u64 {
        return Err(PyValueError::new_err(format!(
            "Range {} is larger than system memory limit {}",
            r.start,
            usize::MAX
        ))
        .into());
    }

    Ok(r)
}
