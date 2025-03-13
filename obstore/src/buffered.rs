use std::io::SeekFrom;
use std::sync::Arc;

use bytes::Bytes;
use object_store::buffered::{BufReader, BufWriter};
use object_store::{ObjectMeta, ObjectStore};
use pyo3::exceptions::{PyIOError, PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::{intern, IntoPyObjectExt};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_bytes::PyBytes;
use pyo3_object_store::{PyObjectStore, PyObjectStoreError, PyObjectStoreResult};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, Lines};
use tokio::sync::Mutex;

use crate::attributes::PyAttributes;
use crate::list::PyObjectMeta;
use crate::runtime::get_runtime;
use crate::tags::PyTagSet;

#[pyfunction]
#[pyo3(signature = (store, path, *, buffer_size=1024 * 1024))]
pub(crate) fn open_reader(
    py: Python,
    store: PyObjectStore,
    path: String,
    buffer_size: usize,
) -> PyObjectStoreResult<PyReadableFile> {
    let store = store.into_inner();
    let runtime = get_runtime(py)?;
    let (reader, meta) =
        py.allow_threads(|| runtime.block_on(create_reader(store, path, buffer_size)))?;
    Ok(PyReadableFile::new(reader, meta, false))
}

#[pyfunction]
#[pyo3(signature = (store, path, *, buffer_size=1024 * 1024))]
pub(crate) fn open_reader_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    buffer_size: usize,
) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner();
    future_into_py(py, async move {
        let (reader, meta) = create_reader(store, path, buffer_size).await?;
        Ok(PyReadableFile::new(reader, meta, true))
    })
}

async fn create_reader(
    store: Arc<dyn ObjectStore>,
    path: String,
    capacity: usize,
) -> PyObjectStoreResult<(BufReader, ObjectMeta)> {
    let meta = store
        .head(&path.into())
        .await
        .map_err(PyObjectStoreError::ObjectStoreError)?;
    Ok((BufReader::with_capacity(store, &meta, capacity), meta))
}

#[pyclass(name = "ReadableFile", frozen)]
pub(crate) struct PyReadableFile {
    reader: Arc<Mutex<BufReader>>,
    meta: ObjectMeta,
    r#async: bool,
}

impl PyReadableFile {
    fn new(reader: BufReader, meta: ObjectMeta, r#async: bool) -> Self {
        Self {
            reader: Arc::new(Mutex::new(reader)),
            meta,
            r#async,
        }
    }
}

#[pymethods]
impl PyReadableFile {
    // Note: to enable this, we'd have to make the PyReadableFile contain an `Option<>` that here
    // we could move out.
    // async fn __aiter__(&mut self) -> PyObjectStoreResult<PyLinesReader> {
    //     let reader = self.reader.clone();
    //     let reader = reader.lock().await;
    //     let lines = reader.lines();
    //     Ok(PyLinesReader(Arc::new(Mutex::new(lines))))
    // }

    // Maybe this should dispose of the internal reader? In that case we want to store an
    // `Option<Arc<Mutex<BufReader>>>`.
    fn close(&self) {}

    #[getter]
    fn meta(&self) -> PyObjectMeta {
        self.meta.clone().into()
    }

    #[pyo3(signature = (size = None, /))]
    fn read<'py>(&'py self, py: Python<'py>, size: Option<usize>) -> PyResult<PyObject> {
        let reader = self.reader.clone();
        if self.r#async {
            let out = future_into_py(py, read(reader, size))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(read(reader, size)))?;
            out.into_py_any(py)
        }
    }

    fn readall<'py>(&'py self, py: Python<'py>) -> PyResult<PyObject> {
        self.read(py, None)
    }

    fn readline<'py>(&'py self, py: Python<'py>) -> PyResult<PyObject> {
        let reader = self.reader.clone();
        if self.r#async {
            let out = future_into_py(py, readline(reader))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(readline(reader)))?;
            out.into_py_any(py)
        }
        // TODO: should raise at EOF when read_line returns 0?
    }

    #[pyo3(signature = (hint = -1))]
    fn readlines<'py>(&'py self, py: Python<'py>, hint: i64) -> PyResult<PyObject> {
        let reader = self.reader.clone();
        if self.r#async {
            let out = future_into_py(py, readlines(reader, hint))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(readlines(reader, hint)))?;
            out.into_py_any(py)
        }
    }

    #[pyo3(
        signature = (offset, whence=0, /),
        text_signature = "(offset, whence=os.SEEK_SET, /)")
    ]
    fn seek<'py>(&'py self, py: Python<'py>, offset: i64, whence: usize) -> PyResult<PyObject> {
        let reader = self.reader.clone();
        let pos = match whence {
            0 => SeekFrom::Start(offset as _),
            1 => SeekFrom::Current(offset as _),
            2 => SeekFrom::End(offset as _),
            other => {
                return Err(PyIOError::new_err(format!(
                    "Invalid value for whence in seek: {}",
                    other
                )))
            }
        };

        if self.r#async {
            let out = future_into_py(py, seek(reader, pos))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(seek(reader, pos)))?;
            out.into_py_any(py)
        }
    }

    fn seekable(&self) -> bool {
        true
    }

    #[getter]
    fn size(&self) -> u64 {
        self.meta.size
    }

    fn tell<'py>(&'py self, py: Python<'py>) -> PyResult<PyObject> {
        let reader = self.reader.clone();
        if self.r#async {
            let out = future_into_py(py, tell(reader))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(tell(reader)))?;
            out.into_py_any(py)
        }
    }
}

async fn read(reader: Arc<Mutex<BufReader>>, size: Option<usize>) -> PyResult<PyBytes> {
    let mut reader = reader.lock().await;
    if let Some(size) = size {
        let mut buf = vec![0; size as _];
        reader.read_exact(&mut buf).await?;
        Ok(Bytes::from(buf).into())
    } else {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok(Bytes::from(buf).into())
    }
}

async fn readline(reader: Arc<Mutex<BufReader>>) -> PyResult<PyBytes> {
    let mut reader = reader.lock().await;
    let mut buf = String::new();
    reader.read_line(&mut buf).await?;
    Ok(Bytes::from(buf.into_bytes()).into())
}

async fn readlines(reader: Arc<Mutex<BufReader>>, hint: i64) -> PyResult<Vec<PyBytes>> {
    let mut reader = reader.lock().await;
    if hint <= 0 {
        let mut lines = Vec::new();
        loop {
            let mut buf = String::new();
            let n = reader.read_line(&mut buf).await?;
            lines.push(Bytes::from(buf.into_bytes()).into());
            // Ok(0) signifies EOF
            if n == 0 {
                return Ok(lines);
            }
        }
    } else {
        let mut lines = Vec::new();
        let mut byte_count = 0;
        loop {
            if byte_count >= hint as usize {
                return Ok(lines);
            }

            let mut buf = String::new();
            let n = reader.read_line(&mut buf).await?;
            byte_count += n;
            lines.push(Bytes::from(buf.into_bytes()).into());
            // Ok(0) signifies EOF
            if n == 0 {
                return Ok(lines);
            }
        }
    }
}

async fn seek(reader: Arc<Mutex<BufReader>>, pos: SeekFrom) -> PyResult<u64> {
    let mut reader = reader.lock().await;
    let pos = reader.seek(pos).await?;
    Ok(pos)
}

async fn tell(reader: Arc<Mutex<BufReader>>) -> PyResult<u64> {
    let mut reader = reader.lock().await;
    let pos = reader.stream_position().await?;
    Ok(pos)
}

#[pyclass(frozen)]
pub(crate) struct PyLinesReader(Arc<Mutex<Lines<BufReader>>>);

#[pymethods]
impl PyLinesReader {
    fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lines = self.0.clone();
        future_into_py(py, next_line(lines, true))
    }

    fn __next__<'py>(&'py self, py: Python<'py>) -> PyResult<String> {
        let runtime = get_runtime(py)?;
        let lines = self.0.clone();
        py.allow_threads(|| runtime.block_on(next_line(lines, false)))
    }
}

async fn next_line(reader: Arc<Mutex<Lines<BufReader>>>, r#async: bool) -> PyResult<String> {
    let mut reader = reader.lock().await;
    if let Some(line) = reader.next_line().await.unwrap() {
        Ok(line)
    } else if r#async {
        Err(PyStopAsyncIteration::new_err("stream exhausted"))
    } else {
        Err(PyStopIteration::new_err("stream exhausted"))
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, *, attributes=None, buffer_size=10 * 1024 * 1024, tags=None, max_concurrency=12))]
pub(crate) fn open_writer(
    store: PyObjectStore,
    path: String,
    attributes: Option<PyAttributes>,
    buffer_size: usize,
    tags: Option<PyTagSet>,
    max_concurrency: usize,
) -> PyObjectStoreResult<PyWritableFile> {
    Ok(PyWritableFile::new(
        create_writer(store, path, attributes, buffer_size, tags, max_concurrency),
        false,
    ))
}

#[pyfunction]
#[pyo3(signature = (store, path, *, attributes=None, buffer_size=10 * 1024 * 1024, tags=None, max_concurrency=12))]
pub(crate) fn open_writer_async(
    store: PyObjectStore,
    path: String,
    attributes: Option<PyAttributes>,
    buffer_size: usize,
    tags: Option<PyTagSet>,
    max_concurrency: usize,
) -> PyResult<PyWritableFile> {
    Ok(PyWritableFile::new(
        create_writer(store, path, attributes, buffer_size, tags, max_concurrency),
        true,
    ))
}

fn create_writer(
    store: PyObjectStore,
    path: String,
    attributes: Option<PyAttributes>,
    capacity: usize,
    tags: Option<PyTagSet>,
    max_concurrency: usize,
) -> Arc<Mutex<Option<BufWriter>>> {
    let store = store.into_inner();
    let mut writer = BufWriter::with_capacity(store, path.into(), capacity)
        .with_max_concurrency(max_concurrency);
    if let Some(attributes) = attributes {
        writer = writer.with_attributes(attributes.into_inner());
    }
    if let Some(tags) = tags {
        writer = writer.with_tags(tags.into_inner());
    }
    Arc::new(Mutex::new(Some(writer)))
}

#[pyclass(name = "WritableFile", frozen)]
pub(crate) struct PyWritableFile {
    writer: Arc<Mutex<Option<BufWriter>>>,
    r#async: bool,
}

impl PyWritableFile {
    fn new(writer: Arc<Mutex<Option<BufWriter>>>, r#async: bool) -> Self {
        Self { writer, r#async }
    }
}

#[pymethods]
impl PyWritableFile {
    fn __repr__<'py>(&'py self, py: Python<'py>) -> &'py Bound<'py, PyString> {
        if self.r#async {
            intern!(py, "AsyncWritableFile")
        } else {
            intern!(py, "WritableFile")
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __aenter__(slf: Py<Self>, py: Python) -> PyResult<Bound<PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (exc_type, exc_value, traceback))]
    fn __exit__(
        &self,
        py: Python,
        exc_type: Option<PyObject>,
        exc_value: Option<PyObject>,
        traceback: Option<PyObject>,
    ) -> PyResult<()> {
        let writer = self.writer.clone();
        let runtime = get_runtime(py)?;
        if exc_type.is_some() {
            py.allow_threads(|| runtime.block_on(abort_writer(writer)))?;
        } else {
            py.allow_threads(|| runtime.block_on(close_writer(writer)))?;
        }
        Ok(())
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (exc_type, exc_value, traceback))]
    fn __aexit__<'py>(
        &'py self,
        py: Python<'py>,
        exc_type: Option<PyObject>,
        exc_value: Option<PyObject>,
        traceback: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let writer = self.writer.clone();
        let runtime = get_runtime(py)?;
        if exc_type.is_some() {
            future_into_py(py, abort_writer(writer))
        } else {
            future_into_py(py, close_writer(writer))
        }
    }

    fn close<'py>(&'py self, py: Python<'py>) -> PyResult<PyObject> {
        let writer = self.writer.clone();
        if self.r#async {
            let out = future_into_py(py, close_writer(writer))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            py.allow_threads(|| runtime.block_on(close_writer(writer)))?;
            Ok(py.None())
        }
    }

    /// It's a little unfortunate that this is a method instead of an attribute.
    ///
    /// We need an `Option` somewhere in order to be able to drop the internal `BufWriter` to check
    /// that it has already been closed. (The `object_store` API will error if the file is closed
    /// twice, but doesn't give a way to check if the file has already been closed).
    ///
    /// This being an async method is an artifact of storing the underlying BufWriter inside of an
    /// ```rs
    /// Arc<Mutex<Option<BufWriter>>>
    /// ```
    /// where the `Mutex` is a `tokio::sync::Mutex`.
    ///
    /// Thus we need to use async to open the mutex. We could add a second layer of mutex, where
    /// the top-level mutex is a `std::sync::Mutex`, but I assume that two levels of mutexes would
    /// be detrimental for performance.
    fn closed<'py>(&'py self, py: Python<'py>) -> PyResult<PyObject> {
        let writer = self.writer.clone();
        if self.r#async {
            let out = future_into_py(py, is_closed(writer))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(is_closed(writer)))?;
            out.into_py_any(py)
        }
    }

    fn flush<'py>(&'py self, py: Python<'py>) -> PyResult<PyObject> {
        let writer = self.writer.clone();
        if self.r#async {
            let out = future_into_py(py, flush(writer))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            py.allow_threads(|| runtime.block_on(flush(writer)))?;
            Ok(py.None())
        }
    }

    fn write<'py>(&'py self, py: Python<'py>, buffer: PyBytes) -> PyResult<PyObject> {
        let writer = self.writer.clone();
        if self.r#async {
            let out = future_into_py(py, write(writer, buffer))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(write(writer, buffer)))?;
            out.into_py_any(py)
        }
    }
}

async fn is_closed(writer: Arc<Mutex<Option<BufWriter>>>) -> PyResult<bool> {
    let writer = writer.lock().await;
    Ok(writer.is_none())
}

async fn abort_writer(writer: Arc<Mutex<Option<BufWriter>>>) -> PyResult<()> {
    let mut writer = writer.lock().await;
    let mut writer = writer
        .take()
        .ok_or(PyIOError::new_err("Writer already closed."))?;
    writer.abort().await.map_err(PyObjectStoreError::from)?;
    Ok(())
}

async fn close_writer(writer: Arc<Mutex<Option<BufWriter>>>) -> PyResult<()> {
    let mut writer = writer.lock().await;
    let mut writer = writer
        .take()
        .ok_or(PyIOError::new_err("Writer already closed."))?;
    writer.shutdown().await?;
    Ok(())
}

async fn flush(writer: Arc<Mutex<Option<BufWriter>>>) -> PyResult<()> {
    let mut writer = writer.lock().await;
    let writer = writer
        .as_mut()
        .ok_or(PyIOError::new_err("Writer already closed."))?;
    writer.flush().await?;
    Ok(())
}

async fn write(writer: Arc<Mutex<Option<BufWriter>>>, buffer: PyBytes) -> PyResult<usize> {
    let mut writer = writer.lock().await;
    let writer = writer
        .as_mut()
        .ok_or(PyIOError::new_err("Writer already closed."))?;
    let buffer = buffer.into_inner();
    let buffer_length = buffer.len();
    writer.put(buffer).await.map_err(PyObjectStoreError::from)?;
    Ok(buffer_length)
}
