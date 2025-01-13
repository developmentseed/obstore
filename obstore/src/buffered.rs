use std::io::SeekFrom;
use std::sync::Arc;

use arrow::buffer::Buffer;
use object_store::buffered::BufReader;
use pyo3::exceptions::{PyIOError, PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3_arrow::buffer::PyArrowBuffer;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_object_store::{PyObjectStore, PyObjectStoreError, PyObjectStoreResult};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, Lines};
use tokio::sync::Mutex;

use crate::runtime::get_runtime;

#[pyfunction]
pub(crate) fn open(
    py: Python,
    store: PyObjectStore,
    path: String,
) -> PyObjectStoreResult<PyReadableFile> {
    let store = store.into_inner();
    let runtime = get_runtime(py)?;
    let meta = py.allow_threads(|| runtime.block_on(store.head(&path.into())))?;
    let reader = Arc::new(Mutex::new(BufReader::new(store, &meta)));
    Ok(PyReadableFile::new(reader, false))
}

#[pyfunction]
pub(crate) fn open_async(py: Python, store: PyObjectStore, path: String) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner();
    future_into_py(py, async move {
        let meta = store
            .head(&path.into())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        let reader = Arc::new(Mutex::new(BufReader::new(store, &meta)));
        Ok(PyReadableFile::new(reader, true))
    })
}

#[pyclass(name = "ReadableFile", frozen)]
pub(crate) struct PyReadableFile {
    reader: Arc<Mutex<BufReader>>,
    r#async: bool,
}

impl PyReadableFile {
    fn new(reader: Arc<Mutex<BufReader>>, r#async: bool) -> Self {
        Self { reader, r#async }
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

    #[pyo3(signature = (size = None, /))]
    fn read<'py>(&'py self, py: Python<'py>, size: Option<usize>) -> PyResult<PyObject> {
        let reader = self.reader.clone();
        if self.r#async {
            let out = future_into_py(py, read(reader, size))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(read(reader, size)))?;
            Ok(out.into_pyobject(py)?.into_any().unbind())
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
            Ok(out.into_pyobject(py)?.into_any().unbind())
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
            Ok(out.into_pyobject(py)?.into_any().unbind())
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
            Ok(out.into_pyobject(py)?.into_any().unbind())
        }
    }

    fn seekable(&self) -> bool {
        true
    }

    fn tell<'py>(&'py self, py: Python<'py>) -> PyResult<PyObject> {
        let reader = self.reader.clone();
        if self.r#async {
            let out = future_into_py(py, tell(reader))?;
            Ok(out.unbind())
        } else {
            let runtime = get_runtime(py)?;
            let out = py.allow_threads(|| runtime.block_on(tell(reader)))?;
            Ok(out.into_pyobject(py)?.into_any().unbind())
        }
    }
}

async fn read(reader: Arc<Mutex<BufReader>>, size: Option<usize>) -> PyResult<PyArrowBuffer> {
    let mut reader = reader.lock().await;
    if let Some(size) = size {
        let mut buf = vec![0; size as _];
        reader.read_exact(&mut buf).await?;
        Ok(PyArrowBuffer::new(Buffer::from_vec(buf)))
    } else {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok(PyArrowBuffer::new(Buffer::from_vec(buf)))
    }
}

async fn readline(reader: Arc<Mutex<BufReader>>) -> PyResult<PyArrowBuffer> {
    let mut reader = reader.lock().await;
    let mut buf = String::new();
    reader.read_line(&mut buf).await?;
    Ok(PyArrowBuffer::new(Buffer::from_vec(buf.into_bytes())))
}

async fn readlines(reader: Arc<Mutex<BufReader>>, hint: i64) -> PyResult<Vec<PyArrowBuffer>> {
    let mut reader = reader.lock().await;
    if hint <= 0 {
        let mut lines = Vec::new();
        loop {
            let mut buf = String::new();
            let n = reader.read_line(&mut buf).await?;
            lines.push(PyArrowBuffer::new(Buffer::from_vec(buf.into_bytes())));
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
            lines.push(PyArrowBuffer::new(Buffer::from_vec(buf.into_bytes())));
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
