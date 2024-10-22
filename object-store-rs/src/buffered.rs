use std::io::SeekFrom;
use std::sync::Arc;

use object_store::buffered::BufReader;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, Lines};
use tokio::sync::Mutex;

use crate::get::PyBytesWrapper;
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
    Ok(PyReadableFile(Arc::new(Mutex::new(BufReader::new(
        store, &meta,
    )))))
}

#[pyfunction]
pub(crate) fn open_async(py: Python, store: PyObjectStore, path: String) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let meta = store
            .head(&path.into())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(PyReadableFile(Arc::new(Mutex::new(BufReader::new(
            store, &meta,
        )))))
    })
}

#[pyclass(name = "ReadableFile")]
pub(crate) struct PyReadableFile(Arc<Mutex<BufReader>>);

#[pymethods]
impl PyReadableFile {
    // fn __aiter__(&mut self) -> PyObjectStoreResult<PyLinesReader> {
    //     let reader = self.0.take().unwrap();
    //     Ok(PyLinesReader(Arc::new(Mutex::new(reader.lines()))))
    // }

    // Maybe this should dispose of the internal reader? In that case we want to store an
    // `Arc<Mutex<BufReader>>`.
    fn close(&self) {}

    fn read<'py>(&'py mut self, py: Python<'py>, size: i64) -> PyResult<Bound<PyAny>> {
        let reader = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut reader = reader.lock().await;
            if size <= 0 {
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await?;
                Ok(PyBytesWrapper::new(buf.into()))
            } else {
                let mut buf = vec![0; size as _];
                reader.read_exact(&mut buf).await?;
                Ok(PyBytesWrapper::new(buf.into()))
            }
        })
    }

    fn readall<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        self.read(py, -1)
    }

    fn readline<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let reader = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut reader = reader.lock().await;
            let mut buf = String::new();
            reader.read_line(&mut buf).await?;
            Ok(buf)
        })
        // TODO: should raise at EOF when read_line returns 0?
    }

    fn readlines<'py>(&'py mut self, py: Python<'py>, hint: i64) -> PyResult<Bound<PyAny>> {
        let reader = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut reader = reader.lock().await;
            if hint <= 0 {
                let mut lines = Vec::new();
                loop {
                    let mut buf = String::new();
                    let n = reader.read_line(&mut buf).await?;
                    lines.push(buf);
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
                    lines.push(buf);
                    // Ok(0) signifies EOF
                    if n == 0 {
                        return Ok(lines);
                    }
                }
            }
        })
    }

    fn seek<'py>(
        &'py mut self,
        py: Python<'py>,
        offset: i64,
        whence: usize,
    ) -> PyResult<Bound<PyAny>> {
        let reader = self.0.clone();
        let pos = match whence {
            0 => SeekFrom::Start(offset as _),
            1 => SeekFrom::Current(offset as _),
            2 => SeekFrom::End(offset as _),
            _ => unreachable!(),
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut reader = reader.lock().await;
            reader.seek(pos).await.unwrap();
            Ok(())
        })
    }

    #[staticmethod]
    fn seekable() -> bool {
        true
    }

    fn tell<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let reader = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut reader = reader.lock().await;
            let pos = reader.stream_position().await?;
            Ok(pos)
        })
    }
}

#[pyclass]
pub(crate) struct PyLinesReader(Arc<Mutex<Lines<BufReader>>>);

#[pymethods]
impl PyLinesReader {
    fn __anext__<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let lines = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut lines = lines.lock().await;
            if let Some(line) = lines.next_line().await.unwrap() {
                Ok(line)
            } else {
                Err(PyStopAsyncIteration::new_err("stream exhausted"))
            }
        })
    }
}

// #[cfg(test)]
// mod test {

//     use tokio::fs::File;
//     use tokio::io::AsyncReadExt;

//     #[tokio::test]
//     async fn tmp() {
//         let path = "/Users/kyle/github/developmentseed/object-store-rs/foo.txt";
//         let mut f = File::open(path).await.unwrap();
//         // let mut buffer = BytesMut::with_capacity(10);
//         let mut buffer = vec![0; 10];

//         dbg!(buffer.is_empty());
//         dbg!(buffer.capacity());
//         dbg!(buffer.len());

//         // note that the return value is not needed to access the data
//         // that was read as `buffer`'s internal cursor is updated.
//         //
//         // this might read more than 10 bytes if the capacity of `buffer`
//         // is larger than 10.
//         let amt = f.read(&mut buffer).await.unwrap();
//         dbg!(buffer.len());
//         dbg!(amt);
//         // buffer.res

//         println!("The bytes: {:?}", &buffer[..].to_ascii_lowercase());

//         let amt = f.read(&mut buffer).await.unwrap();
//         dbg!(buffer.len());
//         dbg!(amt);
//         println!("The bytes: {:?}", &buffer[..].to_ascii_lowercase());
//     }
// }
