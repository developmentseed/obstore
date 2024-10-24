use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use indexmap::IndexMap;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload, PutResult, WriteMultipart};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3_file::PyFileLikeObject;
use pyo3_object_store::error::PyObjectStoreResult;
use pyo3_object_store::PyObjectStore;

use crate::runtime::get_runtime;

/// Input types supported by multipart upload
#[derive(Debug)]
pub(crate) enum MultipartPutInput {
    File(BufReader<File>),
    FileLike(PyFileLikeObject),
    Buffer(Cursor<PyBackedBytes>),
}

impl MultipartPutInput {
    /// Number of bytes in the file-like object
    fn nbytes(&mut self) -> PyObjectStoreResult<usize> {
        let origin_pos = self.stream_position()?;
        let size = self.seek(SeekFrom::End(0))?;
        self.seek(SeekFrom::Start(origin_pos))?;
        Ok(size.try_into().unwrap())
    }

    /// Whether to use multipart uploads.
    fn use_multipart(&mut self, chunk_size: usize) -> PyObjectStoreResult<bool> {
        Ok(self.nbytes()? > chunk_size)
    }
}

impl<'py> FromPyObject<'py> for MultipartPutInput {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(BufReader::new(File::open(path)?)))
        } else if let Ok(buffer) = ob.extract::<PyBackedBytes>() {
            Ok(Self::Buffer(Cursor::new(buffer)))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::with_requirements(
                ob.into_py(py),
                true,
                false,
                true,
                false,
            )?))
        }
    }
}

impl Read for MultipartPutInput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            Self::FileLike(f) => f.read(buf),
            Self::Buffer(f) => f.read(buf),
        }
    }
}

impl Seek for MultipartPutInput {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(f) => f.seek(pos),
            Self::FileLike(f) => f.seek(pos),
            Self::Buffer(f) => f.seek(pos),
        }
    }
}

pub(crate) struct PyPutResult(PutResult);

impl IntoPy<PyObject> for PyPutResult {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = IndexMap::with_capacity(2);
        dict.insert("e_tag", self.0.e_tag.into_py(py));
        dict.insert("version", self.0.version.into_py(py));
        dict.into_py(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, file, *, use_multipart = None, chunk_size = 5242880, max_concurrency = 12))]
pub(crate) fn put(
    py: Python,
    store: PyObjectStore,
    path: String,
    mut file: MultipartPutInput,
    use_multipart: Option<bool>,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyObjectStoreResult<PyPutResult> {
    let use_multipart = if let Some(use_multipart) = use_multipart {
        use_multipart
    } else {
        file.use_multipart(chunk_size)?
    };
    let runtime = get_runtime(py)?;
    if use_multipart {
        runtime.block_on(put_multipart_inner(
            store.into_inner(),
            &path.into(),
            file,
            chunk_size,
            max_concurrency,
        ))
    } else {
        runtime.block_on(put_inner(store.into_inner(), &path.into(), file))
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, file, *, use_multipart = None, chunk_size = 5242880, max_concurrency = 12))]
pub(crate) fn put_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    mut file: MultipartPutInput,
    use_multipart: Option<bool>,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyResult<Bound<PyAny>> {
    let use_multipart = if let Some(use_multipart) = use_multipart {
        use_multipart
    } else {
        file.use_multipart(chunk_size)?
    };
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let result = if use_multipart {
            put_multipart_inner(
                store.into_inner(),
                &path.into(),
                file,
                chunk_size,
                max_concurrency,
            )
            .await?
        } else {
            put_inner(store.into_inner(), &path.into(), file).await?
        };
        Ok(result)
    })
}

async fn put_inner(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    mut reader: MultipartPutInput,
) -> PyObjectStoreResult<PyPutResult> {
    let nbytes = reader.nbytes()?;
    let mut buffer = Vec::with_capacity(nbytes);
    reader.read_to_end(&mut buffer)?;
    let payload = PutPayload::from_bytes(buffer.into());
    Ok(PyPutResult(store.put(path, payload).await?))
}

async fn put_multipart_inner<R: Read>(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    mut reader: R,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyObjectStoreResult<PyPutResult> {
    let upload = store.put_multipart(path).await?;
    let mut write = WriteMultipart::new(upload);
    let mut scratch_buffer = vec![0; chunk_size];
    loop {
        let read_size = reader.read(&mut scratch_buffer)?;
        if read_size == 0 {
            break;
        } else {
            write.wait_for_capacity(max_concurrency).await?;
            write.write(&scratch_buffer[0..read_size]);
        }
    }
    Ok(PyPutResult(write.finish().await?))
}
