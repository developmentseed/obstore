use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use indexmap::IndexMap;
use object_store::path::Path;
use object_store::{
    ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, UpdateVersion,
    WriteMultipart,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::{PyBackedBytes, PyBackedStr};
use pyo3::types::PyDict;
use pyo3_file::PyFileLikeObject;
use pyo3_object_store::{PyObjectStore, PyObjectStoreResult};

use crate::attributes::PyAttributes;
use crate::runtime::get_runtime;
use crate::tags::PyTagSet;

pub(crate) struct PyPutMode(PutMode);

impl<'py> FromPyObject<'py> for PyPutMode {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<PyBackedStr>() {
            let s = s.to_ascii_lowercase();
            match s.as_str() {
                "create" => Ok(Self(PutMode::Create)),
                "overwrite" => Ok(Self(PutMode::Overwrite)),
                _ => Err(PyValueError::new_err(format!(
                    "Unexpected input for PutMode: {}",
                    s
                ))),
            }
        } else {
            let update_version = ob.extract::<PyUpdateVersion>()?;
            Ok(Self(PutMode::Update(update_version.0)))
        }
    }
}

pub(crate) struct PyUpdateVersion(UpdateVersion);

impl<'py> FromPyObject<'py> for PyUpdateVersion {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        // Update to use derive(FromPyObject) when default is implemented:
        // https://github.com/PyO3/pyo3/issues/4643
        let dict = ob.extract::<HashMap<String, Bound<PyAny>>>()?;
        Ok(Self(UpdateVersion {
            e_tag: dict.get("e_tag").map(|x| x.extract()).transpose()?,
            version: dict.get("version").map(|x| x.extract()).transpose()?,
        }))
    }
}

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
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(BufReader::new(File::open(path)?)))
        } else if let Ok(buffer) = ob.extract::<PyBackedBytes>() {
            Ok(Self::Buffer(Cursor::new(buffer)))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::with_requirements(
                ob.clone().unbind(),
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

impl<'py> IntoPyObject<'py> for PyPutResult {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let mut dict = IndexMap::with_capacity(2);
        dict.insert("e_tag", self.0.e_tag.into_pyobject(py)?.into_any());
        dict.insert("version", self.0.version.into_pyobject(py)?.into_any());
        dict.into_pyobject(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, file, *, attributes = None, tags = None, mode = None, use_multipart = None, chunk_size = 5242880, max_concurrency = 12))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn put(
    py: Python,
    store: PyObjectStore,
    path: String,
    mut file: MultipartPutInput,
    attributes: Option<PyAttributes>,
    tags: Option<PyTagSet>,
    mode: Option<PyPutMode>,
    use_multipart: Option<bool>,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyObjectStoreResult<PyPutResult> {
    let mut use_multipart = if let Some(use_multipart) = use_multipart {
        use_multipart
    } else {
        file.use_multipart(chunk_size)?
    };

    // If mode is provided and not Overwrite, force a non-multipart put
    if let Some(mode) = &mode {
        if !matches!(mode.0, PutMode::Overwrite) {
            use_multipart = false;
        }
    }

    let runtime = get_runtime(py)?;
    if use_multipart {
        runtime.block_on(put_multipart_inner(
            store.into_inner(),
            &path.into(),
            file,
            chunk_size,
            max_concurrency,
            attributes,
            tags,
        ))
    } else {
        runtime.block_on(put_inner(
            store.into_inner(),
            &path.into(),
            file,
            attributes,
            tags,
            mode,
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, file, *, attributes = None, tags = None, mode = None, use_multipart = None, chunk_size = 5242880, max_concurrency = 12))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn put_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    mut file: MultipartPutInput,
    attributes: Option<PyAttributes>,
    tags: Option<PyTagSet>,
    mode: Option<PyPutMode>,
    use_multipart: Option<bool>,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyResult<Bound<PyAny>> {
    let mut use_multipart = if let Some(use_multipart) = use_multipart {
        use_multipart
    } else {
        file.use_multipart(chunk_size)?
    };

    // If mode is provided and not Overwrite, force a non-multipart put
    if let Some(mode) = &mode {
        if !matches!(mode.0, PutMode::Overwrite) {
            use_multipart = false;
        }
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let result = if use_multipart {
            put_multipart_inner(
                store.into_inner(),
                &path.into(),
                file,
                chunk_size,
                max_concurrency,
                attributes,
                tags,
            )
            .await?
        } else {
            put_inner(
                store.into_inner(),
                &path.into(),
                file,
                attributes,
                tags,
                mode,
            )
            .await?
        };
        Ok(result)
    })
}

async fn put_inner(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    mut reader: MultipartPutInput,
    attributes: Option<PyAttributes>,
    tags: Option<PyTagSet>,
    mode: Option<PyPutMode>,
) -> PyObjectStoreResult<PyPutResult> {
    let mut opts = PutOptions::default();

    if let Some(attributes) = attributes {
        opts.attributes = attributes.into_inner();
    }
    if let Some(tags) = tags {
        opts.tags = tags.into_inner();
    }
    if let Some(mode) = mode {
        opts.mode = mode.0;
    }

    let nbytes = reader.nbytes()?;
    let mut buffer = Vec::with_capacity(nbytes);
    reader.read_to_end(&mut buffer)?;
    let payload = PutPayload::from_bytes(buffer.into());
    Ok(PyPutResult(store.put_opts(path, payload, opts).await?))
}

async fn put_multipart_inner<R: Read>(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    mut reader: R,
    chunk_size: usize,
    max_concurrency: usize,
    attributes: Option<PyAttributes>,
    tags: Option<PyTagSet>,
) -> PyObjectStoreResult<PyPutResult> {
    let mut opts = PutMultipartOpts::default();

    if let Some(attributes) = attributes {
        opts.attributes = attributes.into_inner();
    }
    if let Some(tags) = tags {
        opts.tags = tags.into_inner();
    }

    let upload = store.put_multipart_opts(path, opts).await?;
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
