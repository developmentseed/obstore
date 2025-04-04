use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::{
    ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, UpdateVersion,
    WriteMultipart,
};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyDict;
use pyo3::{intern, IntoPyObjectExt};
use pyo3_bytes::PyBytes;
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

/// Sources to `put` that are pull-based. I.e. we can pull a specific number of bytes from them.
pub(crate) enum PullSource {
    File(BufReader<File>),
    FileLike(PyFileLikeObject),
    Buffer(Cursor<Bytes>),
}

impl PullSource {
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

impl Read for PullSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            Self::FileLike(f) => f.read(buf),
            Self::Buffer(f) => f.read(buf),
        }
    }
}

impl Seek for PullSource {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(f) => f.seek(pos),
            Self::FileLike(f) => f.seek(pos),
            Self::Buffer(f) => f.seek(pos),
        }
    }
}

/// Sources to `put` that are push-based and synchronous.
///
/// I.e. we don't know how large each chunk will be before we receive it.
pub(crate) enum SyncPushSource {
    /// A Python Iterator: An object with a __next__ method that returns a buffer protocol object
    /// (anything that can be extracted into `PyBytes`)
    Iterator(PyObject),
}

impl SyncPushSource {
    fn next_chunk(&mut self) -> PyObjectStoreResult<Option<Bytes>> {
        match self {
            Self::Iterator(iter) => {
                Python::with_gil(|py| match iter.call_method0(py, intern!(py, "__next__")) {
                    Ok(item) => {
                        let buf = item.extract::<PyBytes>(py)?;
                        Ok(Some(buf.into_inner()))
                    }
                    Err(err) => {
                        if err.is_instance_of::<PyStopIteration>(py) {
                            Ok(None)
                        } else {
                            Err(err.into())
                        }
                    }
                })
            }
        }
    }

    fn read_all(&mut self) -> PyObjectStoreResult<PutPayload> {
        let buffers = self.into_iter().collect::<PyObjectStoreResult<Vec<_>>>()?;
        Ok(PutPayload::from_iter(buffers))
    }
}

impl Iterator for SyncPushSource {
    type Item = PyObjectStoreResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk().transpose()
    }
}

/// Sources to `put` that are push-based and asynchronous.
///
/// I.e. we don't know how large each chunk will be before we receive it.
pub(crate) enum AsyncPushSource {
    /// A Python Async Iterator: An object with an __anext__ method that returns a buffer protocol
    /// object (anything that can be extracted into `PyBytes`)
    AsyncIterator(Py<PyAny>),
}

impl AsyncPushSource {
    async fn read_all(&mut self) -> PyObjectStoreResult<PutPayload> {
        let mut buffers = vec![];
        while let Some(buf) = self.next_chunk().await? {
            buffers.push(buf);
        }
        Ok(PutPayload::from_iter(buffers))
    }

    async fn next_chunk(&mut self) -> PyObjectStoreResult<Option<Bytes>> {
        match self {
            Self::AsyncIterator(iter) => {
                // Note: we have to acquire the GIL once to create the future and a separate time
                // to extract the result of the future.
                let future = Python::with_gil(|py| {
                    let coroutine = iter.bind(py).call_method0(intern!(py, "__anext__"))?;
                    pyo3_async_runtimes::tokio::into_future(coroutine)
                })?;

                // This await needs to happen outside of Python::with_gil because you can't use
                // await in a sync closure
                let future_result = future.await;

                Python::with_gil(|py| match future_result {
                    Ok(result) => {
                        let buf = result.extract::<PyBytes>(py)?;
                        Ok(Some(buf.into_inner()))
                    }
                    Err(err) => {
                        if err.is_instance_of::<PyStopAsyncIteration>(py) {
                            Ok(None)
                        } else {
                            Err(err.into())
                        }
                    }
                })
            }
        }
    }
}

// #[derive(Debug)]
pub(crate) enum PutInput {
    /// Input that we can pull from
    Pull(PullSource),

    /// Input that gives us chunks of unknown size, synchronously
    SyncPush(SyncPushSource),

    /// Input that gives us chunks of unknown size, asynchronously
    AsyncPush(AsyncPushSource),
}

impl PutInput {
    /// Whether to use multipart uploads.
    fn use_multipart(&mut self, chunk_size: usize) -> PyObjectStoreResult<bool> {
        match self {
            Self::Pull(pull_source) => pull_source.use_multipart(chunk_size),
            // We always use multipart uploads for push-based sources because we have no way of
            // knowing how large they'll be and we don't want to buffer them into memory.
            _ => Ok(true),
        }
    }

    async fn read_all(&mut self) -> PyObjectStoreResult<PutPayload> {
        match self {
            Self::Pull(pull_source) => match pull_source {
                PullSource::Buffer(buffer) => Ok(buffer.get_ref().clone().into()),
                source => {
                    let mut buf = Vec::new();
                    source.read_to_end(&mut buf)?;
                    Ok(Bytes::from(buf).into())
                }
            },
            Self::SyncPush(push_source) => push_source.read_all(),
            Self::AsyncPush(push_source) => push_source.read_all().await,
        }
    }
}

impl<'py> FromPyObject<'py> for PutInput {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::Pull(PullSource::File(BufReader::new(File::open(
                path,
            )?))))
        } else if let Ok(buffer) = ob.extract::<PyBytes>() {
            Ok(Self::Pull(PullSource::Buffer(Cursor::new(
                buffer.into_inner(),
            ))))
        }
        // Check for file-like object
        else if ob.hasattr(intern!(py, "read"))? && ob.hasattr(intern!(py, "seek"))? {
            Ok(Self::Pull(PullSource::FileLike(
                PyFileLikeObject::py_with_requirements(ob.clone(), true, false, true, false)?,
            )))
        }
        // Ensure we check _first_ for an async generator before a sync one
        else if ob.hasattr(intern!(py, "__aiter__"))? {
            Ok(Self::AsyncPush(AsyncPushSource::AsyncIterator(
                ob.call_method0(intern!(py, "__aiter__"))?.unbind(),
            )))
        } else if ob.hasattr(intern!(py, "__anext__"))? {
            Ok(Self::AsyncPush(AsyncPushSource::AsyncIterator(
                ob.clone().unbind(),
            )))
        } else if ob.hasattr(intern!(py, "__iter__"))? {
            Ok(Self::SyncPush(SyncPushSource::Iterator(
                ob.call_method0(intern!(py, "__iter__"))?.unbind(),
            )))
        } else if ob.hasattr(intern!(py, "__next__"))? {
            Ok(Self::SyncPush(SyncPushSource::Iterator(
                ob.clone().unbind(),
            )))
        } else {
            Err(PyValueError::new_err("Unexpected input for PutInput"))
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
        dict.insert("e_tag", self.0.e_tag.into_bound_py_any(py)?);
        dict.insert("version", self.0.version.into_bound_py_any(py)?);
        dict.into_pyobject(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, path, file, *, attributes=None, tags=None, mode=None, use_multipart=None, chunk_size=5242880, max_concurrency=12))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn put(
    py: Python,
    store: PyObjectStore,
    path: String,
    mut file: PutInput,
    attributes: Option<PyAttributes>,
    tags: Option<PyTagSet>,
    mode: Option<PyPutMode>,
    use_multipart: Option<bool>,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyObjectStoreResult<PyPutResult> {
    if matches!(file, PutInput::AsyncPush(_)) {
        return Err(
            PyValueError::new_err("Async input not allowed in 'put'. Use 'put_async'.").into(),
        );
    }

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
#[pyo3(signature = (store, path, file, *, attributes=None, tags=None, mode=None, use_multipart=None, chunk_size=5242880, max_concurrency=12))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn put_async(
    py: Python,
    store: PyObjectStore,
    path: String,
    mut file: PutInput,
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
    mut reader: PutInput,
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

    let payload = reader.read_all().await?;
    Ok(PyPutResult(store.put_opts(path, payload, opts).await?))
}

async fn put_multipart_inner(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    reader: PutInput,
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
    let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);

    // Make sure to call abort if the multipart upload failed for any reason
    match write_multipart(&mut writer, reader, chunk_size, max_concurrency).await {
        Ok(()) => Ok(PyPutResult(writer.finish().await?)),
        Err(err) => {
            writer.abort().await?;
            Err(err)
        }
    }
}

async fn write_multipart(
    writer: &mut WriteMultipart,
    reader: PutInput,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyObjectStoreResult<()> {
    // Match across pull, push, async push
    match reader {
        PutInput::Pull(mut pull_reader) => loop {
            let mut scratch_buffer = vec![0; chunk_size];
            let read_size = pull_reader.read(&mut scratch_buffer)?;
            if read_size == 0 {
                break;
            } else {
                writer.wait_for_capacity(max_concurrency).await?;
                writer.write(&scratch_buffer[0..read_size]);
            }
        },
        PutInput::SyncPush(push_reader) => {
            for buf in push_reader {
                writer.wait_for_capacity(max_concurrency).await?;
                writer.put(buf?);
            }
        }
        PutInput::AsyncPush(mut push_reader) => {
            // Note: I believe that only one __anext__ call can happen at a time
            while let Some(buf) = push_reader.next_chunk().await? {
                writer.wait_for_capacity(max_concurrency).await?;
                writer.put(buf);
            }
        }
    }

    Ok(())
}
