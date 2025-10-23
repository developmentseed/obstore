use std::ops::AddAssign;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use futures::stream::{BoxStream, Fuse};
use futures::StreamExt;
use indexmap::IndexMap;
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::path::Path;
use object_store::{ListResult, ObjectMeta, ObjectStore};
use pyo3::exceptions::{PyImportError, PyStopAsyncIteration, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyDict;
use pyo3::{intern, IntoPyObjectExt, PyTypeInfo};
use pyo3_arrow::export::{Arro3RecordBatch, Arro3Table};
use pyo3_arrow::PyTable;
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_object_store::{
    PyAzureStore, PyGCSStore, PyHttpStore, PyLocalStore, PyMemoryStore, PyObjectStore,
    PyObjectStoreError, PyObjectStoreResult, PyS3Store,
};
use tokio::sync::Mutex;

pub(crate) enum MaybePaginatedListStore {
    SupportsPagination(Arc<dyn PaginatedListStore>),
    NoPagination(Arc<dyn ObjectStore>),
}

impl<'py> FromPyObject<'py> for MaybePaginatedListStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(store) = ob.downcast::<PyS3Store>() {
            Ok(Self::SupportsPagination(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyAzureStore>() {
            Ok(Self::SupportsPagination(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyGCSStore>() {
            Ok(Self::SupportsPagination(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyHttpStore>() {
            Ok(Self::NoPagination(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyLocalStore>() {
            Ok(Self::NoPagination(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyMemoryStore>() {
            Ok(Self::NoPagination(store.get().as_ref().clone()))
        } else {
            let py = ob.py();
            // Check for object-store instance from other library
            let cls_name = ob
                .getattr(intern!(py, "__class__"))?
                .getattr(intern!(py, "__name__"))?
                .extract::<PyBackedStr>()?;
            if [
                PyAzureStore::NAME,
                PyGCSStore::NAME,
                PyHttpStore::NAME,
                PyLocalStore::NAME,
                PyMemoryStore::NAME,
                PyS3Store::NAME,
            ]
            .contains(&cls_name.as_ref())
            {
                return Err(PyValueError::new_err("You must use an object store instance exported from **the same library** as this function. They cannot be used across libraries.\nThis is because object store instances are compiled with a specific version of Rust and Python." ));
            }

            Err(PyValueError::new_err(format!(
                "Expected an object store instance, got {}",
                ob.repr()?
            )))
        }
    }
}

pub(crate) struct PyObjectMeta(ObjectMeta);

impl PyObjectMeta {
    pub(crate) fn new(meta: ObjectMeta) -> Self {
        Self(meta)
    }
}

impl AsRef<ObjectMeta> for PyObjectMeta {
    fn as_ref(&self) -> &ObjectMeta {
        &self.0
    }
}

impl From<ObjectMeta> for PyObjectMeta {
    fn from(value: ObjectMeta) -> Self {
        Self(value)
    }
}

impl<'py> IntoPyObject<'py> for PyObjectMeta {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let mut dict = IndexMap::with_capacity(5);
        // Note, this uses "path" instead of "location" because we standardize the API to accept
        // the keyword "path" everywhere.
        dict.insert("path", self.0.location.as_ref().into_bound_py_any(py)?);
        dict.insert("last_modified", self.0.last_modified.into_bound_py_any(py)?);
        dict.insert("size", self.0.size.into_bound_py_any(py)?);
        dict.insert("e_tag", self.0.e_tag.into_bound_py_any(py)?);
        dict.insert("version", self.0.version.into_bound_py_any(py)?);
        dict.into_pyobject(py)
    }
}

// Note: we fuse the underlying stream so that we can get `None` multiple times.
//
// In general, you can't poll an iterator after it's already emitted None. But the issue here is
// that we need _two_ states for the Python async iterator. It needs to first get all returned
// results, and then it needs its **own** PyStopAsyncIteration/PyStopIteration. But these are _two_
// results to be returned from the Rust call, and we can't return them both at the same time. The
// easiest way to fix this is to safely return `None` from the stream multiple times. The first
// time we see `None` we return any batched results, the second time we see `None`, there are no
// batched results and we return PyStopAsyncIteration/PyStopIteration.
//
// Note: another way we could solve this is by removing any batching from the stream, but batching
// should improve the performance of the Rust/Python bridge.
//
// Ref:
// - https://stackoverflow.com/a/66964599
// - https://docs.rs/futures/latest/futures/prelude/stream/trait.StreamExt.html#method.fuse
#[pyclass(name = "ListStream", frozen)]
pub(crate) struct PyListStream {
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<ObjectMeta>>>>>,
    chunk_size: usize,
    return_arrow: bool,
}

impl PyListStream {
    fn new(
        stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
        chunk_size: usize,
        return_arrow: bool,
    ) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream.fuse())),
            chunk_size,
            return_arrow,
        }
    }
}

#[pymethods]
impl PyListStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn collect(&self, py: Python) -> PyResult<PyListIterResult> {
        let runtime = get_runtime();
        let stream = self.stream.clone();
        py.detach(|| runtime.block_on(collect_stream(stream, self.return_arrow)))
    }

    fn collect_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, collect_stream(stream, self.return_arrow))
    }

    fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            next_stream(stream, self.chunk_size, false, self.return_arrow),
        )
    }

    fn __next__(&self, py: Python) -> PyResult<PyListIterResult> {
        let runtime = get_runtime();
        let stream = self.stream.clone();
        py.detach(|| {
            runtime.block_on(next_stream(
                stream,
                self.chunk_size,
                true,
                self.return_arrow,
            ))
        })
    }
}

#[derive(IntoPyObject)]
enum PyListIterResult {
    Arrow(Arro3RecordBatch),
    Native(Vec<PyObjectMeta>),
}

async fn next_stream(
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<ObjectMeta>>>>>,
    chunk_size: usize,
    sync: bool,
    return_arrow: bool,
) -> PyResult<PyListIterResult> {
    let mut stream = stream.lock().await;
    let mut metas: Vec<PyObjectMeta> = vec![];
    loop {
        match stream.next().await {
            Some(Ok(meta)) => {
                metas.push(PyObjectMeta(meta));
                if metas.len() >= chunk_size {
                    match return_arrow {
                        true => {
                            return Ok(PyListIterResult::Arrow(
                                object_meta_to_arrow(&metas).into(),
                            ));
                        }
                        false => {
                            return Ok(PyListIterResult::Native(metas));
                        }
                    }
                }
            }
            Some(Err(e)) => return Err(PyObjectStoreError::from(e).into()),
            None => {
                if metas.is_empty() {
                    // Depending on whether the iteration is sync or not, we raise either a
                    // StopIteration or a StopAsyncIteration
                    if sync {
                        return Err(PyStopIteration::new_err("stream exhausted"));
                    } else {
                        return Err(PyStopAsyncIteration::new_err("stream exhausted"));
                    }
                } else {
                    match return_arrow {
                        true => {
                            return Ok(PyListIterResult::Arrow(
                                object_meta_to_arrow(&metas).into(),
                            ));
                        }
                        false => {
                            return Ok(PyListIterResult::Native(metas));
                        }
                    }
                }
            }
        };
    }
}

async fn collect_stream(
    stream: Arc<Mutex<Fuse<BoxStream<'static, object_store::Result<ObjectMeta>>>>>,
    return_arrow: bool,
) -> PyResult<PyListIterResult> {
    let mut stream = stream.lock().await;
    let mut metas: Vec<PyObjectMeta> = vec![];
    loop {
        match stream.next().await {
            Some(Ok(meta)) => {
                metas.push(PyObjectMeta(meta));
            }
            Some(Err(e)) => return Err(PyObjectStoreError::from(e).into()),
            None => match return_arrow {
                true => {
                    return Ok(PyListIterResult::Arrow(object_meta_to_arrow(&metas).into()));
                }
                false => {
                    return Ok(PyListIterResult::Native(metas));
                }
            },
        };
    }
}

/// Array capacities for each string array
struct ObjectMetaCapacity {
    location: usize,
    e_tag: usize,
    version: usize,
}

impl ObjectMetaCapacity {
    fn new() -> Self {
        Self {
            location: 0,
            e_tag: 0,
            version: 0,
        }
    }
}

impl AddAssign<&ObjectMeta> for ObjectMetaCapacity {
    fn add_assign(&mut self, rhs: &ObjectMeta) {
        self.location += rhs.location.as_ref().len();
        if let Some(e_tag) = rhs.e_tag.as_ref() {
            self.e_tag += e_tag.len();
        }
        if let Some(version) = rhs.version.as_ref() {
            self.version += version.len();
        }
    }
}

fn object_meta_capacities(metas: &[PyObjectMeta]) -> ObjectMetaCapacity {
    let mut capacity = ObjectMetaCapacity::new();
    for meta in metas {
        capacity += &meta.0;
    }
    capacity
}

fn object_meta_to_arrow(metas: &[PyObjectMeta]) -> RecordBatch {
    let capacity = object_meta_capacities(metas);

    let mut location = StringBuilder::with_capacity(metas.len(), capacity.location);
    let mut last_modified = TimestampMicrosecondBuilder::with_capacity(metas.len());
    let mut size = UInt64Builder::with_capacity(metas.len());
    let mut e_tag = StringBuilder::with_capacity(metas.len(), capacity.e_tag);
    let mut version = StringBuilder::with_capacity(metas.len(), capacity.version);

    for meta in metas {
        location.append_value(meta.as_ref().location.as_ref());
        last_modified.append_value(meta.as_ref().last_modified.timestamp_micros());
        size.append_value(meta.as_ref().size as _);
        e_tag.append_option(meta.as_ref().e_tag.as_ref());
        version.append_option(meta.as_ref().version.as_ref());
    }

    let fields = vec![
        // Note, this uses "path" instead of "location" because we standardize the API to accept
        // the keyword "path" everywhere.
        Field::new("path", DataType::Utf8, false),
        Field::new(
            "last_modified",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("size", DataType::UInt64, false),
        Field::new("e_tag", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
    ];
    let schema = Schema::new(fields);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(location.finish()),
        Arc::new(last_modified.finish().with_timezone("UTC")),
        Arc::new(size.finish()),
        Arc::new(e_tag.finish()),
        Arc::new(version.finish()),
    ];
    // This unwrap is ok because we know the RecordBatch is valid.
    RecordBatch::try_new(schema.into(), columns).unwrap()
}

pub(crate) struct PyListResult {
    result: ListResult,
    return_arrow: bool,
}

impl PyListResult {
    fn new(result: ListResult, return_arrow: bool) -> Self {
        Self {
            result,
            return_arrow,
        }
    }
}

impl<'py> IntoPyObject<'py> for PyListResult {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let mut dict = IndexMap::with_capacity(2);
        dict.insert(
            "common_prefixes",
            self.result
                .common_prefixes
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
                .into_bound_py_any(py)?,
        );
        let objects = self
            .result
            .objects
            .into_iter()
            .map(PyObjectMeta)
            .collect::<Vec<_>>();
        let objects = if self.return_arrow {
            let batch = object_meta_to_arrow(&objects);
            let schema = batch.schema();
            Arro3Table::from(PyTable::try_new(vec![batch], schema).unwrap()).into_bound_py_any(py)
        } else {
            objects.into_bound_py_any(py)
        }?;
        dict.insert("objects", objects);
        dict.into_pyobject(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, prefix=None, *, offset=None, chunk_size=50, return_arrow=false))]
pub(crate) fn list(
    py: Python,
    store: MaybePaginatedListStore,
    prefix: Option<String>,
    offset: Option<String>,
    chunk_size: usize,
    return_arrow: bool,
) -> PyObjectStoreResult<PyListStream> {
    if return_arrow {
        // Ensure that arro3.core is installed if returning as arrow.
        // The IntoPy impl is infallible, but `PyRecordBatch::to_arro3` can fail if arro3 is not
        // installed.
        let msg = concat!(
            "arro3.core is a required dependency for returning results as arrow.\n",
            "\nInstall with `pip install arro3-core`."
        );
        py.import(intern!(py, "arro3.core"))
            .map_err(|err| PyImportError::new_err(format!("{msg}\n\n{err}")))?;
    }

    let stream = match store {
        MaybePaginatedListStore::SupportsPagination(paginated_store) => {
            create_paginated_stream(paginated_store, prefix, offset, chunk_size)
        }
        MaybePaginatedListStore::NoPagination(object_store) => {
            create_filtered_stream(object_store, prefix, offset)
        }
    };
    Ok(PyListStream::new(stream, chunk_size, return_arrow))
}

#[pyfunction]
#[pyo3(signature = (store, prefix=None, *, return_arrow=false))]
pub(crate) fn list_with_delimiter(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
    return_arrow: bool,
) -> PyObjectStoreResult<PyListResult> {
    let runtime = get_runtime();
    py.detach(|| {
        let out = runtime.block_on(list_with_delimiter_materialize(
            store.into_inner(),
            prefix.map(|s| s.into()).as_ref(),
            return_arrow,
        ))?;
        Ok::<_, PyObjectStoreError>(out)
    })
}

#[pyfunction]
#[pyo3(signature = (store, prefix=None, *, return_arrow=false))]
pub(crate) fn list_with_delimiter_async(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
    return_arrow: bool,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = list_with_delimiter_materialize(
            store.into_inner(),
            prefix.map(|s| s.into()).as_ref(),
            return_arrow,
        )
        .await?;
        Ok(out)
    })
}

async fn list_with_delimiter_materialize(
    store: Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
    return_arrow: bool,
) -> PyObjectStoreResult<PyListResult> {
    let list_result = store.list_with_delimiter(prefix).await?;
    Ok(PyListResult::new(list_result, return_arrow))
}

fn create_paginated_stream(
    store: Arc<dyn PaginatedListStore>,
    prefix: Option<String>,
    offset: Option<String>,
    chunk_size: usize,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    // Create a stream that will fetch from the paginated store
    let stream = futures::stream::unfold(
        (store, prefix, offset, None, true),
        move |(store, prefix, offset, page_token, has_more)| async move {
            if !has_more {
                return None;
            }

            let opts = PaginatedListOptions {
                offset: offset.clone(),
                delimiter: None,
                max_keys: Some(chunk_size),
                page_token,
                ..Default::default()
            };

            match store.list_paginated(prefix.as_deref(), opts).await {
                Ok(result) => {
                    let next_has_more = result.page_token.is_some();
                    let next_page_token = result.page_token;
                    let objects = result.result.objects;

                    let next_state = (store, prefix, offset, next_page_token, next_has_more);
                    Some((objects, next_state))
                }
                Err(_e) => {
                    // TODO: propagate error
                    // For errors, return empty list and stop
                    Some((Vec::new(), (store, prefix, offset, None, false)))
                }
            }
        },
    )
    .flat_map(|objects| futures::stream::iter(objects.into_iter().map(Ok)));

    Box::pin(stream)
}

fn create_filtered_stream(
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
    offset: Option<String>,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    // For substring filtering, we need to split the prefix into:
    // 1. A directory prefix for efficient listing
    // 2. A substring filter to apply to the results
    let (list_prefix, substring_filter) = if let Some(prefix_str) = &prefix {
        if let Some((dir_prefix, substring)) = prefix_str.rsplit_once('/') {
            (Some(dir_prefix.to_string()), Some(substring.to_string()))
        } else {
            (None, Some(prefix_str.clone()))
        }
    } else {
        (None, None)
    };

    let prefix_path = list_prefix.map(|s| s.into());
    let base_stream = if let Some(offset) = offset {
        store.list_with_offset(prefix_path.as_ref(), &offset.into())
    } else {
        store.list(prefix_path.as_ref())
    };

    // Apply substring filtering if needed
    let filtered_stream = if let Some(substring) = substring_filter {
        Box::pin(base_stream.filter_map(move |result| {
            let substring = substring.clone();
            async move {
                match result {
                    Ok(meta) => {
                        // Extract filename from path for substring matching
                        let path_str = meta.location.as_ref();
                        if let Some(filename) = path_str.split('/').last() {
                            if filename.contains(&substring) {
                                Some(Ok(meta))
                            } else {
                                None
                            }
                        } else {
                            Some(Ok(meta))
                        }
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }))
    } else {
        base_stream
    };

    filtered_stream
}
