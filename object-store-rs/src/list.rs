use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::{ListResult, ObjectMeta, ObjectStore};
use ouroboros::self_referencing;
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;
use tokio::sync::Mutex;

use crate::runtime::get_runtime;

pub(crate) struct PyObjectMeta(ObjectMeta);

impl PyObjectMeta {
    pub(crate) fn new(meta: ObjectMeta) -> Self {
        Self(meta)
    }
}

impl IntoPy<PyObject> for PyObjectMeta {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = IndexMap::with_capacity(5);
        // Note, this uses "path" instead of "location" because we standardize the API to accept
        // the keyword "path" everywhere.
        dict.insert("path", self.0.location.as_ref().into_py(py));
        dict.insert("last_modified", self.0.last_modified.into_py(py));
        dict.insert("size", self.0.size.into_py(py));
        dict.insert("e_tag", self.0.e_tag.into_py(py));
        dict.insert("version", self.0.version.into_py(py));
        dict.into_py(py)
    }
}

pub(crate) struct PyMaterializedListResult(ListResult);

impl IntoPy<PyObject> for PyMaterializedListResult {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = IndexMap::with_capacity(2);
        dict.insert(
            "common_prefixes",
            self.0
                .common_prefixes
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
                .into_py(py),
        );
        dict.insert(
            "objects",
            self.0
                .objects
                .into_iter()
                .map(PyObjectMeta)
                .collect::<Vec<_>>()
                .into_py(py),
        );
        dict.into_py(py)
    }
}

#[pyclass(name = "ListResult")]
#[self_referencing]
pub(crate) struct PyListResult {
    store: Arc<dyn ObjectStore>,
    #[borrows(store)]
    payload: BoxStream<'this, object_store::Result<ObjectMeta>>,
}

// impl PyListResult {
//     fn new(
//         // store: Arc<dyn ObjectStore>,
//         // prefix: Option<&Path>,
//         stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
//         min_chunk_size: usize,
//     ) -> Self {
//         // let stream = store.as_ref().list(prefix);
//         Self {
//             payload: stream,
//             min_chunk_size,
//         }
//     }
// }

// #[pyclass(name = "ListStream")]
// pub struct PyListStream {
//     stream: Arc<Mutex<BoxStream<'static, object_store::Result<ObjectMeta>>>>,
//     min_chunk_size: usize,
// }

// impl PyListStream {
//     fn new(
//         stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
//         min_chunk_size: usize,
//     ) -> Self {
//         Self {
//             stream: Arc::new(Mutex::new(stream)),
//             min_chunk_size,
//         }
//     }
// }

// async fn next_stream(
//     stream: Arc<Mutex<BoxStream<'static, object_store::Result<ObjectMeta>>>>,
//     min_chunk_size: usize,
//     sync: bool,
// ) -> PyResult<()> {
//     let mut stream = stream.lock().await;
//     let mut metas: Vec<ObjectMeta> = vec![];
//     loop {
//         match stream.next().await {
//             Some(Ok(meta)) => {
//                 metas.push(meta);
//                 if metas.len() >= min_chunk_size {
//                     todo!()
//                     // return Ok(PyBytesWrapper::new_multiple(buffers));
//                 }
//             }
//             Some(Err(e)) => return Err(PyObjectStoreError::from(e).into()),
//             None => {
//                 if metas.is_empty() {
//                     // Depending on whether the iteration is sync or not, we raise either a
//                     // StopIteration or a StopAsyncIteration
//                     if sync {
//                         return Err(PyStopIteration::new_err("stream exhausted"));
//                     } else {
//                         return Err(PyStopAsyncIteration::new_err("stream exhausted"));
//                     }
//                 } else {
//                     todo!()
//                     // return Ok(PyBytesWrapper::new_multiple(buffers));
//                 }
//             }
//         };
//     }
// }

// #[pymethods]
// impl PyListStream {
//     fn __aiter__(slf: Py<Self>) -> Py<Self> {
//         slf
//     }

//     fn __iter__(slf: Py<Self>) -> Py<Self> {
//         slf
//     }

//     fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
//         let stream = self.stream.clone();
//         pyo3_async_runtimes::tokio::future_into_py(
//             py,
//             next_stream(stream, self.min_chunk_size, false),
//         )
//     }

//     fn __next__<'py>(&'py self, py: Python<'py>) -> PyResult<()> {
//         let runtime = get_runtime(py)?;
//         let stream = self.stream.clone();
//         runtime.block_on(next_stream(stream, self.min_chunk_size, true))
//     }
// }

// #[pyo3(signature = (store, prefix = None, *, min_chunk_size = 1000))]
// pub(crate) fn list<'py>(
//     py: Python<'py>,
//     store: PyObjectStore,
//     prefix: Option<String>,
//     min_chunk_size: usize,
// ) -> PyObjectStoreResult<PyListResult> {
//     // todo!()
//     // // let runtime = get_runtime(py)?;
//     // let store = store.into_inner();
//     let stream = store.as_ref().list(prefix.map(|s| s.into()).as_ref());
//     // todo!()
//     Ok(PyListResult::new(stream, min_chunk_size))
//     // Ok::<_, PyObjectStoreError>(PyListResult::new(store, stream, min_chunk_size))
//     // // py.allow_threads(move || {

//     //     // let x = runtime.block_on(fut);
//     //     // let out = runtime.block_on(list_materialize(
//     //     //     store.into_inner(),
//     //     //     prefix.map(|s| s.into()).as_ref(),
//     //     // ))?;
//     //     // Ok::<_, PyObjectStoreError>(out)
//     // })

#[pyfunction]
#[pyo3(signature = (store, prefix = None, *, offset = None, max_items = 2000))]
pub(crate) fn list(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
    offset: Option<String>,
    max_items: Option<usize>,
) -> PyObjectStoreResult<Vec<PyObjectMeta>> {
    let store = store.into_inner();
    let prefix = prefix.map(|s| s.into());
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        let stream = if let Some(offset) = offset {
            store.list_with_offset(prefix.as_ref(), &offset.into())
        } else {
            store.list(prefix.as_ref())
        };
        let out = runtime.block_on(materialize_list_stream(stream, max_items))?;
        Ok::<_, PyObjectStoreError>(out)
    })
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None, *, offset = None, max_items = 2000))]
pub(crate) fn list_async(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
    offset: Option<String>,
    max_items: Option<usize>,
) -> PyResult<Bound<PyAny>> {
    let store = store.into_inner();
    let prefix = prefix.map(|s| s.into());

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let stream = if let Some(offset) = offset {
            store.list_with_offset(prefix.as_ref(), &offset.into())
        } else {
            store.list(prefix.as_ref())
        };
        Ok(materialize_list_stream(stream, max_items).await?)
    })
}

async fn materialize_list_stream(
    mut stream: BoxStream<'_, object_store::Result<ObjectMeta>>,
    max_items: Option<usize>,
) -> PyObjectStoreResult<Vec<PyObjectMeta>> {
    let mut result = vec![];
    while let Some(object) = stream.next().await {
        result.push(PyObjectMeta(object?));
        if let Some(max_items) = max_items {
            if result.len() >= max_items {
                return Ok(result);
            }
        }
    }

    Ok(result)
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None))]
pub(crate) fn list_with_delimiter(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
) -> PyObjectStoreResult<PyMaterializedListResult> {
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        let out = runtime.block_on(list_with_delimiter_materialize(
            store.into_inner(),
            prefix.map(|s| s.into()).as_ref(),
        ))?;
        Ok::<_, PyObjectStoreError>(out)
    })
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None))]
pub(crate) fn list_with_delimiter_async(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out =
            list_with_delimiter_materialize(store.into_inner(), prefix.map(|s| s.into()).as_ref())
                .await?;
        Ok(out)
    })
}

async fn list_with_delimiter_materialize(
    store: Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
) -> PyObjectStoreResult<PyMaterializedListResult> {
    let list_result = store.list_with_delimiter(prefix).await?;
    Ok(PyMaterializedListResult(list_result))
}
