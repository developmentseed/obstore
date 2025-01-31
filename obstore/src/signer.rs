use core::time::Duration;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::Method;
use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::signer::Signer;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_object_store::{
    MaybePrefixedStore, PyAzureStore, PyGCSStore, PyObjectStoreError, PyObjectStoreResult,
    PyS3Store, PyUrl,
};
use url::Url;

use crate::path::PyPaths;
use crate::runtime::get_runtime;

#[derive(Debug)]
pub(crate) enum SignCapableStore {
    S3(Arc<MaybePrefixedStore<AmazonS3>>),
    Gcs(Arc<MaybePrefixedStore<GoogleCloudStorage>>),
    Azure(Arc<MaybePrefixedStore<MicrosoftAzure>>),
}

impl<'py> FromPyObject<'py> for SignCapableStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(store) = ob.downcast::<PyS3Store>() {
            Ok(Self::S3(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyGCSStore>() {
            Ok(Self::Gcs(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyAzureStore>() {
            Ok(Self::Azure(store.get().as_ref().clone()))
        } else {
            let py = ob.py();
            // Check for object-store instance from other library
            let cls_name = ob
                .getattr(intern!(py, "__class__"))?
                .getattr(intern!(py, "__name__"))?
                .extract::<PyBackedStr>()?;
            if [
                "AzureStore",
                "GCSStore",
                "HTTPStore",
                "LocalStore",
                "MemoryStore",
                "S3Store",
            ]
            .contains(&cls_name.as_ref())
            {
                return Err(PyValueError::new_err("You must use an object store instance exported from **the same library** as this function. They cannot be used across libraries.\nThis is because object store instances are compiled with a specific version of Rust and Python." ));
            }

            Err(PyValueError::new_err(format!(
                "Expected an S3Store, GCSStore, or AzureStore instance, got {}",
                ob.repr()?
            )))
        }
    }
}

impl Signer for SignCapableStore {
    fn signed_url<'life0, 'life1, 'async_trait>(
        &'life0 self,
        method: Method,
        path: &'life1 Path,
        expires_in: Duration,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Url>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::S3(inner) => inner.as_ref().inner().signed_url(method, path, expires_in),
            Self::Gcs(inner) => inner.as_ref().inner().signed_url(method, path, expires_in),
            Self::Azure(inner) => inner.as_ref().inner().signed_url(method, path, expires_in),
        }
    }

    fn signed_urls<'life0, 'life1, 'async_trait>(
        &'life0 self,
        method: Method,
        paths: &'life1 [Path],
        expires_in: Duration,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Vec<Url>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::S3(inner) => inner
                .as_ref()
                .inner()
                .signed_urls(method, paths, expires_in),
            Self::Gcs(inner) => inner
                .as_ref()
                .inner()
                .signed_urls(method, paths, expires_in),
            Self::Azure(inner) => inner
                .as_ref()
                .inner()
                .signed_urls(method, paths, expires_in),
        }
    }
}

pub(crate) struct PyMethod(Method);

impl<'py> FromPyObject<'py> for PyMethod {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?;
        let method = match s.as_ref() {
            "GET" => Method::GET,
            "PUT" => Method::PUT,
            "POST" => Method::POST,
            "HEAD" => Method::HEAD,
            "PATCH" => Method::PATCH,
            "TRACE" => Method::TRACE,
            "DELETE" => Method::DELETE,
            "OPTIONS" => Method::OPTIONS,
            "CONNECT" => Method::CONNECT,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported HTTP method {}",
                    other
                )))
            }
        };
        Ok(Self(method))
    }
}

#[derive(IntoPyObject)]
pub(crate) struct PyUrls(Vec<PyUrl>);

#[derive(IntoPyObject)]
pub(crate) enum PySignResult {
    One(PyUrl),
    Many(PyUrls),
}

#[pyfunction]
pub(crate) fn sign(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    paths: PyPaths,
    expires_in: Duration,
) -> PyObjectStoreResult<PySignResult> {
    let runtime = get_runtime(py)?;
    let method = method.0;

    py.allow_threads(|| match paths {
        PyPaths::One(path) => {
            let url = runtime.block_on(store.signed_url(method, &path, expires_in))?;
            Ok(PySignResult::One(PyUrl::new(url)))
        }
        PyPaths::Many(paths) => {
            let urls = runtime.block_on(store.signed_urls(method, &paths, expires_in))?;
            Ok(PySignResult::Many(PyUrls(
                urls.into_iter().map(PyUrl::new).collect(),
            )))
        }
    })
}

#[pyfunction]
pub(crate) fn sign_async(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    paths: PyPaths,
    expires_in: Duration,
) -> PyResult<Bound<PyAny>> {
    let method = method.0;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        match paths {
            PyPaths::One(path) => {
                let url = store
                    .signed_url(method, &path, expires_in)
                    .await
                    .map_err(PyObjectStoreError::ObjectStoreError)?;
                Ok(PySignResult::One(PyUrl::new(url)))
            }
            PyPaths::Many(paths) => {
                let urls = store
                    .signed_urls(method, &paths, expires_in)
                    .await
                    .map_err(PyObjectStoreError::ObjectStoreError)?;
                Ok(PySignResult::Many(PyUrls(
                    urls.into_iter().map(PyUrl::new).collect(),
                )))
            }
        }
    })
}
