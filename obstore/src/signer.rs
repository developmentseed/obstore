use core::time::Duration;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use http::{HeaderMap, HeaderName, HeaderValue, Method};
use indexmap::IndexMap;
use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::signer::{SignedUrlOptions, Signer};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_object_store::{
    MaybePrefixedStore, PyAzureStore, PyGCSStore, PyObjectStoreError, PyObjectStoreResult,
    PyS3Store, PyUrl,
};
use url::Url;

use crate::path::PyPaths;

#[derive(Debug)]
pub(crate) enum SignCapableStore {
    S3(Arc<MaybePrefixedStore<AmazonS3>>),
    Gcs(Arc<MaybePrefixedStore<GoogleCloudStorage>>),
    Azure(Arc<MaybePrefixedStore<MicrosoftAzure>>),
}

impl<'py> FromPyObject<'_, 'py> for SignCapableStore {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(store) = obj.cast::<PyS3Store>() {
            Ok(Self::S3(store.get().as_ref().clone()))
        } else if let Ok(store) = obj.cast::<PyGCSStore>() {
            Ok(Self::Gcs(store.get().as_ref().clone()))
        } else if let Ok(store) = obj.cast::<PyAzureStore>() {
            Ok(Self::Azure(store.get().as_ref().clone()))
        } else {
            let py = obj.py();
            // Check for object-store instance from other library
            let cls_name = obj
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
                obj.repr()?
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
            Self::S3(inner) => inner.signed_url(method, path, expires_in),
            Self::Gcs(inner) => inner.signed_url(method, path, expires_in),
            Self::Azure(inner) => inner.signed_url(method, path, expires_in),
        }
    }

    fn signed_url_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        method: Method,
        path: &'life1 Path,
        expires_in: Duration,
        options: &'life2 SignedUrlOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Url>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::S3(inner) => inner.signed_url_opts(method, path, expires_in, options),
            Self::Gcs(inner) => inner.signed_url_opts(method, path, expires_in, options),
            Self::Azure(inner) => inner.signed_url_opts(method, path, expires_in, options),
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
            Self::S3(inner) => inner.signed_urls(method, paths, expires_in),
            Self::Gcs(inner) => inner.signed_urls(method, paths, expires_in),
            Self::Azure(inner) => inner.signed_urls(method, paths, expires_in),
        }
    }
}

pub(crate) struct PyMethod(Method);

impl<'py> FromPyObject<'_, 'py> for PyMethod {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let s = obj.extract::<PyBackedStr>()?;
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
                    "Unsupported HTTP method {other}"
                )))
            }
        };
        Ok(Self(method))
    }
}

pub(crate) struct PySignedHeaders(HeaderMap);

impl<'py> FromPyObject<'_, 'py> for PySignedHeaders {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let input = obj.extract::<IndexMap<PyBackedStr, PyBackedStr>>()?;
        let mut headers = HeaderMap::with_capacity(input.len());
        for (name, value) in input.iter() {
            let header_name = HeaderName::from_str(name).map_err(|err| {
                PyValueError::new_err(format!("Invalid signed header name {name:?}: {err}"))
            })?;
            let header_value = HeaderValue::from_str(value).map_err(|err| {
                PyValueError::new_err(format!("Invalid value for signed header {name:?}: {err}"))
            })?;
            headers.append(header_name, header_value);
        }
        Ok(Self(headers))
    }
}

/// Returns `None` when there is nothing extra to sign, so that signing goes through
/// `signed_url`/`signed_urls` exactly as it does without these arguments.
fn signed_url_options(
    extra_query: Option<IndexMap<String, String>>,
    signed_headers: Option<PySignedHeaders>,
) -> Option<SignedUrlOptions> {
    let mut options = SignedUrlOptions::default().with_query(extra_query.unwrap_or_default());
    if let Some(headers) = signed_headers {
        options.signed_headers = headers.0;
    }
    (!options.is_empty()).then_some(options)
}

#[derive(IntoPyObject)]
pub(crate) struct PyUrls(Vec<PyUrl>);

#[derive(IntoPyObject)]
pub(crate) enum PySignResult {
    One(PyUrl),
    Many(PyUrls),
}

async fn sign_paths(
    store: &SignCapableStore,
    method: Method,
    paths: PyPaths,
    expires_in: Duration,
    options: Option<&SignedUrlOptions>,
) -> object_store::Result<PySignResult> {
    match (paths, options) {
        (PyPaths::One(path), None) => {
            let url = store.signed_url(method, &path, expires_in).await?;
            Ok(PySignResult::One(PyUrl::new(url)))
        }
        (PyPaths::One(path), Some(options)) => {
            let url = store
                .signed_url_opts(method, &path, expires_in, options)
                .await?;
            Ok(PySignResult::One(PyUrl::new(url)))
        }
        (PyPaths::Many(paths), None) => {
            let urls = store.signed_urls(method, &paths, expires_in).await?;
            Ok(PySignResult::Many(PyUrls(
                urls.into_iter().map(PyUrl::new).collect(),
            )))
        }
        (PyPaths::Many(paths), Some(options)) => {
            let mut urls = Vec::with_capacity(paths.len());
            for path in &paths {
                let url = store
                    .signed_url_opts(method.clone(), path, expires_in, options)
                    .await?;
                urls.push(PyUrl::new(url));
            }
            Ok(PySignResult::Many(PyUrls(urls)))
        }
    }
}

#[pyfunction]
#[pyo3(signature = (store, method, paths, expires_in, *, extra_query=None, signed_headers=None))]
pub(crate) fn sign(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    paths: PyPaths,
    expires_in: Duration,
    extra_query: Option<IndexMap<String, String>>,
    signed_headers: Option<PySignedHeaders>,
) -> PyObjectStoreResult<PySignResult> {
    let runtime = get_runtime();
    let options = signed_url_options(extra_query, signed_headers);
    py.detach(|| {
        Ok(runtime.block_on(sign_paths(
            &store,
            method.0,
            paths,
            expires_in,
            options.as_ref(),
        ))?)
    })
}

#[pyfunction]
#[pyo3(signature = (store, method, paths, expires_in, *, extra_query=None, signed_headers=None))]
pub(crate) fn sign_async(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    paths: PyPaths,
    expires_in: Duration,
    extra_query: Option<IndexMap<String, String>>,
    signed_headers: Option<PySignedHeaders>,
) -> PyResult<Bound<PyAny>> {
    let options = signed_url_options(extra_query, signed_headers);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let result = sign_paths(&store, method.0, paths, expires_in, options.as_ref())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(result)
    })
}
