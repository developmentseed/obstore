//! The Python-facing class: construction, configuration, pickling.

use std::sync::Arc;

use object_store::client::{HttpConnector, ReqwestConnector};
use object_store::path::Path;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};
use url::Url;

use crate::error::PyObjectStoreResult;
use crate::prefix::MaybePrefixedStore;
use crate::retry::PyRetryConfig;
use crate::{PyClientOptions, PyUrl};

use super::list::decode_prefix;
use super::signer::PySigner;
use super::{RemoteSignedS3Store, STORE};

/// Resolve an `s3://bucket/prefix` location against an S3 endpoint into the HTTPS URL that
/// [`PyRemoteSignedS3Store::new`] takes.
fn s3_endpoint_url(
    url: &Url,
    endpoint: &Url,
    virtual_hosted_style_request: bool,
) -> PyResult<String> {
    if !matches!(url.scheme(), "s3" | "s3a") {
        return Err(PyValueError::new_err(format!(
            "Expected an s3:// or s3a:// URL, got {}. Pass an HTTPS endpoint URL to \
             RemoteSignedS3Store() directly instead.",
            url.scheme(),
        )));
    }
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| PyValueError::new_err(format!("{url} does not name a bucket")))?;
    let prefix = url
        .path_segments()
        .into_iter()
        .flatten()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/");

    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err(PyValueError::new_err(format!(
            "endpoint must be an http:// or https:// URL, got {endpoint}",
        )));
    }
    // An endpoint served below a path would make the split between endpoint, bucket and key
    // prefix ambiguous, so require a bare origin and let the caller assemble the URL itself.
    if !matches!(endpoint.path(), "" | "/") {
        return Err(PyValueError::new_err(format!(
            "endpoint must not include a path, got {}. Build the full URL yourself and pass \
             it to RemoteSignedS3Store() instead.",
            endpoint.path(),
        )));
    }

    let mut resolved = endpoint.clone();
    resolved.set_query(None);
    resolved.set_fragment(None);
    if virtual_hosted_style_request {
        let host = endpoint.host_str().ok_or_else(|| {
            PyValueError::new_err(format!("endpoint {endpoint} does not include a host"))
        })?;
        resolved
            .set_host(Some(&format!("{bucket}.{host}")))
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        resolved.set_path(&format!("/{prefix}"));
    } else {
        resolved.set_path(&format!("/{bucket}/{prefix}"));
    }
    Ok(resolved.into())
}

/// The constructor arguments of a [`PyRemoteSignedS3Store`], retained for pickling.
#[derive(Debug, Clone)]
struct RemoteSignedS3Config {
    url: String,
    signer: PySigner,
    virtual_hosted_style_request: bool,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
    /// The bucket named by `url`, and the key prefix it implies. Derived rather than passed,
    /// but kept here so the Python getters can report them.
    bucket: String,
    prefix: Option<Path>,
}

impl RemoteSignedS3Config {
    fn __getnewargs_ex__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let args =
            PyTuple::new(py, [self.url.clone().into_bound_py_any(py)?])?.into_bound_py_any(py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(intern!(py, "signer"), self.signer.clone())?;
        if self.virtual_hosted_style_request {
            kwargs.set_item(intern!(py, "virtual_hosted_style_request"), true)?;
        }
        if let Some(client_options) = &self.client_options {
            kwargs.set_item(intern!(py, "client_options"), client_options.clone())?;
        }
        if let Some(retry_config) = &self.retry_config {
            kwargs.set_item(intern!(py, "retry_config"), retry_config.clone())?;
        }
        PyTuple::new(py, [args, kwargs.into_bound_py_any(py)?])
    }

    /// Whether two stores were built from equivalent arguments.
    ///
    /// The signer is compared by identity: two distinct callables cannot be shown to sign
    /// equivalently.
    fn eq(&self, other: &Self, py: Python<'_>) -> bool {
        self.url == other.url
            && self.virtual_hosted_style_request == other.virtual_hosted_style_request
            && self.client_options == other.client_options
            && self.retry_config == other.retry_config
            && self.signer.0.bind(py).is(other.signer.0.bind(py))
    }
}

/// Python-native wrapper for [`RemoteSignedS3Store`].
#[derive(Debug, Clone)]
#[pyclass(name = "RemoteSignedS3Store", frozen, subclass, from_py_object)]
pub struct PyRemoteSignedS3Store {
    store: Arc<MaybePrefixedStore<RemoteSignedS3Store>>,
    /// The arguments used for pickling. This must stay in sync with the underlying store.
    config: RemoteSignedS3Config,
}

impl AsRef<Arc<MaybePrefixedStore<RemoteSignedS3Store>>> for PyRemoteSignedS3Store {
    fn as_ref(&self) -> &Arc<MaybePrefixedStore<RemoteSignedS3Store>> {
        &self.store
    }
}

impl PyRemoteSignedS3Store {
    /// Consume self and return the underlying [`RemoteSignedS3Store`].
    pub fn into_inner(self) -> Arc<MaybePrefixedStore<RemoteSignedS3Store>> {
        self.store
    }
}

#[pymethods]
impl PyRemoteSignedS3Store {
    #[new]
    #[pyo3(signature = (url, signer, *, virtual_hosted_style_request=false, client_options=None, retry_config=None))]
    fn new(
        url: String,
        signer: PySigner,
        virtual_hosted_style_request: bool,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let parsed = Url::parse(&url).map_err(|error| PyValueError::new_err(error.to_string()))?;
        let mut segments = parsed
            .path_segments()
            .ok_or_else(|| PyValueError::new_err(format!("{STORE} URL must include a path")))?
            .filter(|segment| !segment.is_empty());

        // In path style the first segment names the bucket and the rest is the key prefix; in
        // virtual-hosted style the bucket is the host's first label, so every segment is prefix.
        let mut bucket_url = parsed.clone();
        bucket_url.set_query(None);
        bucket_url.set_fragment(None);
        let (bucket, prefix) = if virtual_hosted_style_request {
            let host = parsed.host_str().ok_or_else(|| {
                PyValueError::new_err(format!(
                    "{STORE} URL must include a host naming the bucket when \
                     virtual_hosted_style_request is set"
                ))
            })?;
            let bucket = host.split('.').next().unwrap_or(host).to_owned();
            bucket_url.set_path("/");
            (bucket, decode_prefix(segments))
        } else {
            // The bucket segment needs no decoding: S3 bucket names are restricted to
            // characters that a URL never percent-encodes.
            let bucket = segments
                .next()
                .ok_or_else(|| PyValueError::new_err(format!("{STORE} URL must include a bucket")))?
                .to_owned();
            bucket_url.set_path(&format!("/{bucket}/"));
            (bucket, decode_prefix(segments))
        };
        // An empty prefix stays `None`, so that `MaybePrefixedStore` passes paths straight
        // through instead of rewriting them.
        let prefix = if prefix.is_empty() {
            None
        } else {
            Some(Path::parse(prefix).map_err(|error| PyValueError::new_err(error.to_string()))?)
        };

        let options = client_options.clone().map(Into::into).unwrap_or_default();
        let client = ReqwestConnector {}.connect(&options)?;
        let store = RemoteSignedS3Store {
            bucket_url,
            bucket: bucket.clone(),
            signer: signer.clone(),
            client,
            retry: retry_config.clone().map(Into::into).unwrap_or_default(),
        };
        Ok(Self {
            store: Arc::new(MaybePrefixedStore::new(store, prefix.clone())),
            config: RemoteSignedS3Config {
                url,
                signer,
                virtual_hosted_style_request,
                client_options,
                retry_config,
                bucket,
                prefix,
            },
        })
    }

    /// Construct a store from an `s3://` location plus the S3 endpoint that serves it.
    ///
    /// Catalogs hand out locations as `s3://bucket/key` and the endpoint separately, so this
    /// saves callers assembling the HTTPS URL themselves.
    #[classmethod]
    #[pyo3(signature = (url, signer, *, endpoint, virtual_hosted_style_request=false, client_options=None, retry_config=None))]
    fn from_s3_url<'py>(
        cls: &Bound<'py, PyType>,
        url: PyUrl,
        signer: PySigner,
        endpoint: PyUrl,
        virtual_hosted_style_request: bool,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Bound<'py, PyAny>> {
        let url = s3_endpoint_url(
            url.as_ref(),
            endpoint.as_ref(),
            virtual_hosted_style_request,
        )?;

        // Note: we pass **back** through Python so that if cls is a subclass, we instantiate the
        // subclass
        let kwargs = PyDict::new(cls.py());
        kwargs.set_item(intern!(cls.py(), "signer"), signer)?;
        kwargs.set_item(
            intern!(cls.py(), "virtual_hosted_style_request"),
            virtual_hosted_style_request,
        )?;
        kwargs.set_item(intern!(cls.py(), "client_options"), client_options)?;
        kwargs.set_item(intern!(cls.py(), "retry_config"), retry_config)?;
        Ok(cls.call((url,), Some(&kwargs))?)
    }

    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        // Ensure we never error on __eq__ by returning false if the other object is not the same
        // type
        other
            .cast::<PyRemoteSignedS3Store>()
            .map(|other| self.config.eq(&other.get().config, other.py()))
            .unwrap_or(false)
    }

    fn __getnewargs_ex__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        self.config.__getnewargs_ex__(py)
    }

    fn __repr__(&self) -> String {
        format!("RemoteSignedS3Store(\"{}\")", self.config.url)
    }

    #[getter]
    fn url(&self) -> &str {
        &self.config.url
    }

    #[getter]
    fn bucket(&self) -> &str {
        &self.config.bucket
    }

    #[getter]
    fn prefix(&self) -> Option<&str> {
        self.config.prefix.as_ref().map(Path::as_ref)
    }

    #[getter]
    fn signer(&self) -> PySigner {
        self.config.signer.clone()
    }

    #[getter]
    fn virtual_hosted_style_request(&self) -> bool {
        self.config.virtual_hosted_style_request
    }

    #[getter]
    fn client_options(&self) -> Option<PyClientOptions> {
        self.config.client_options.clone()
    }

    #[getter]
    fn retry_config(&self) -> Option<PyRetryConfig> {
        self.config.retry_config.clone()
    }
}
