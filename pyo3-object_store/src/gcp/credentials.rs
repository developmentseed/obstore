use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use object_store::gcp::GcpCredential;
use object_store::CredentialProvider;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;

use crate::credentials::{TemporaryToken, TokenCache};

/// Ref https://github.com/apache/arrow-rs/pull/6638
const DEFAULT_GCP_MIN_TTL: TimeDelta = TimeDelta::minutes(4);

/// A wrapper around a [GcpCredential] that includes an optional expiry timestamp.
struct PyGcpCredential {
    credential: GcpCredential,
    expires_at: Option<DateTime<Utc>>,
}

impl<'py> FromPyObject<'py> for PyGcpCredential {
    /// Converts from a Python dictionary of the form
    ///
    /// ```py
    /// class GCSCredential(TypedDict):
    ///     token: str
    ///     expires_at: datetime | None
    /// ```
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let bearer = ob
            .get_item(intern!(ob.py(), "token"))?
            .extract::<String>()?;
        let credential = GcpCredential { bearer };
        let expires_at = ob.get_item(intern!(ob.py(), "expires_at"))?.extract()?;
        Ok(Self {
            credential,
            expires_at,
        })
    }
}

// TODO: don't use a cache for static credentials where `expires_at` is `None`
// (so you don't need to access a mutex)
#[derive(Debug)]
pub struct PyGcpCredentialProvider {
    /// The provided user callback to manage credential refresh
    user_callback: PyObject,
    /// The provided user callback to manage credential refresh
    cache: TokenCache<Arc<GcpCredential>>,
}

impl Clone for PyGcpCredentialProvider {
    fn clone(&self) -> Self {
        let cloned_callback = Python::with_gil(|py| self.user_callback.clone_ref(py));
        Self {
            user_callback: cloned_callback,
            cache: self.cache.clone(),
        }
    }
}

impl<'py> FromPyObject<'py> for PyGcpCredentialProvider {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if !ob.hasattr(intern!(ob.py(), "__call__"))? {
            return Err(PyTypeError::new_err(
                "Expected callable object for credential_provider.",
            ));
        }
        let min_ttl =
            if let Ok(refresh_threshold) = ob.getattr(intern!(ob.py(), "refresh_threshold")) {
                refresh_threshold.extract()?
            } else {
                DEFAULT_GCP_MIN_TTL
            };
        let cache = TokenCache::default().with_min_ttl(min_ttl);
        Ok(Self {
            user_callback: ob.clone().unbind(),
            cache,
        })
    }
}

impl<'py> IntoPyObject<'py> for PyGcpCredentialProvider {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.user_callback.bind(py).clone())
    }
}

/// Note: This is copied across providers at the moment
enum PyCredentialProviderResult {
    Async(PyObject),
    Sync(PyGcpCredential),
}

impl PyCredentialProviderResult {
    async fn resolve(self) -> PyResult<PyGcpCredential> {
        match self {
            Self::Sync(credentials) => Ok(credentials),
            Self::Async(coroutine) => {
                let future = Python::with_gil(|py| {
                    pyo3_async_runtimes::tokio::into_future(coroutine.bind(py).clone())
                })?;
                let result = future.await?;
                Python::with_gil(|py| result.extract(py))
            }
        }
    }
}

impl<'py> FromPyObject<'py> for PyCredentialProviderResult {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(credentials) = ob.extract() {
            Ok(Self::Sync(credentials))
        } else {
            Ok(Self::Async(ob.clone().unbind()))
        }
    }
}

impl PyGcpCredentialProvider {
    /// Call the user-provided callback and extract to a token.
    ///
    /// This is separate from `fetch_token` below so that it can return a `PyResult`.
    async fn call(&self) -> PyResult<PyGcpCredential> {
        let call_result = Python::with_gil(|py| {
            self.user_callback
                .call0(py)?
                .extract::<PyCredentialProviderResult>(py)
        })?;
        call_result.resolve().await
    }

    /// Call the user-provided callback
    async fn fetch_token(&self) -> object_store::Result<TemporaryToken<Arc<GcpCredential>>> {
        let credential = self
            .call()
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "External GCP credential provider",
                source: Box::new(err),
            })?;

        Ok(TemporaryToken {
            token: Arc::new(credential.credential),
            expiry: credential.expires_at,
        })
    }
}

#[async_trait]
impl CredentialProvider for PyGcpCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        self.cache.get_or_insert_with(|| self.fetch_token()).await
    }
}
