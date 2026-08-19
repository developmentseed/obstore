//! The Python boundary: handing each request to a user callback to be signed.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use http::{HeaderMap, HeaderName, HeaderValue, Method};
use object_store::Result;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use url::Url;

use super::error_for;

/// A Python callable that signs a single S3 request.
pub(super) struct PySigner(pub(super) Py<PyAny>);

impl Clone for PySigner {
    fn clone(&self) -> Self {
        Python::attach(|py| Self(self.0.clone_ref(py)))
    }
}

impl Debug for PySigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PySigner(..)")
    }
}

impl<'py> FromPyObject<'_, 'py> for PySigner {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if !obj.hasattr(intern!(obj.py(), "__call__"))? {
            return Err(PyTypeError::new_err("Expected callable object for signer."));
        }
        Ok(Self(obj.as_unbound().clone_ref(obj.py())))
    }
}

impl<'py> IntoPyObject<'py> for PySigner {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.0.into_bound(py))
    }
}

/// A signer callback may be a plain function or a coroutine function.
enum PySignerResult {
    Async(Py<PyAny>),
    Sync((String, HashMap<String, String>)),
}

impl PySignerResult {
    async fn resolve(self) -> PyResult<(String, HashMap<String, String>)> {
        match self {
            Self::Sync(result) => Ok(result),
            Self::Async(coroutine) => {
                let future = Python::attach(|py| {
                    pyo3_async_runtimes::tokio::into_future(coroutine.bind(py).clone())
                })?;
                let result = future.await?;
                Python::attach(|py| result.extract(py))
            }
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for PySignerResult {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if obj.hasattr(intern!(obj.py(), "__await__"))? {
            Ok(Self::Async(obj.as_unbound().clone_ref(obj.py())))
        } else {
            Ok(Self::Sync(obj.extract()?))
        }
    }
}

impl PySigner {
    /// Hand `(method, uri, headers)` to the callback and return the signed `(uri, headers)`.
    ///
    /// The returned headers *replace* the headers passed in, rather than being merged into them.
    /// This is what the S3 remote-signing contract specifies: a signer computes a signature over a
    /// specific set of headers, so silently adding to or keeping headers it did not return would
    /// invalidate that signature.
    pub(super) async fn sign(
        &self,
        method: &Method,
        uri: &Url,
        headers: &HeaderMap,
    ) -> Result<(Url, HeaderMap)> {
        let headers: HashMap<String, String> = headers
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.as_str().to_owned(),
                    value
                        .to_str()
                        .map_err(|error| error_for("invalid request header", error))?
                        .to_owned(),
                ))
            })
            .collect::<Result<_>>()?;
        let result = Python::attach(|py| {
            self.0
                .call1(py, (method.as_str(), uri.as_str(), headers))?
                .extract::<PySignerResult>(py)
        })
        .map_err(|error| error_for("signer callback failed", error))?
        .resolve()
        .await
        .map_err(|error| error_for("signer callback failed", error))?;
        let uri = Url::parse(&result.0)
            .map_err(|error| error_for("signer returned invalid URI", error))?;
        let headers = result
            .1
            .into_iter()
            .map(|(name, value)| {
                Ok((
                    HeaderName::from_bytes(name.as_bytes())
                        .map_err(|error| error_for("signer returned invalid header name", error))?,
                    HeaderValue::from_str(&value).map_err(|error| {
                        error_for("signer returned invalid header value", error)
                    })?,
                ))
            })
            .collect::<Result<HeaderMap>>()?;
        Ok((uri, headers))
    }
}
