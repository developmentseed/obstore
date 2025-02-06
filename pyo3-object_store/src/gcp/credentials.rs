use std::sync::Arc;

use async_trait::async_trait;
use object_store::gcp::GcpCredential;
use object_store::CredentialProvider;
use pyo3::intern;
use pyo3::prelude::*;

struct PyGcpCredential(GcpCredential);

// Extract the dict {"token": str}
impl<'py> FromPyObject<'py> for PyGcpCredential {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let bearer = ob
            .get_item(intern!(ob.py(), "token"))?
            .extract::<String>()?;
        Ok(Self(GcpCredential { bearer }))
    }
}

#[derive(Debug, FromPyObject)]
pub struct PyGcpCredentialProvider(PyObject);

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
    async fn call(&self) -> PyResult<PyGcpCredential> {
        let call_result =
            Python::with_gil(|py| self.0.call0(py)?.extract::<PyCredentialProviderResult>(py))?;
        let resolved = call_result.resolve().await?;
        Ok(resolved)
    }
}

// TODO: store expiration time and only call the external Python function as needed
#[async_trait]
impl CredentialProvider for PyGcpCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credential = self
            .call()
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "External GCP credential provider",
                source: Box::new(err),
            })?;
        Ok(Arc::new(credential.0))
    }
}
