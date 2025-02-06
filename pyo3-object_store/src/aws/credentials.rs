use std::sync::Arc;

use async_trait::async_trait;
use object_store::aws::AwsCredential;
use object_store::CredentialProvider;
use pyo3::intern;
use pyo3::prelude::*;

struct PyAwsCredential {
    credential: AwsCredential,
    // TODO: convert to timestamp
    // expiration: (),
}

impl<'py> FromPyObject<'py> for PyAwsCredential {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let key_id = ob.get_item(intern!(py, "access_key_id"))?.extract()?;
        let secret_key = ob.get_item(intern!(py, "secret_access_key"))?.extract()?;
        // TODO: check this
        let token = ob.get_item(intern!(py, "token"))?.extract()?;
        let credential = AwsCredential {
            key_id,
            secret_key,
            token,
        };
        Ok(Self {
            credential,
            // expiration: (),
        })
    }
}

// access_key_id: str
// secret_access_key: str
// token: str | None
// timeout: datetime | None

#[derive(Debug, FromPyObject)]
pub struct ExternalAWSCredentialProvider(PyObject);

enum CredentialProviderResult {
    Async(PyObject),
    Sync(PyAwsCredential),
}

impl CredentialProviderResult {
    async fn resolve(self) -> PyResult<PyAwsCredential> {
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

impl<'py> FromPyObject<'py> for CredentialProviderResult {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(credentials) = ob.extract() {
            Ok(Self::Sync(credentials))
        } else {
            Ok(Self::Async(ob.clone().unbind()))
        }
    }
}

impl ExternalAWSCredentialProvider {
    async fn call(&self) -> PyResult<PyAwsCredential> {
        let call_result =
            Python::with_gil(|py| self.0.call0(py)?.extract::<CredentialProviderResult>(py))?;
        let resolved = call_result.resolve().await?;
        Ok(resolved)
    }
}

// TODO: store expiration time and only call the external Python function as needed
#[async_trait]
impl CredentialProvider for ExternalAWSCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credential = self
            .call()
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "External AWS credential provider",
                source: Box::new(err),
            })?;
        Ok(Arc::new(credential.credential))
    }
}
