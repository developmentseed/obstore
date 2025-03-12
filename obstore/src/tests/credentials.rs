use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_object_store::_private::PyAWSCredentialProvider;

#[pyfunction]
pub fn call_s3_credential_provider(py: Python, provider: PyAWSCredentialProvider) {
    future_into_py(py, async move {
        let x = provider.call().await.unwrap();
        x.into
    })
}
