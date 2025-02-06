use std::sync::Arc;

use async_trait::async_trait;
use object_store::azure::{AzureAccessKey, AzureCredential};
use object_store::CredentialProvider;
use percent_encoding::percent_decode_str;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

use crate::azure::error::Error;
use crate::PyObjectStoreResult;

struct PyAzureAccessKey(AzureAccessKey);

// Extract the dict {"access_key": str}
impl<'py> FromPyObject<'py> for PyAzureAccessKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob
            .get_item(intern!(ob.py(), "access_key"))?
            .extract::<PyBackedStr>()?;
        let key =
            AzureAccessKey::try_new(&s).map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(key))
    }
}

struct PyAzureSASToken(Vec<(String, String)>);

impl PyAzureSASToken {
    fn from_str(sas: &str) -> PyObjectStoreResult<Self> {
        Ok(Self(split_sas(sas)?))
    }
}

// Extract the dict {"sas_token": str | list[tuple[str, str]]}
impl<'py> FromPyObject<'py> for PyAzureSASToken {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_sas_token = ob.get_item(intern!(ob.py(), "sas_token"))?;
        if let Ok(sas_token_str) = py_sas_token.extract::<PyBackedStr>() {
            Ok(Self::from_str(&sas_token_str)?)
        } else if let Ok(sas_token_list) = py_sas_token.extract::<Vec<(String, String)>>() {
            Ok(Self(sas_token_list))
        } else {
            Err(PyTypeError::new_err(
                "Expected a string or list[tuple[str, str]]",
            ))
        }
    }
}

struct PyBearerToken(String);

// Extract the dict {"token": str}
impl<'py> FromPyObject<'py> for PyBearerToken {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob
            .get_item(intern!(ob.py(), "token"))?
            .extract::<String>()?;
        Ok(Self(s))
    }
}

#[derive(FromPyObject)]
enum PyAzureCredential {
    AccessKey(PyAzureAccessKey),
    SASToken(PyAzureSASToken),
    BearerToken(PyBearerToken),
}

impl From<PyAzureCredential> for AzureCredential {
    fn from(value: PyAzureCredential) -> Self {
        match value {
            PyAzureCredential::AccessKey(key) => Self::AccessKey(key.0),
            PyAzureCredential::SASToken(token) => Self::SASToken(token.0),
            PyAzureCredential::BearerToken(token) => Self::BearerToken(token.0),
        }
    }
}

// Vendored from upstream
// https://github.com/apache/arrow-rs/blob/92cfd99e9ab4a6c54500ec65252027b9edf1ee55/object_store/src/azure/builder.rs#L1055-L1072
fn split_sas(sas: &str) -> Result<Vec<(String, String)>, object_store::Error> {
    let sas = percent_decode_str(sas)
        .decode_utf8()
        .map_err(|source| Error::DecodeSasKey { source })?;
    let kv_str_pairs = sas
        .trim_start_matches('?')
        .split('&')
        .filter(|s| !s.chars().all(char::is_whitespace));
    let mut pairs = Vec::new();
    for kv_pair_str in kv_str_pairs {
        let (k, v) = kv_pair_str
            .trim()
            .split_once('=')
            .ok_or(Error::MissingSasComponent {})?;
        pairs.push((k.into(), v.into()))
    }
    Ok(pairs)
}

#[derive(Debug, FromPyObject)]
pub struct PyAzureCredentialProvider(PyObject);

enum PyCredentialProviderResult {
    Async(PyObject),
    Sync(PyAzureCredential),
}

impl PyCredentialProviderResult {
    async fn resolve(self) -> PyResult<PyAzureCredential> {
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

impl PyAzureCredentialProvider {
    async fn call(&self) -> PyResult<PyAzureCredential> {
        let call_result =
            Python::with_gil(|py| self.0.call0(py)?.extract::<PyCredentialProviderResult>(py))?;
        let resolved = call_result.resolve().await?;
        Ok(resolved)
    }
}

// TODO: store expiration time and only call the external Python function as needed
#[async_trait]
impl CredentialProvider for PyAzureCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credential = self
            .call()
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "External GCP credential provider",
                source: Box::new(err),
            })?;
        Ok(Arc::new(credential.into()))
    }
}
