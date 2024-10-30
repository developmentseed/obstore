use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyType;

use crate::client::PyClientOptions;
use crate::error::{PyObjectStoreError, PyObjectStoreResult};
use crate::retry::PyRetryConfig;

/// A Python-facing wrapper around a [`MicrosoftAzure`].
#[pyclass(name = "AzureStore")]
pub struct PyAzureStore(Arc<MicrosoftAzure>);

impl AsRef<Arc<MicrosoftAzure>> for PyAzureStore {
    fn as_ref(&self) -> &Arc<MicrosoftAzure> {
        &self.0
    }
}

impl PyAzureStore {
    /// Consume self and return the underlying [`MicrosoftAzure`].
    pub fn into_inner(self) -> Arc<MicrosoftAzure> {
        self.0
    }
}

#[pymethods]
impl PyAzureStore {
    // Create from env variables
    #[classmethod]
    #[pyo3(signature = (container, *, config=None, client_options=None, retry_config=None))]
    fn from_env(
        _cls: &Bound<PyType>,
        container: String,
        config: Option<HashMap<PyAzureConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(container);
        if let Some(config) = config {
            for (key, value) in config.into_iter() {
                builder = builder.with_config(key.0, value);
            }
        }
        if let Some(client_options) = client_options {
            builder = builder.with_client_options(client_options.into())
        }
        if let Some(retry_config) = retry_config {
            builder = builder.with_retry(retry_config.into())
        }
        Ok(Self(Arc::new(builder.build()?)))
    }

    #[classmethod]
    #[pyo3(signature = (url, *, config=None, client_options=None, retry_config=None))]
    fn from_url(
        _cls: &Bound<PyType>,
        url: &str,
        config: Option<HashMap<PyAzureConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = MicrosoftAzureBuilder::from_env().with_url(url);
        if let Some(config) = config {
            for (key, value) in config.into_iter() {
                builder = builder.with_config(key.0, value);
            }
        }
        if let Some(client_options) = client_options {
            builder = builder.with_client_options(client_options.into())
        }
        if let Some(retry_config) = retry_config {
            builder = builder.with_retry(retry_config.into())
        }
        Ok(Self(Arc::new(builder.build()?)))
    }

    fn __repr__(&self) -> String {
        let repr = self.0.to_string();
        repr.replacen("MicrosoftAzure", "AzureStore", 1)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyAzureConfigKey(AzureConfigKey);

impl<'py> FromPyObject<'py> for PyAzureConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = AzureConfigKey::from_str(&s).map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}
