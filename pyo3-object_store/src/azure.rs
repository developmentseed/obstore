use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyType;

use crate::client::PyClientOptions;
use crate::config::PyConfigValue;
use crate::error::{ObstoreError, PyObjectStoreError, PyObjectStoreResult};
use crate::retry::PyRetryConfig;

/// A Python-facing wrapper around a [`MicrosoftAzure`].
#[pyclass(name = "AzureStore", frozen)]
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
    // Create from parameters
    #[new]
    #[pyo3(signature = (container, *, config=None, client_options=None, retry_config=None, **kwargs))]
    fn new(
        container: String,
        config: Option<PyAzureConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAzureConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = MicrosoftAzureBuilder::new().with_container_name(container);
        if let Some(config_kwargs) = combine_config_kwargs(config, kwargs)? {
            builder = config_kwargs.apply_config(builder);
        }
        if let Some(client_options) = client_options {
            builder = builder.with_client_options(client_options.into())
        }
        if let Some(retry_config) = retry_config {
            builder = builder.with_retry(retry_config.into())
        }
        Ok(Self(Arc::new(builder.build()?)))
    }

    // Create from env variables
    #[classmethod]
    #[pyo3(signature = (container, *, config=None, client_options=None, retry_config=None, **kwargs))]
    fn from_env(
        _cls: &Bound<PyType>,
        container: String,
        config: Option<PyAzureConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAzureConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(container);
        if let Some(config_kwargs) = combine_config_kwargs(config, kwargs)? {
            builder = config_kwargs.apply_config(builder);
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
    #[pyo3(signature = (url, *, config=None, client_options=None, retry_config=None, **kwargs))]
    fn from_url(
        _cls: &Bound<PyType>,
        url: &str,
        config: Option<PyAzureConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAzureConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = MicrosoftAzureBuilder::from_env().with_url(url);
        if let Some(config_kwargs) = combine_config_kwargs(config, kwargs)? {
            builder = config_kwargs.apply_config(builder);
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PyAzureConfigKey(AzureConfigKey);

impl<'py> FromPyObject<'py> for PyAzureConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = AzureConfigKey::from_str(&s).map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyAzureConfig(HashMap<PyAzureConfigKey, PyConfigValue>);

impl<'py> FromPyObject<'py> for PyAzureConfig {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(ob.extract()?))
    }
}

impl PyAzureConfig {
    fn apply_config(self, mut builder: MicrosoftAzureBuilder) -> MicrosoftAzureBuilder {
        for (key, value) in self.0.into_iter() {
            builder = builder.with_config(key.0, value.0);
        }
        builder
    }

    fn merge(mut self, other: PyAzureConfig) -> PyObjectStoreResult<PyAzureConfig> {
        for (k, v) in other.0.into_iter() {
            let old_value = self.0.insert(k.clone(), v);
            if old_value.is_some() {
                return Err(ObstoreError::new_err(format!(
                    "Duplicate key {} between config and kwargs",
                    k.0.as_ref()
                ))
                .into());
            }
        }

        Ok(self)
    }
}

fn combine_config_kwargs(
    config: Option<PyAzureConfig>,
    kwargs: Option<PyAzureConfig>,
) -> PyObjectStoreResult<Option<PyAzureConfig>> {
    match (config, kwargs) {
        (None, None) => Ok(None),
        (Some(x), None) | (None, Some(x)) => Ok(Some(x)),
        (Some(config), Some(kwargs)) => Ok(Some(config.merge(kwargs)?)),
    }
}
