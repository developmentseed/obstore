use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::ObjectStoreScheme;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyString, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::client::PyClientOptions;
use crate::config::PyConfigValue;
use crate::error::{ObstoreError, PyObjectStoreError, PyObjectStoreResult};
use crate::path::PyPath;
use crate::retry::PyRetryConfig;
use crate::{MaybePrefixedStore, PyUrl};

struct AzureConfig {
    container: Option<String>,
    // Note: we need to persist the URL passed in via from_url because object_store defers the URL
    // parsing until its `build` method, and then we have no way to persist the state of its parsed
    // components.
    url: Option<PyUrl>,
    prefix: Option<PyPath>,
    config: Option<PyAzureConfig>,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
}

impl AzureConfig {
    fn __getnewargs_ex__(&self, py: Python) -> PyResult<PyObject> {
        let args =
            PyTuple::new(py, vec![self.container.clone().into_pyobject(py)?])?.into_py_any(py)?;
        let kwargs = PyDict::new(py);

        if let Some(prefix) = &self.prefix {
            kwargs.set_item(intern!(py, "prefix"), prefix.as_ref().as_ref())?;
        }
        if let Some(url) = &self.url {
            kwargs.set_item(intern!(py, "url"), url.as_ref().as_str())?;
        }
        if let Some(config) = &self.config {
            kwargs.set_item(intern!(py, "config"), config.clone())?;
        }
        if let Some(client_options) = &self.client_options {
            kwargs.set_item(intern!(py, "client_options"), client_options.clone())?;
        }
        if let Some(retry_config) = &self.retry_config {
            kwargs.set_item(intern!(py, "retry_config"), retry_config.clone())?;
        }

        PyTuple::new(py, [args, kwargs.into_py_any(py)?])?.into_py_any(py)
    }
}

/// A Python-facing wrapper around a [`MicrosoftAzure`].
#[pyclass(name = "AzureStore", module = "obstore.store", frozen)]
pub struct PyAzureStore {
    store: Arc<MaybePrefixedStore<MicrosoftAzure>>,
    /// A config used for pickling. This must stay in sync with the underlying store's config.
    config: AzureConfig,
}

impl AsRef<Arc<MaybePrefixedStore<MicrosoftAzure>>> for PyAzureStore {
    fn as_ref(&self) -> &Arc<MaybePrefixedStore<MicrosoftAzure>> {
        &self.store
    }
}

impl PyAzureStore {
    /// Consume self and return the underlying [`MicrosoftAzure`].
    pub fn into_inner(self) -> Arc<MaybePrefixedStore<MicrosoftAzure>> {
        self.store
    }
}

#[pymethods]
impl PyAzureStore {
    // Create from parameters
    #[new]
    #[pyo3(signature = (container=None, *, prefix=None, config=None, client_options=None, retry_config=None, url=None, **kwargs))]
    fn new(
        container: Option<String>,
        prefix: Option<PyPath>,
        config: Option<PyAzureConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        // Note: URL is undocumented in the type hint as it's only used for pickle support.
        url: Option<PyUrl>,
        kwargs: Option<PyAzureConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = MicrosoftAzureBuilder::from_env();
        if let Some(container) = container.clone() {
            builder = builder.with_container_name(container);
        }
        if let Some(url) = url.clone() {
            builder = builder.with_url(url);
        }
        let combined_config = combine_config_kwargs(config, kwargs)?;
        if let Some(config_kwargs) = combined_config.clone() {
            builder = config_kwargs.apply_config(builder);
        }
        if let Some(client_options) = client_options.clone() {
            builder = builder.with_client_options(client_options.into())
        }
        if let Some(retry_config) = retry_config.clone() {
            builder = builder.with_retry(retry_config.into())
        }
        Ok(Self {
            store: Arc::new(MaybePrefixedStore::new(builder.build()?, prefix.clone())),
            config: AzureConfig {
                prefix,
                url,
                container,
                config: combined_config,
                client_options,
                retry_config,
            },
        })
    }

    #[classmethod]
    #[pyo3(signature = (url, *, config=None, client_options=None, retry_config=None, **kwargs))]
    fn from_url(
        _cls: &Bound<PyType>,
        url: PyUrl,
        config: Option<PyAzureConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAzureConfig>,
    ) -> PyObjectStoreResult<Self> {
        // We manually parse the URL to find the prefix because `with_url` does not apply the
        // prefix.
        let (_, prefix) =
            ObjectStoreScheme::parse(url.as_ref()).map_err(object_store::Error::from)?;
        let prefix = if prefix.parts().count() != 0 {
            Some(prefix.into())
        } else {
            None
        };

        let mut builder = MicrosoftAzureBuilder::from_env().with_url(url.clone());
        let combined_config = combine_config_kwargs(config, kwargs)?;
        if let Some(config_kwargs) = combined_config.clone() {
            builder = config_kwargs.apply_config(builder);
        }
        if let Some(client_options) = client_options.clone() {
            builder = builder.with_client_options(client_options.into())
        }
        if let Some(retry_config) = retry_config.clone() {
            builder = builder.with_retry(retry_config.into())
        }
        Ok(Self {
            store: Arc::new(MaybePrefixedStore::new(builder.build()?, prefix.clone())),
            config: AzureConfig {
                prefix,
                url: Some(url),
                container: None,
                config: combined_config,
                client_options,
                retry_config,
            },
        })
        // Ok(Self(Arc::new(builder.build()?)))
    }

    fn __getnewargs_ex__(&self, py: Python) -> PyResult<PyObject> {
        self.config.__getnewargs_ex__(py)
    }

    fn __repr__(&self) -> String {
        if let Some(container) = &self.config.container {
            if let Some(prefix) = &self.config.prefix {
                format!(
                    "AzureStore(container=\"{}\", prefix=\"{}\")",
                    container,
                    prefix.as_ref()
                )
            } else {
                format!("AzureStore(container=\"{}\")", container)
            }
        } else if let Some(url) = &self.config.url {
            format!("AzureStore(url=\"{}\")", url.as_ref())
        } else {
            "AzureStore".to_string()
        }
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

impl<'py> IntoPyObject<'py> for PyAzureConfigKey {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, self.0.as_ref()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, FromPyObject, IntoPyObject)]
pub struct PyAzureConfig(HashMap<PyAzureConfigKey, PyConfigValue>);

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
