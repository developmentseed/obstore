use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder, GoogleConfigKey};
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

struct GCSConfig {
    bucket: Option<String>,
    // Note: we need to persist the URL passed in via from_url because object_store defers the URL
    // parsing until its `build` method, and then we have no way to persist the state of its parsed
    // components.
    url: Option<PyUrl>,
    prefix: Option<PyPath>,
    config: Option<PyGoogleConfig>,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
}

impl GCSConfig {
    fn __getnewargs_ex__(&self, py: Python) -> PyResult<PyObject> {
        let args =
            PyTuple::new(py, vec![self.bucket.clone().into_pyobject(py)?])?.into_py_any(py)?;
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

/// A Python-facing wrapper around a [`GoogleCloudStorage`].
#[pyclass(name = "GCSStore", module = "obstore.store", frozen)]
pub struct PyGCSStore {
    store: Arc<MaybePrefixedStore<GoogleCloudStorage>>,
    /// A config used for pickling. This must stay in sync with the underlying store's config.
    config: GCSConfig,
}

impl AsRef<Arc<MaybePrefixedStore<GoogleCloudStorage>>> for PyGCSStore {
    fn as_ref(&self) -> &Arc<MaybePrefixedStore<GoogleCloudStorage>> {
        &self.store
    }
}

impl PyGCSStore {
    /// Consume self and return the underlying [`GoogleCloudStorage`].
    pub fn into_inner(self) -> Arc<MaybePrefixedStore<GoogleCloudStorage>> {
        self.store
    }
}

#[pymethods]
impl PyGCSStore {
    // Create from parameters
    #[new]
    #[pyo3(signature = (bucket=None, *, prefix=None, config=None, client_options=None, retry_config=None, url=None, **kwargs))]
    fn new(
        bucket: Option<String>,
        prefix: Option<PyPath>,
        config: Option<PyGoogleConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        // Note: URL is undocumented in the type hint as it's only used for pickle support.
        url: Option<PyUrl>,
        kwargs: Option<PyGoogleConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = GoogleCloudStorageBuilder::from_env();
        if let Some(bucket) = bucket.clone() {
            builder = builder.with_bucket_name(bucket);
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
            config: GCSConfig {
                prefix,
                url,
                bucket,
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
        config: Option<PyGoogleConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyGoogleConfig>,
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
        let mut builder = GoogleCloudStorageBuilder::from_env().with_url(url.clone());
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
            config: GCSConfig {
                prefix,
                url: Some(url),
                bucket: None,
                config: combined_config,
                client_options,
                retry_config,
            },
        })
    }

    fn __getnewargs_ex__(&self, py: Python) -> PyResult<PyObject> {
        self.config.__getnewargs_ex__(py)
    }

    fn __repr__(&self) -> String {
        if let Some(bucket) = &self.config.bucket {
            if let Some(prefix) = &self.config.prefix {
                format!(
                    "GCSStore(bucket=\"{}\", prefix=\"{}\")",
                    bucket,
                    prefix.as_ref()
                )
            } else {
                format!("GCSStore(bucket=\"{}\")", bucket)
            }
        } else if let Some(url) = &self.config.url {
            format!("GCSStore(url=\"{}\")", url.as_ref())
        } else {
            "GCSStore".to_string()
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PyGoogleConfigKey(GoogleConfigKey);

impl<'py> FromPyObject<'py> for PyGoogleConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = GoogleConfigKey::from_str(&s).map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}

impl<'py> IntoPyObject<'py> for PyGoogleConfigKey {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, self.0.as_ref()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, FromPyObject, IntoPyObject)]
pub struct PyGoogleConfig(HashMap<PyGoogleConfigKey, PyConfigValue>);

impl PyGoogleConfig {
    fn apply_config(self, mut builder: GoogleCloudStorageBuilder) -> GoogleCloudStorageBuilder {
        for (key, value) in self.0.into_iter() {
            builder = builder.with_config(key.0, value.0);
        }
        builder
    }

    fn merge(mut self, other: PyGoogleConfig) -> PyObjectStoreResult<PyGoogleConfig> {
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
    config: Option<PyGoogleConfig>,
    kwargs: Option<PyGoogleConfig>,
) -> PyObjectStoreResult<Option<PyGoogleConfig>> {
    match (config, kwargs) {
        (None, None) => Ok(None),
        (Some(x), None) | (None, Some(x)) => Ok(Some(x)),
        (Some(config), Some(kwargs)) => Ok(Some(config.merge(kwargs)?)),
    }
}
