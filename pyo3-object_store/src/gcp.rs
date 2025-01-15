use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder, GoogleConfigKey};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyType;

use crate::client::PyClientOptions;
use crate::error::{PyObjectStoreError, PyObjectStoreResult};
use crate::retry::PyRetryConfig;

/// A Python-facing wrapper around a [`GoogleCloudStorage`].
#[pyclass(name = "GCSStore", frozen)]
pub struct PyGCSStore(Arc<GoogleCloudStorage>);

impl AsRef<Arc<GoogleCloudStorage>> for PyGCSStore {
    fn as_ref(&self) -> &Arc<GoogleCloudStorage> {
        &self.0
    }
}

impl PyGCSStore {
    /// Consume self and return the underlying [`GoogleCloudStorage`].
    pub fn into_inner(self) -> Arc<GoogleCloudStorage> {
        self.0
    }
}

#[pymethods]
impl PyGCSStore {
    // Create from parameters
    #[new]
    #[pyo3(signature = (bucket, *, config=None, client_options=None, retry_config=None))]
    fn new(
        bucket: String,
        config: Option<HashMap<PyGoogleConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
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

    // Create from env variables
    #[classmethod]
    #[pyo3(signature = (bucket, *, config=None, client_options=None, retry_config=None))]
    fn from_env(
        _cls: &Bound<PyType>,
        bucket: String,
        config: Option<HashMap<PyGoogleConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
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
        config: Option<HashMap<PyGoogleConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = GoogleCloudStorageBuilder::from_env().with_url(url);
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
        repr.replacen("GoogleCloudStorage", "GCSStore", 1)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyGoogleConfigKey(GoogleConfigKey);

impl<'py> FromPyObject<'py> for PyGoogleConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = GoogleConfigKey::from_str(&s).map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}
