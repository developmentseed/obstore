use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyType;

use crate::client::PyClientOptions;
use crate::error::{PyObjectStoreError, PyObjectStoreResult};
use crate::retry::PyRetryConfig;

/// A Python-facing wrapper around an [`AmazonS3`].
#[pyclass(name = "S3Store", frozen)]
pub struct PyS3Store(Arc<AmazonS3>);

impl AsRef<Arc<AmazonS3>> for PyS3Store {
    fn as_ref(&self) -> &Arc<AmazonS3> {
        &self.0
    }
}

impl PyS3Store {
    /// Consume self and return the underlying [`AmazonS3`].
    pub fn into_inner(self) -> Arc<AmazonS3> {
        self.0
    }
}

#[pymethods]
impl PyS3Store {
    // Create from parameters
    #[new]
    #[pyo3(signature = (bucket, *, config=None, client_options=None, retry_config=None))]
    fn new(
        bucket: String,
        config: Option<HashMap<PyAmazonS3ConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
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
    #[pyo3(signature = (bucket=None, *, config=None, client_options=None, retry_config=None))]
    fn from_env(
        _cls: &Bound<PyType>,
        bucket: Option<String>,
        config: Option<HashMap<PyAmazonS3ConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = AmazonS3Builder::from_env();
        if let Some(bucket) = bucket {
            builder = builder.with_bucket_name(bucket);
        }
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

    // Create from an existing boto3.Session or botocore.session.Session object
    // https://stackoverflow.com/a/36291428
    #[classmethod]
    #[pyo3(signature = (session, bucket, *, config=None, client_options=None, retry_config=None))]
    fn from_session(
        _cls: &Bound<PyType>,
        py: Python,
        session: &Bound<PyAny>,
        bucket: String,
        config: Option<HashMap<PyAmazonS3ConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        // boto3.Session has a region_name attribute, but botocore.session.Session does not.
        let region = if let Ok(region) = session.getattr(intern!(py, "region_name")) {
            region.extract::<Option<String>>()?
        } else {
            None
        };

        let creds = session.call_method0(intern!(py, "get_credentials"))?;
        let frozen_creds = creds.call_method0(intern!(py, "get_frozen_credentials"))?;

        let access_key = frozen_creds
            .getattr(intern!(py, "access_key"))?
            .extract::<Option<String>>()?;
        let secret_key = frozen_creds
            .getattr(intern!(py, "secret_key"))?
            .extract::<Option<String>>()?;
        let token = frozen_creds
            .getattr(intern!(py, "token"))?
            .extract::<Option<String>>()?;

        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
        if let Some(region) = region {
            builder = builder.with_region(region);
        }
        if let Some(access_key) = access_key {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = secret_key {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(token) = token {
            builder = builder.with_token(token);
        }
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
        config: Option<HashMap<PyAmazonS3ConfigKey, String>>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = AmazonS3Builder::from_env().with_url(url);
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
        repr.replacen("AmazonS3", "S3Store", 1)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyAmazonS3ConfigKey(AmazonS3ConfigKey);

impl<'py> FromPyObject<'py> for PyAmazonS3ConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = AmazonS3ConfigKey::from_str(&s).map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}
