use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyType;

use crate::client::PyClientOptions;
use crate::config::PyConfigValue;
use crate::error::{ObstoreError, PyObjectStoreError, PyObjectStoreResult};
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
    #[pyo3(signature = (bucket=None, *, config=None, client_options=None, retry_config=None, **kwargs))]
    fn new(
        bucket: Option<String>,
        config: Option<PyAmazonS3Config>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAmazonS3Config>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = AmazonS3Builder::from_env();
        if let Some(bucket) = bucket {
            builder = builder.with_bucket_name(bucket);
        }
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

    // Create from an existing boto3.Session or botocore.session.Session object
    // https://stackoverflow.com/a/36291428
    #[classmethod]
    #[pyo3(signature = (session, bucket, *, config=None, client_options=None, retry_config=None, **kwargs))]
    fn from_session(
        _cls: &Bound<PyType>,
        py: Python,
        session: &Bound<PyAny>,
        bucket: String,
        config: Option<PyAmazonS3Config>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAmazonS3Config>,
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

        // We read env variables because even though boto3.Session reads env variables itself,
        // there may be more variables set than just authentication. Regardless, any variables set
        // by the environment will be overwritten below if they exist/were passed in.
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
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
        config: Option<PyAmazonS3Config>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAmazonS3Config>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = AmazonS3Builder::from_env().with_url(url);
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
        repr.replacen("AmazonS3", "S3Store", 1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PyAmazonS3ConfigKey(AmazonS3ConfigKey);

impl<'py> FromPyObject<'py> for PyAmazonS3ConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = AmazonS3ConfigKey::from_str(&s).map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyAmazonS3Config(HashMap<PyAmazonS3ConfigKey, PyConfigValue>);

impl<'py> FromPyObject<'py> for PyAmazonS3Config {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(ob.extract()?))
    }
}

impl PyAmazonS3Config {
    fn apply_config(self, mut builder: AmazonS3Builder) -> AmazonS3Builder {
        for (key, value) in self.0.into_iter() {
            builder = builder.with_config(key.0, value.0);
        }
        builder
    }

    fn merge(mut self, other: PyAmazonS3Config) -> PyObjectStoreResult<PyAmazonS3Config> {
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
    config: Option<PyAmazonS3Config>,
    kwargs: Option<PyAmazonS3Config>,
) -> PyObjectStoreResult<Option<PyAmazonS3Config>> {
    match (config, kwargs) {
        (None, None) => Ok(None),
        (Some(x), None) | (None, Some(x)) => Ok(Some(x)),
        (Some(config), Some(kwargs)) => Ok(Some(config.merge(kwargs)?)),
    }
}
