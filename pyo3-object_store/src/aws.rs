use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use object_store::ObjectStoreScheme;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyString, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::client::PyClientOptions;
use crate::config::PyConfigValue;
use crate::error::{ObstoreError, PyObjectStoreError, PyObjectStoreResult};
use crate::path::PyPath;
use crate::prefix::MaybePrefixedStore;
use crate::retry::PyRetryConfig;
use crate::PyUrl;

struct S3Config {
    prefix: Option<Path>,
    bucket: Option<String>,
    config: Option<PyAmazonS3Config>,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
}

impl S3Config {
    fn pickle_get_new_args<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let args =
            PyTuple::new(py, vec![self.bucket.clone().into_pyobject(py)?])?.into_py_any(py)?;
        let kwargs = PyDict::new(py);

        if let Some(prefix) = &self.prefix {
            kwargs.set_item(intern!(py, "prefix"), PyString::new(py, prefix.as_ref()))?;
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

        Ok(PyTuple::new(py, [args, kwargs.into_py_any(py)?])?)
    }
}

/// A Python-facing wrapper around an [`AmazonS3`].
#[pyclass(name = "S3Store", module = "obstore.store", frozen)]
pub struct PyS3Store {
    store: Arc<MaybePrefixedStore<AmazonS3>>,
    /// A config used for pickling. This must stay in sync with the underlying store's config.
    config: S3Config,
}

impl AsRef<Arc<MaybePrefixedStore<AmazonS3>>> for PyS3Store {
    fn as_ref(&self) -> &Arc<MaybePrefixedStore<AmazonS3>> {
        &self.store
    }
}

impl PyS3Store {
    fn new(
        mut builder: AmazonS3Builder,
        bucket: Option<String>,
        prefix: Option<PyPath>,
        config: Option<PyAmazonS3Config>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAmazonS3Config>,
    ) -> PyObjectStoreResult<Self> {
        if let Some(bucket) = bucket.clone() {
            builder = builder.with_bucket_name(bucket);
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
        let prefix = prefix.map(|x| x.into_inner());
        Ok(Self {
            store: Arc::new(MaybePrefixedStore::new(builder.build()?, prefix.clone())),
            config: S3Config {
                prefix,
                bucket,
                config: combined_config,
                client_options,
                retry_config,
            },
        })
    }

    /// Consume self and return the underlying [`AmazonS3`].
    pub fn into_inner(self) -> Arc<MaybePrefixedStore<AmazonS3>> {
        self.store
    }
}

#[pymethods]
impl PyS3Store {
    fn __getnewargs_ex__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        self.config.pickle_get_new_args(py)
    }

    // Create from parameters
    #[new]
    #[pyo3(signature = (bucket=None, *, prefix=None, config=None, client_options=None, retry_config=None, **kwargs))]
    fn new_py(
        bucket: Option<String>,
        prefix: Option<PyPath>,
        config: Option<PyAmazonS3Config>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAmazonS3Config>,
    ) -> PyObjectStoreResult<Self> {
        Self::new(
            AmazonS3Builder::from_env(),
            bucket,
            prefix,
            config,
            client_options,
            retry_config,
            kwargs,
        )
    }

    // Create from an existing boto3.Session or botocore.session.Session object
    // https://stackoverflow.com/a/36291428
    #[classmethod]
    #[pyo3(signature = (session, bucket=None, *, prefix=None, config=None, client_options=None, retry_config=None, **kwargs))]
    fn from_session(
        _cls: &Bound<PyType>,
        py: Python,
        session: &Bound<PyAny>,
        bucket: Option<String>,
        prefix: Option<PyPath>,
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
        let mut builder = AmazonS3Builder::from_env();
        if let Some(bucket) = bucket.clone() {
            builder = builder.with_bucket_name(bucket);
        }
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
        Self::new(
            builder,
            bucket,
            prefix,
            config,
            client_options,
            retry_config,
            kwargs,
        )
    }

    #[classmethod]
    #[pyo3(signature = (url, *, config=None, client_options=None, retry_config=None, **kwargs))]
    fn from_url(
        _cls: &Bound<PyType>,
        url: PyUrl,
        config: Option<PyAmazonS3Config>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAmazonS3Config>,
    ) -> PyObjectStoreResult<Self> {
        // We manually parse the URL to find the prefix because `with_url` does not apply the
        // prefix.
        let (_, prefix) =
            ObjectStoreScheme::parse(url.as_ref()).map_err(object_store::Error::from)?;
        let prefix = if prefix.parts().count() == 0 {
            Some(prefix.into())
        } else {
            None
        };
        Self::new(
            AmazonS3Builder::from_env().with_url(url),
            None,
            prefix,
            config,
            client_options,
            retry_config,
            kwargs,
        )
    }

    fn __repr__(&self) -> String {
        let repr = self.store.to_string();
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

impl<'py> IntoPyObject<'py> for PyAmazonS3ConfigKey {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, self.0.as_ref()))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, FromPyObject, IntoPyObject)]
pub struct PyAmazonS3Config(HashMap<PyAmazonS3ConfigKey, PyConfigValue>);

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
