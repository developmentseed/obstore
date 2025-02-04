use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::ObjectStoreScheme;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyString, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};
use url::Url;

use crate::client::PyClientOptions;
use crate::config::PyConfigValue;
use crate::error::{GenericError, ParseUrlError, PyObjectStoreError, PyObjectStoreResult};
use crate::path::PyPath;
use crate::retry::PyRetryConfig;
use crate::{MaybePrefixedStore, PyUrl};

struct AzureConfig {
    prefix: Option<PyPath>,
    config: PyAzureConfig,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
}

impl AzureConfig {
    fn container_name(&self) -> &str {
        self.config
            .0
            .get(&PyAzureConfigKey(AzureConfigKey::ContainerName))
            .expect("Container should always exist in the config")
            .as_ref()
    }

    fn __getnewargs_ex__(&self, py: Python) -> PyResult<PyObject> {
        let args = PyTuple::empty(py).into_py_any(py)?;
        let kwargs = PyDict::new(py);

        if let Some(prefix) = &self.prefix {
            kwargs.set_item(intern!(py, "prefix"), prefix.as_ref().as_ref())?;
        }
        kwargs.set_item(intern!(py, "config"), self.config.clone())?;
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
    #[pyo3(signature = (container=None, *, prefix=None, config=None, client_options=None, retry_config=None, **kwargs))]
    fn new(
        container: Option<String>,
        prefix: Option<PyPath>,
        config: Option<PyAzureConfig>,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
        kwargs: Option<PyAzureConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = MicrosoftAzureBuilder::from_env();
        let mut config = config.unwrap_or_default();
        if let Some(container) = container.clone() {
            // Note: we apply the bucket to the config, not directly to the builder, so they stay
            // in sync.
            config.insert_raising_if_exists(AzureConfigKey::ContainerName, container)?;
        }
        let combined_config = combine_config_kwargs(Some(config), kwargs)?;
        builder = combined_config.clone().apply_config(builder);
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
                config: combined_config,
                client_options,
                retry_config,
            },
        })
    }

    #[classmethod]
    #[pyo3(signature = (url, *, config=None, client_options=None, retry_config=None, **kwargs))]
    pub(crate) fn from_url(
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

        let config = parse_url(config, url.as_ref())?;
        let mut builder = MicrosoftAzureBuilder::from_env().with_url(url.clone());
        let combined_config = combine_config_kwargs(Some(config), kwargs)?;
        builder = combined_config.clone().apply_config(builder);
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
        let container_name = self.config.container_name();
        if let Some(prefix) = &self.config.prefix {
            format!(
                "AzureStore(container=\"{}\", prefix=\"{}\")",
                container_name,
                prefix.as_ref()
            )
        } else {
            format!("AzureStore(container=\"{}\")", container_name)
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

impl AsRef<str> for PyAzureConfigKey {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
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

impl From<AzureConfigKey> for PyAzureConfigKey {
    fn from(value: AzureConfigKey) -> Self {
        Self(value)
    }
}

impl From<PyAzureConfigKey> for AzureConfigKey {
    fn from(value: PyAzureConfigKey) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, IntoPyObject)]
pub struct PyAzureConfig(HashMap<PyAzureConfigKey, PyConfigValue>);

// Note: we manually impl FromPyObject instead of deriving it so that we can raise an
// UnknownConfigurationKeyError instead of a `TypeError` on invalid config keys.
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
        for (key, val) in other.0.into_iter() {
            self.insert_raising_if_exists(key, val)?;
        }

        Ok(self)
    }

    fn insert_raising_if_exists(
        &mut self,
        key: impl Into<PyAzureConfigKey>,
        val: impl Into<String>,
    ) -> PyObjectStoreResult<()> {
        let key = key.into();
        let old_value = self.0.insert(key.clone(), PyConfigValue::new(val.into()));
        if old_value.is_some() {
            return Err(GenericError::new_err(format!(
                "Duplicate key {} between config and kwargs",
                key.0.as_ref()
            ))
            .into());
        }

        Ok(())
    }

    /// Insert a key only if it does not already exist.
    ///
    /// This is used for URL parsing, where any parts of the URL **do not** override any
    /// configuration keys passed manually.
    fn insert_if_not_exists(&mut self, key: impl Into<PyAzureConfigKey>, val: impl Into<String>) {
        self.0.entry(key.into()).or_insert(PyConfigValue::new(val));
    }
}

fn combine_config_kwargs(
    config: Option<PyAzureConfig>,
    kwargs: Option<PyAzureConfig>,
) -> PyObjectStoreResult<PyAzureConfig> {
    match (config, kwargs) {
        (None, None) => Ok(Default::default()),
        (Some(x), None) | (None, Some(x)) => Ok(x),
        (Some(config), Some(kwargs)) => Ok(config.merge(kwargs)?),
    }
}

/// Sets properties on this builder based on a URL
///
/// This is vendored from
/// https://github.com/apache/arrow-rs/blob/f7263e253655b2ee613be97f9d00e063444d3df5/object_store/src/azure/builder.rs#L639-L705
///
/// We do our own URL parsing so that we can keep our own config in sync with what is passed to the
/// underlying ObjectStore builder. Passing the URL on verbatim makes it hard because the URL
/// parsing only happens in `build()`. Then the config parameters we have don't include any config
/// applied from the URL.
fn parse_url(config: Option<PyAzureConfig>, parsed: &Url) -> object_store::Result<PyAzureConfig> {
    let host = parsed
        .host_str()
        .ok_or_else(|| ParseUrlError::UrlNotRecognised {
            url: parsed.as_str().to_string(),
        })?;
    let mut config = config.unwrap_or_default();

    let validate = |s: &str| match s.contains('.') {
        true => Err(ParseUrlError::UrlNotRecognised {
            url: parsed.as_str().to_string(),
        }),
        false => Ok(s.to_string()),
    };

    match parsed.scheme() {
        "az" | "adl" | "azure" => {
            config.insert_if_not_exists(AzureConfigKey::ContainerName, validate(host)?);
        }
        "abfs" | "abfss" => {
            // abfs(s) might refer to the fsspec convention abfs://<container>/<path>
            // or the convention for the hadoop driver abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>
            if parsed.username().is_empty() {
                config.insert_if_not_exists(AzureConfigKey::ContainerName, validate(host)?);
            } else if let Some(a) = host.strip_suffix(".dfs.core.windows.net") {
                config.insert_if_not_exists(
                    AzureConfigKey::ContainerName,
                    validate(parsed.username())?,
                );
                config.insert_if_not_exists(AzureConfigKey::AccountName, validate(a)?);
            } else if let Some(a) = host.strip_suffix(".dfs.fabric.microsoft.com") {
                config.insert_if_not_exists(
                    AzureConfigKey::ContainerName,
                    validate(parsed.username())?,
                );
                config.insert_if_not_exists(AzureConfigKey::AccountName, validate(a)?);
                config.insert_if_not_exists(AzureConfigKey::UseFabricEndpoint, "true");
            } else {
                return Err(ParseUrlError::UrlNotRecognised {
                    url: parsed.as_str().to_string(),
                }
                .into());
            }
        }
        "https" => match host.split_once('.') {
            Some((a, "dfs.core.windows.net")) | Some((a, "blob.core.windows.net")) => {
                config.insert_if_not_exists(AzureConfigKey::AccountName, validate(a)?);
                if let Some(container) = parsed.path_segments().unwrap().next() {
                    config
                        .insert_if_not_exists(AzureConfigKey::ContainerName, validate(container)?);
                }
            }
            Some((a, "dfs.fabric.microsoft.com")) | Some((a, "blob.fabric.microsoft.com")) => {
                config.insert_if_not_exists(AzureConfigKey::AccountName, validate(a)?);
                // Attempt to infer the container name from the URL
                // - https://onelake.dfs.fabric.microsoft.com/<workspaceGUID>/<itemGUID>/Files/test.csv
                // - https://onelake.dfs.fabric.microsoft.com/<workspace>/<item>.<itemtype>/<path>/<fileName>
                //
                // See <https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api>
                if let Some(workspace) = parsed.path_segments().unwrap().next() {
                    if !workspace.is_empty() {
                        config.insert_if_not_exists(AzureConfigKey::ContainerName, workspace);
                    }
                }
                config.insert_if_not_exists(AzureConfigKey::UseFabricEndpoint, "true");
            }
            _ => {
                return Err(ParseUrlError::UrlNotRecognised {
                    url: parsed.as_str().to_string(),
                }
                .into())
            }
        },
        scheme => {
            let scheme = scheme.into();
            return Err(ParseUrlError::UnknownUrlScheme { scheme }.into());
        }
    }

    Ok(config)
}
