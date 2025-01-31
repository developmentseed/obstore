use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStoreScheme;
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::IntoPyObjectExt;

use crate::error::ObstoreError;
use crate::retry::PyRetryConfig;
use crate::url::PyUrl;
use crate::{
    PyAzureStore, PyClientOptions, PyGCSStore, PyHttpStore, PyLocalStore, PyMemoryStore,
    PyObjectStoreResult, PyS3Store,
};

/// Simple construction of stores by url.
// Note: We don't extract the PyObject in the function signature because it's possible that
// AWS/Azure/Google config keys could overlap. And so we don't want to accidentally parse a config
// as an AWS config before knowing that the URL scheme is AWS.
#[pyfunction]
#[pyo3(signature = (url, *, config=None, client_options=None, retry_config=None, **kwargs))]
pub fn new_store(
    py: Python,
    url: PyUrl,
    config: Option<Bound<PyAny>>,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
    kwargs: Option<Bound<PyAny>>,
) -> PyObjectStoreResult<PyObject> {
    let url = url.into_inner();
    let (scheme, _) = ObjectStoreScheme::parse(&url).map_err(object_store::Error::from)?;
    match scheme {
        ObjectStoreScheme::AmazonS3 => {
            let store = PyS3Store::from_url(
                &PyType::new::<PyS3Store>(py),
                url.as_str(),
                config.map(|x| x.extract()).transpose()?,
                client_options,
                retry_config,
                kwargs.map(|x| x.extract()).transpose()?,
            )?;
            Ok(store.into_pyobject(py)?.into_py_any(py)?)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let store = PyGCSStore::from_url(
                &PyType::new::<PyGCSStore>(py),
                url.as_str(),
                config.map(|x| x.extract()).transpose()?,
                client_options,
                retry_config,
                kwargs.map(|x| x.extract()).transpose()?,
            )?;
            Ok(store.into_pyobject(py)?.into_py_any(py)?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let store = PyAzureStore::from_url(
                &PyType::new::<PyAzureStore>(py),
                url.as_str(),
                config.map(|x| x.extract()).transpose()?,
                client_options,
                retry_config,
                kwargs.map(|x| x.extract()).transpose()?,
            )?;
            Ok(store.into_pyobject(py)?.into_py_any(py)?)
        }
        ObjectStoreScheme::Http => {
            raise_if_config_passed(config, kwargs, "http")?;
            let store = PyHttpStore::from_url(
                &PyType::new::<PyHttpStore>(py),
                url.as_str(),
                client_options,
                retry_config,
            )?;
            Ok(store.into_pyobject(py)?.into_py_any(py)?)
        }
        ObjectStoreScheme::Local => {
            raise_if_config_passed(config, kwargs, "local")?;
            let store = PyLocalStore::from_url(&PyType::new::<PyLocalStore>(py), url.as_str())?;
            Ok(store.into_pyobject(py)?.into_py_any(py)?)
        }
        ObjectStoreScheme::Memory => {
            raise_if_config_passed(config, kwargs, "memory")?;
            let store: PyMemoryStore = Arc::new(InMemory::new()).into();
            Ok(store.into_pyobject(py)?.into_py_any(py)?)
        }
        scheme => {
            return Err(ObstoreError::new_err(format!("Unknown URL scheme {:?}", scheme,)).into());
        }
    }
}

fn raise_if_config_passed(
    config: Option<Bound<PyAny>>,
    kwargs: Option<Bound<PyAny>>,
    scheme: &str,
) -> PyObjectStoreResult<()> {
    if config.is_some() || kwargs.is_some() {
        return Err(ObstoreError::new_err(format!(
            "Cannot pass config or keyword parameters for scheme {:?}",
            scheme,
        ))
        .into());
    }
    Ok(())
}
