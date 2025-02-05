use std::sync::Arc;

use object_store::http::{HttpBuilder, HttpStore};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::error::PyObjectStoreResult;
use crate::retry::PyRetryConfig;
use crate::{PyClientOptions, PyUrl};

struct HTTPConfig {
    url: PyUrl,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
}

impl HTTPConfig {
    fn __getnewargs_ex__(&self, py: Python) -> PyResult<PyObject> {
        let args = PyTuple::new(py, vec![self.url.clone().into_pyobject(py)?])?.into_py_any(py)?;
        let kwargs = PyDict::new(py);

        if let Some(client_options) = &self.client_options {
            kwargs.set_item(intern!(py, "client_options"), client_options.clone())?;
        }
        if let Some(retry_config) = &self.retry_config {
            kwargs.set_item(intern!(py, "retry_config"), retry_config.clone())?;
        }

        PyTuple::new(py, [args, kwargs.into_py_any(py)?])?.into_py_any(py)
    }
}

/// A Python-facing wrapper around a [`HttpStore`].
#[pyclass(name = "HTTPStore", module = "obstore.store", frozen)]
pub struct PyHttpStore {
    // Note: we don't need to wrap this in a MaybePrefixedStore because the HttpStore manages its
    // own prefix.
    store: Arc<HttpStore>,
    /// A config used for pickling. This must stay in sync with the underlying store's config.
    config: HTTPConfig,
}

impl AsRef<Arc<HttpStore>> for PyHttpStore {
    fn as_ref(&self) -> &Arc<HttpStore> {
        &self.store
    }
}

impl PyHttpStore {
    /// Consume self and return the underlying [`HttpStore`].
    pub fn into_inner(self) -> Arc<HttpStore> {
        self.store
    }
}

#[pymethods]
impl PyHttpStore {
    #[new]
    #[pyo3(signature = (url, *, client_options=None, retry_config=None))]
    fn new(
        url: PyUrl,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = HttpBuilder::new().with_url(url.clone());
        if let Some(client_options) = client_options.clone() {
            builder = builder.with_client_options(client_options.into())
        }
        if let Some(retry_config) = retry_config.clone() {
            builder = builder.with_retry(retry_config.into())
        }
        Ok(Self {
            store: Arc::new(builder.build()?),
            config: HTTPConfig {
                url,
                client_options,
                retry_config,
            },
        })
    }

    #[classmethod]
    #[pyo3(signature = (url, *, client_options=None, retry_config=None))]
    pub(crate) fn from_url(
        _cls: &Bound<PyType>,
        url: PyUrl,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        Self::new(url, client_options, retry_config)
    }

    fn __getnewargs_ex__(&self, py: Python) -> PyResult<PyObject> {
        self.config.__getnewargs_ex__(py)
    }

    fn __repr__(&self) -> String {
        format!("HTTPStore(\"{}\")", &self.config.url.as_ref())
    }

    #[getter]
    fn url(&self) -> &PyUrl {
        &self.config.url
    }

    #[getter]
    fn client_options(&self) -> Option<PyClientOptions> {
        self.config.client_options.clone()
    }

    #[getter]
    fn retry_config(&self) -> Option<PyRetryConfig> {
        self.config.retry_config.clone()
    }
}
