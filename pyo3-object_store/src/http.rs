use std::sync::Arc;

use object_store::http::{HttpBuilder, HttpStore};
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::error::PyObjectStoreResult;
use crate::retry::PyRetryConfig;
use crate::PyClientOptions;

/// A Python-facing wrapper around a [`HttpStore`].
#[pyclass(name = "HTTPStore")]
pub struct PyHttpStore(Arc<HttpStore>);

impl AsRef<Arc<HttpStore>> for PyHttpStore {
    fn as_ref(&self) -> &Arc<HttpStore> {
        &self.0
    }
}

impl PyHttpStore {
    /// Consume self and return the underlying [`HttpStore`].
    pub fn into_inner(self) -> Arc<HttpStore> {
        self.0
    }

    fn __repr__(&self) -> String {
        self.0.to_string()
    }
}

#[pymethods]
impl PyHttpStore {
    #[classmethod]
    #[pyo3(signature = (url, *, client_options=None, retry_config=None))]
    fn from_url(
        _cls: &Bound<PyType>,
        url: &str,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let mut builder = HttpBuilder::new().with_url(url);
        if let Some(client_options) = client_options {
            builder = builder.with_client_options(client_options.into())
        }
        if let Some(retry_config) = retry_config {
            builder = builder.with_retry(retry_config.into())
        }
        Ok(Self(Arc::new(builder.build()?)))
    }
}
