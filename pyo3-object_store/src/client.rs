use std::collections::HashMap;
use std::str::FromStr;

use object_store::{ClientConfigKey, ClientOptions};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

use crate::config::PyConfigValue;
use crate::error::PyObjectStoreError;

/// A wrapper around `ClientConfigKey` that implements [`FromPyObject`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyClientConfigKey(ClientConfigKey);

impl<'py> FromPyObject<'py> for PyClientConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = ClientConfigKey::from_str(&s).map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}

/// A wrapper around `ClientOptions` that implements [`FromPyObject`].
#[derive(Debug)]
pub struct PyClientOptions(ClientOptions);

impl<'py> FromPyObject<'py> for PyClientOptions {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_input = ob.extract::<HashMap<PyClientConfigKey, PyConfigValue>>()?;
        let mut options = ClientOptions::new();
        for (key, value) in py_input.into_iter() {
            options = options.with_config(key.0, value.0);
        }
        Ok(Self(options))
    }
}

impl From<PyClientOptions> for ClientOptions {
    fn from(value: PyClientOptions) -> Self {
        value.0
    }
}
