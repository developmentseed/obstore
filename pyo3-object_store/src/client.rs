use std::collections::HashMap;
use std::str::FromStr;

use object_store::{ClientConfigKey, ClientOptions};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

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

/// A wrapper around `String` used to store values for the ClientConfig. This allows Python `True`
/// and `False` as well as `str`. A Python `True` becomes `"true"` and a Python `False` becomes
/// `"false"`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyClientConfigValue(String);

impl<'py> FromPyObject<'py> for PyClientConfigValue {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(val) = ob.extract::<bool>() {
            Ok(Self(val.to_string()))
        } else {
            Ok(Self(ob.extract()?))
        }
    }
}

/// A wrapper around `ClientOptions` that implements [`FromPyObject`].
#[derive(Debug)]
pub struct PyClientOptions(ClientOptions);

impl<'py> FromPyObject<'py> for PyClientOptions {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_input = ob.extract::<HashMap<PyClientConfigKey, String>>()?;
        let mut options = ClientOptions::new();
        for (key, value) in py_input.into_iter() {
            options = options.with_config(key.0, value);
        }
        Ok(Self(options))
    }
}

impl From<PyClientOptions> for ClientOptions {
    fn from(value: PyClientOptions) -> Self {
        value.0
    }
}
