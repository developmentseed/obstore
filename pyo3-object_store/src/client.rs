use std::collections::HashMap;

use object_store::{ClientConfigKey, ClientOptions};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyString;

use crate::config::PyConfigValue;
use crate::error::PyObjectStoreError;

/// A wrapper around `ClientConfigKey` that implements [`FromPyObject`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PyClientConfigKey(ClientConfigKey);

impl<'py> FromPyObject<'py> for PyClientConfigKey {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        let key = s.parse().map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(Self(key))
    }
}

impl<'py> IntoPyObject<'py> for PyClientConfigKey {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, self.0.as_ref()))
    }
}

impl<'py> IntoPyObject<'py> for &PyClientConfigKey {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, self.0.as_ref()))
    }
}

/// A wrapper around `ClientOptions` that implements [`FromPyObject`].
#[derive(Clone, Debug, FromPyObject, IntoPyObject, IntoPyObjectRef, PartialEq)]
pub struct PyClientOptions(HashMap<PyClientConfigKey, PyConfigValue>);

impl From<PyClientOptions> for ClientOptions {
    fn from(value: PyClientOptions) -> Self {
        let mut options = ClientOptions::new();
        for (key, value) in value.0.into_iter() {
            options = options.with_config(key.0, value.0);
        }
        options
    }
}
