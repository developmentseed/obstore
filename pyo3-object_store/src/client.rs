use std::collections::HashMap;
use std::str::FromStr;

use http::{HeaderMap, HeaderName, HeaderValue};
use object_store::{ClientConfigKey, ClientOptions};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyDict;

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
#[pyclass(name = "ClientOptions", frozen)]
pub struct PyClientOptions(ClientOptions);

impl<'py> FromPyObject<'py> for PyClientOptions {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(options) = ob.downcast::<PyClientOptions>() {
            return Ok(Self(options.get().0.clone()));
        }

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

#[pymethods]
impl PyClientOptions {
    #[new]
    #[pyo3(signature = (*, default_headers = None, **kwargs))]
    // TODO: add kwargs
    fn py_new(
        default_headers: Option<PyHeaderMap>,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Self> {
        let mut options = ClientOptions::default();
        if let Some(default_headers) = default_headers {
            options = options.with_default_headers(default_headers.0);
        }
        if let Some(kwargs) = kwargs {
            let kwargs = kwargs.extract::<HashMap<PyClientConfigKey, String>>()?;
            for (key, value) in kwargs.into_iter() {
                options = options.with_config(key.0, value);
            }
        }

        Ok(Self(options))
    }
}

// use pyo3::prelude::*;
// use pyo3::types::PyDict;

// #[pyfunction]
// #[pyo3(signature = (**kwds))]
// fn num_kwds(kwds: Option<&Bound<'_, PyDict>>) -> usize {
//     kwds.map_or(0, |dict| dict.len())
// }

// #[pymodule]
// fn module_with_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
//     m.add_function(wrap_pyfunction!(num_kwds, m)?)
// }

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyHeaderName(HeaderName);

impl<'py> FromPyObject<'py> for PyHeaderName {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        // TODO: check that this works on both str and bytes input
        Ok(Self(HeaderName::from_bytes(ob.extract()?).unwrap()))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyHeaderValue(HeaderValue);

impl<'py> FromPyObject<'py> for PyHeaderValue {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(
            HeaderValue::from_str(ob.extract::<PyBackedStr>()?.as_ref()).unwrap(),
        ))
    }
}

pub struct PyHeaderMap(HeaderMap);

impl<'py> FromPyObject<'py> for PyHeaderMap {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_input = ob.extract::<HashMap<PyHeaderName, PyHeaderValue>>()?;
        let mut header = HeaderMap::with_capacity(py_input.len());
        for (key, value) in py_input.into_iter() {
            header.insert(key.0, value.0);
        }
        Ok(Self(header))
    }
}
