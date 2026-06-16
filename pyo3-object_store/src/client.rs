use std::collections::HashMap;
use std::str::FromStr;

use http::{HeaderMap, HeaderName, HeaderValue};
use object_store::{Certificate, ClientConfigKey, ClientOptions};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::{PyBackedBytes, PyBackedStr};
use pyo3::types::{PyBytes, PyDict, PyString};

use crate::config::PyConfigValue;
use crate::error::PyObjectStoreError;

fn extract_pem(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = value.extract::<PyBackedBytes>() {
        Ok(bytes.as_ref().to_vec())
    } else {
        let s = value.extract::<PyBackedStr>()?;
        Ok(s.as_bytes().to_vec())
    }
}

/// A wrapper around `ClientConfigKey` that implements [`FromPyObject`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PyClientConfigKey(ClientConfigKey);

impl<'py> FromPyObject<'_, 'py> for PyClientConfigKey {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, pyo3::PyAny>) -> PyResult<Self> {
        let s = obj.extract::<PyBackedStr>()?.to_lowercase();
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
#[derive(Clone, Debug, PartialEq)]
pub struct PyClientOptions {
    string_options: HashMap<PyClientConfigKey, PyConfigValue>,
    default_headers: Option<PyHeaderMap>,
    // Stored as raw PEM so it round-trips through `IntoPyObject`; parsed on the
    // way into `ClientOptions`.
    root_certificate: Option<Vec<u8>>,
}

impl<'py> FromPyObject<'_, 'py> for PyClientOptions {
    type Error = PyErr;

    // Need custom extraction because default headers is not a string value
    fn extract(obj: Borrowed<'_, 'py, pyo3::PyAny>) -> PyResult<Self> {
        let dict = obj.extract::<Bound<PyDict>>()?;
        let mut string_options = HashMap::new();
        let mut default_headers = None;
        let mut root_certificate = None;

        for (key, value) in dict.iter() {
            if let Ok(key) = key.extract::<PyClientConfigKey>() {
                string_options.insert(key, value.extract::<PyConfigValue>()?);
            } else {
                let key = key.extract::<PyBackedStr>()?;
                match &*key {
                    "default_headers" => default_headers = Some(value.extract::<PyHeaderMap>()?),
                    "root_certificate" => root_certificate = Some(extract_pem(&value)?),
                    _ => return Err(PyValueError::new_err(format!("Invalid key: {key}."))),
                }
            }
        }

        Ok(Self {
            string_options,
            default_headers,
            root_certificate,
        })
    }
}

impl<'py> IntoPyObject<'py> for PyClientOptions {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = self.string_options.into_pyobject(py)?;
        if let Some(headers) = self.default_headers {
            dict.set_item("default_headers", headers)?;
        }
        if let Some(pem) = self.root_certificate {
            dict.set_item("root_certificate", PyBytes::new(py, &pem))?;
        }
        Ok(dict)
    }
}

impl<'py> IntoPyObject<'py> for &PyClientOptions {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = (&self.string_options).into_pyobject(py)?;
        if let Some(headers) = &self.default_headers {
            dict.set_item("default_headers", headers)?;
        }
        if let Some(pem) = &self.root_certificate {
            dict.set_item("root_certificate", PyBytes::new(py, pem))?;
        }
        Ok(dict.clone())
    }
}

impl TryFrom<PyClientOptions> for ClientOptions {
    type Error = PyObjectStoreError;

    fn try_from(value: PyClientOptions) -> Result<Self, Self::Error> {
        let mut options = ClientOptions::new();
        for (key, value) in value.string_options.into_iter() {
            options = options.with_config(key.0, value.0);
        }

        if let Some(headers) = value.default_headers {
            options = options.with_default_headers(headers.0);
        }

        if let Some(pem) = value.root_certificate {
            for certificate in Certificate::from_pem_bundle(&pem)? {
                options = options.with_root_certificate(certificate);
            }
        }

        Ok(options)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct PyHeaderMap(HeaderMap);

impl<'py> FromPyObject<'_, 'py> for PyHeaderMap {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, pyo3::PyAny>) -> PyResult<Self> {
        let dict = obj.extract::<Bound<PyDict>>()?;
        let mut header_map = HeaderMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key = HeaderName::from_str(&key.extract::<PyBackedStr>()?)
                .map_err(|err| PyValueError::new_err(err.to_string()))?;

            // HTTP Header values can have non-ascii bytes, so first try to extract as bytes.
            let value = if let Ok(value_bytes) = value.extract::<PyBackedBytes>() {
                HeaderValue::from_bytes(&value_bytes)
            } else {
                HeaderValue::from_str(&value.extract::<PyBackedStr>()?)
            }
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

            header_map.insert(key, value);
        }
        Ok(Self(header_map))
    }
}

impl<'py> IntoPyObject<'py> for PyHeaderMap {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        for (key, value) in self.0.iter() {
            dict.set_item(key.as_str(), value.as_bytes())?;
        }
        Ok(dict)
    }
}

impl<'py> IntoPyObject<'py> for &PyHeaderMap {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        for (key, value) in self.0.iter() {
            dict.set_item(key.as_str(), value.as_bytes())?;
        }
        Ok(dict)
    }
}
