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
use crate::PyObjectStoreResult;

/// A wrapper around one or more `Certificate`s parsed from PEM input.
///
/// The original PEM is retained so the value round-trips through
/// [`IntoPyObject`]; parsing happens once, on extraction.
#[derive(Clone, Debug)]
struct PyCertificate {
    pem: Vec<u8>,
    certificates: Vec<Certificate>,
}

impl PyCertificate {
    fn new(pem: Vec<u8>) -> PyObjectStoreResult<Self> {
        let certificates = Certificate::from_pem_bundle(&pem)?;
        if certificates.is_empty() {
            return Err(PyValueError::new_err(
                "No certificates found in `root_certificate` input; expected one or more PEM-encoded certificates.",
            )
            .into());
        }
        Ok(Self { pem, certificates })
    }
}

impl PartialEq for PyCertificate {
    fn eq(&self, other: &Self) -> bool {
        self.pem == other.pem
    }
}

impl<'py> FromPyObject<'_, 'py> for PyCertificate {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, pyo3::PyAny>) -> PyResult<Self> {
        let pem = if let Ok(bytes) = obj.extract::<Vec<u8>>() {
            bytes
        } else {
            obj.extract::<String>()?.into_bytes()
        };
        Ok(Self::new(pem)?)
    }
}

impl<'py> IntoPyObject<'py> for &PyCertificate {
    type Target = PyBytes;
    type Output = Bound<'py, PyBytes>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyBytes::new(py, &self.pem))
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
    root_certificate: Option<PyCertificate>,
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
                    "root_certificate" => {
                        root_certificate = Some(value.extract::<PyCertificate>()?)
                    }
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
        if let Some(certificate) = &self.root_certificate {
            dict.set_item("root_certificate", certificate)?;
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
        if let Some(certificate) = &self.root_certificate {
            dict.set_item("root_certificate", certificate)?;
        }
        Ok(dict.clone())
    }
}

impl From<PyClientOptions> for ClientOptions {
    fn from(value: PyClientOptions) -> Self {
        let mut options = ClientOptions::new();
        for (key, value) in value.string_options.into_iter() {
            options = options.with_config(key.0, value.0);
        }

        if let Some(headers) = value.default_headers {
            options = options.with_default_headers(headers.0);
        }

        if let Some(certificate) = value.root_certificate {
            for certificate in certificate.certificates {
                options = options.with_root_certificate(certificate);
            }
        }

        options
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
