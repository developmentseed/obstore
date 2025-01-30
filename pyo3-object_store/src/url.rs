use pyo3::exceptions::PyValueError;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyAnyMethods;
use pyo3::FromPyObject;
use url::Url;

/// A wrapper around [`url::Url`] that implements [`FromPyObject`].
pub struct PyUrl(Url);

impl PyUrl {
    pub(crate) fn into_inner(self) -> Url {
        self.0
    }
}

impl<'py> FromPyObject<'py> for PyUrl {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?;
        let url = Url::parse(&s).map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(url))
    }
}

impl AsRef<Url> for PyUrl {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}
