use object_store::path::Path;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyType;

use crate::PyObjectStoreResult;

/// A wrapper around [`object_store::path::Path`].
///
/// This exists as a pyclass so that end users can customize the parsing from string to Path
///
/// Most Rust users should use [PyPathInput] instead, which is a more flexible input type that can
/// accept both Path and strings.
#[derive(Clone, Debug, Default, PartialEq)]
#[pyclass(name = "Path", frozen)]
pub struct PyPath(Path);

#[pymethods]
impl PyPath {
    #[classmethod]
    pub(crate) fn parse(_cls: &Bound<PyType>, s: PyBackedStr) -> PyObjectStoreResult<Self> {
        let path = Path::parse(&s).map_err(|err| {
            PyValueError::new_err(format!("Failed to parse path from string '{}': {}", s, err))
        })?;
        Ok(Self(path))
    }

    fn __str__(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<PyPathInput> for PyPath {
    fn from(value: PyPathInput) -> Self {
        Self(value.0)
    }
}

/// An input type that accepts either a `Path` or a `str`.
///
/// Note that strings will be converted to `Path` using the `From` impl. This does not support
/// percent-encoded sequences, and users should use `PyPath::parse` instead.
#[derive(Debug, Clone, PartialEq)]
pub struct PyPathInput(Path);

impl<'py> FromPyObject<'py> for PyPathInput {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path) = ob.downcast::<PyPath>() {
            Ok(Self(path.get().0.clone()))
        } else if let Ok(path) = ob.extract::<PyBackedStr>() {
            let path_ref: &str = path.as_ref();
            Ok(Self(path_ref.into()))
        } else {
            Err(PyValueError::new_err(format!(
                "Expected Path or str input, got {}",
                ob.get_type()
            )))
        }
    }
}

impl From<PyPathInput> for Path {
    fn from(value: PyPathInput) -> Self {
        value.0
    }
}

// impl<'py> IntoPyObject<'py> for PyPath {
//     type Target = PyString;
//     type Output = Bound<'py, PyString>;
//     type Error = PyErr;

//     fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
//         Ok(PyString::new(py, self.0.as_ref()))
//     }
// }

// impl<'py> IntoPyObject<'py> for &PyPath {
//     type Target = PyString;
//     type Output = Bound<'py, PyString>;
//     type Error = PyErr;

//     fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
//         Ok(PyString::new(py, self.0.as_ref()))
//     }
// }

impl AsRef<Path> for PyPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl From<PyPath> for Path {
    fn from(value: PyPath) -> Self {
        value.0
    }
}

impl From<Path> for PyPath {
    fn from(value: Path) -> Self {
        Self(value)
    }
}
