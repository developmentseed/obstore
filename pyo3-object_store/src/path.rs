use object_store::path::Path;
use pyo3::prelude::*;
use pyo3::types::PyString;

#[derive(Clone, Debug, Default)]
pub(crate) struct PyPath(Path);

impl PyPath {
    pub fn into_inner(self) -> Path {
        self.0
    }
}

impl<'py> FromPyObject<'py> for PyPath {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(ob.extract::<String>()?.into()))
    }
}

impl<'py> IntoPyObject<'py> for PyPath {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, self.0.as_ref()))
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
