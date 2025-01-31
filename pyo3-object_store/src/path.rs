use object_store::path::Path;
use pyo3::prelude::*;

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
