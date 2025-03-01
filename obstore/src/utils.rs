use pyo3::prelude::*;

/// Returning `()` from `future_into_py` returns an empty tuple instead of None
/// https://github.com/developmentseed/obstore/issues/240
pub(crate) struct PyNone;

impl<'py> IntoPyObject<'py> for PyNone {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(py.None().bind(py).clone())
    }
}
