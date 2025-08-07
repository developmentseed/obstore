use object_store::path::Path;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_object_store::PyPath;

pub(crate) enum PyPaths {
    One(Path),
    // TODO: also support an Arrow String Array here.
    Many(Vec<Path>),
}

impl<'py> FromPyObject<'py> for PyPaths {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path) = ob.extract::<PyPath>() {
            Ok(Self::One(path.into_inner()))
        } else if let Ok(paths) = ob.extract::<Vec<PyPath>>() {
            Ok(Self::Many(
                paths.into_iter().map(|path| path.into_inner()).collect(),
            ))
        } else {
            Err(PyTypeError::new_err(
                "Expected string path or sequence of string paths.",
            ))
        }
    }
}
