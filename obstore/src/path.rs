use object_store::path::Path;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_object_store::PyPath;

pub(crate) enum PyPaths {
    One(Path),
    // TODO: also support an Arrow String Array here.
    Many(Vec<Path>),
}

impl<'py> FromPyObject<'_, 'py> for PyPaths {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(path) = obj.extract::<PyPath>() {
            Ok(Self::One(path.into_inner()))
        } else if let Ok(paths) = obj.extract::<Vec<PyPath>>() {
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
