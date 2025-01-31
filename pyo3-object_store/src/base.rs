use pyo3::prelude::*;

/// Base class from which all other stores subclass
#[pyclass(name = "BaseObjectStore", frozen, subclass)]
pub struct PyBaseObjectStore {}
