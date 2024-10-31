use pyo3::intern;
use pyo3::prelude::*;

use crate::error::*;
use crate::{PyAzureStore, PyGCSStore, PyHttpStore, PyLocalStore, PyMemoryStore, PyS3Store};

/// Export the default Python API as a submodule named `store` within the given parent module
// https://github.com/PyO3/pyo3/issues/1517#issuecomment-808664021
// https://github.com/PyO3/pyo3/issues/759#issuecomment-977835119
pub fn register_store_module(
    py: Python<'_>,
    parent_module: &Bound<'_, PyModule>,
    parent_module_str: &str,
) -> PyResult<()> {
    let full_module_string = format!("{}.store", parent_module_str);

    let child_module = PyModule::new_bound(parent_module.py(), "store")?;

    child_module.add_class::<PyAzureStore>()?;
    child_module.add_class::<PyGCSStore>()?;
    child_module.add_class::<PyHttpStore>()?;
    child_module.add_class::<PyLocalStore>()?;
    child_module.add_class::<PyMemoryStore>()?;
    child_module.add_class::<PyS3Store>()?;

    parent_module.add_submodule(&child_module)?;

    py.import_bound(intern!(py, "sys"))?
        .getattr(intern!(py, "modules"))?
        .set_item(full_module_string.as_str(), child_module.to_object(py))?;

    // needs to be set *after* `add_submodule()`
    child_module.setattr("__name__", full_module_string)?;

    Ok(())
}

/// Export exceptions as a submodule named `exceptions` within the given parent module
// https://github.com/PyO3/pyo3/issues/1517#issuecomment-808664021
// https://github.com/PyO3/pyo3/issues/759#issuecomment-977835119
pub fn register_exceptions_module(
    py: Python<'_>,
    parent_module: &Bound<'_, PyModule>,
    parent_module_str: &str,
) -> PyResult<()> {
    let full_module_string = format!("{}.exceptions", parent_module_str);

    let child_module = PyModule::new_bound(parent_module.py(), "exceptions")?;

    child_module.add("ObstoreError", py.get_type_bound::<ObstoreError>())?;
    child_module.add("GenericError", py.get_type_bound::<GenericError>())?;
    child_module.add("NotFoundError", py.get_type_bound::<NotFoundError>())?;
    child_module.add("InvalidPathError", py.get_type_bound::<InvalidPathError>())?;
    child_module.add("JoinError", py.get_type_bound::<JoinError>())?;
    child_module.add(
        "NotSupportedError",
        py.get_type_bound::<NotSupportedError>(),
    )?;
    child_module.add(
        "AlreadyExistsError",
        py.get_type_bound::<AlreadyExistsError>(),
    )?;
    child_module.add(
        "PreconditionError",
        py.get_type_bound::<PreconditionError>(),
    )?;
    child_module.add("NotModifiedError", py.get_type_bound::<NotModifiedError>())?;
    child_module.add(
        "PermissionDeniedError",
        py.get_type_bound::<PermissionDeniedError>(),
    )?;
    child_module.add(
        "UnauthenticatedError",
        py.get_type_bound::<UnauthenticatedError>(),
    )?;
    child_module.add(
        "UnknownConfigurationKeyError",
        py.get_type_bound::<UnknownConfigurationKeyError>(),
    )?;

    parent_module.add_submodule(&child_module)?;

    py.import_bound(intern!(py, "sys"))?
        .getattr(intern!(py, "modules"))?
        .set_item(full_module_string.as_str(), child_module.to_object(py))?;

    // needs to be set *after* `add_submodule()`
    child_module.setattr("__name__", full_module_string)?;

    Ok(())
}
