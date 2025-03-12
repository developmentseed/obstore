mod credentials;

use pyo3::intern;
use pyo3::prelude::*;

pub fn register_tests_module(
    py: Python<'_>,
    parent_module: &Bound<'_, PyModule>,
    parent_module_str: &str,
) -> PyResult<()> {
    let full_module_string = format!("{}._tests", parent_module_str);

    let child_module = PyModule::new(parent_module.py(), "_tests")?;

    child_module.add_wrapped(wrap_pyfunction!(from_url))?;
    child_module.add_class::<PyAzureStore>()?;
    child_module.add_class::<PyGCSStore>()?;
    child_module.add_class::<PyHttpStore>()?;
    child_module.add_class::<PyLocalStore>()?;
    child_module.add_class::<PyMemoryStore>()?;
    child_module.add_class::<PyS3Store>()?;

    // Set the value of `__module__` correctly on each publicly exposed function or class
    let __module__ = intern!(py, "__module__");
    child_module
        .getattr("from_url")?
        .setattr(__module__, &full_module_string)?;
    child_module
        .getattr("AzureStore")?
        .setattr(__module__, &full_module_string)?;
    child_module
        .getattr("GCSStore")?
        .setattr(__module__, &full_module_string)?;
    child_module
        .getattr("HTTPStore")?
        .setattr(__module__, &full_module_string)?;
    child_module
        .getattr("LocalStore")?
        .setattr(__module__, &full_module_string)?;
    child_module
        .getattr("MemoryStore")?
        .setattr(__module__, &full_module_string)?;
    child_module
        .getattr("S3Store")?
        .setattr(__module__, &full_module_string)?;

    // Add the child module to the parent module
    parent_module.add_submodule(&child_module)?;

    py.import(intern!(py, "sys"))?
        .getattr(intern!(py, "modules"))?
        .set_item(&full_module_string, &child_module)?;

    // needs to be set *after* `add_submodule()`
    child_module.setattr("__name__", full_module_string)?;

    Ok(())
}
