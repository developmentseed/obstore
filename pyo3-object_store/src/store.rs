use std::sync::Arc;

use object_store::ObjectStore;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

use crate::{
    PyAzureStore, PyGCSStore, PyHttpStore, PyLocalStore, PyMemoryStore, PyPrefixStore, PyS3Store,
};

/// A wrapper around a Rust ObjectStore instance that allows any rust-native implementation of
/// ObjectStore.
// (In the future we'll have a separate AnyObjectStore that allows either an fsspec-based
// implementation or a rust-based implementation.)
pub struct PyObjectStore(Arc<dyn ObjectStore>);

impl<'py> FromPyObject<'py> for PyObjectStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(store) = ob.downcast::<PyS3Store>() {
            Ok(Self(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyAzureStore>() {
            Ok(Self(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyGCSStore>() {
            Ok(Self(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyHttpStore>() {
            Ok(Self(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyLocalStore>() {
            Ok(Self(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyMemoryStore>() {
            Ok(Self(store.get().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyPrefixStore>() {
            Ok(Self(store.get().as_ref().clone()))
        } else {
            let py = ob.py();
            // Check for object-store instance from other library
            let cls_name = ob
                .getattr(intern!(py, "__class__"))?
                .getattr(intern!(py, "__name__"))?
                .extract::<PyBackedStr>()?;
            if [
                "AzureStore",
                "GCSStore",
                "HTTPStore",
                "LocalStore",
                "MemoryStore",
                "S3Store",
                "PrefixStore",
            ]
            .contains(&cls_name.as_ref())
            {
                return Err(PyValueError::new_err("You must use an object store instance exported from **the same library** as this function. They cannot be used across libraries.\nThis is because object store instances are compiled with a specific version of Rust and Python." ));
            }

            // TODO: Check for fsspec
            Err(PyValueError::new_err(format!(
                "Expected an object store instance, got {}",
                ob.repr()?
            )))
        }
    }
}

impl AsRef<Arc<dyn ObjectStore>> for PyObjectStore {
    fn as_ref(&self) -> &Arc<dyn ObjectStore> {
        &self.0
    }
}

impl PyObjectStore {
    /// Consume self and return the underlying [`ObjectStore`].
    pub fn into_inner(self) -> Arc<dyn ObjectStore> {
        self.0
    }
}
