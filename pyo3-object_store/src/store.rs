use std::sync::Arc;

use object_store::ObjectStore;
use pyo3::exceptions::{PyRuntimeWarning, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{intern, PyTypeInfo};

use crate::{PyAzureStore, PyGCSStore, PyHttpStore, PyLocalStore, PyMemoryStore, PyS3Store};

/// A wrapper around a Rust ObjectStore instance that allows any rust-native implementation of
/// ObjectStore.
///
/// This will only accept ObjectStore instances created from the same library. See
/// [register_store_module][crate::register_store_module].
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
        } else {
            let py = ob.py();
            // Check for object-store instance from other library
            let cls_name = ob
                .getattr(intern!(py, "__class__"))?
                .getattr(intern!(py, "__name__"))?
                .extract::<PyBackedStr>()?;
            if [
                PyAzureStore::NAME,
                PyGCSStore::NAME,
                PyHttpStore::NAME,
                PyLocalStore::NAME,
                PyMemoryStore::NAME,
                PyS3Store::NAME,
            ]
            .contains(&cls_name.as_ref())
            {
                return Err(PyValueError::new_err("You must use an object store instance exported from **the same library** as this function. They cannot be used across libraries.\nThis is because object store instances are compiled with a specific version of Rust and Python." ));
            }

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

impl From<PyObjectStore> for Arc<dyn ObjectStore> {
    fn from(value: PyObjectStore) -> Self {
        value.0
    }
}

impl PyObjectStore {
    /// Consume self and return the underlying [`ObjectStore`].
    pub fn into_inner(self) -> Arc<dyn ObjectStore> {
        self.0
    }

    /// Consume self and return a reference-counted [`ObjectStore`].
    pub fn into_dyn(self) -> Arc<dyn ObjectStore> {
        self.0
    }
}

/// This is defined as a separate enum so that variants aren't public
#[derive(Debug, Clone)]
enum PyExternalObjectStoreInner {
    Azure(PyAzureStore),
    #[allow(clippy::upper_case_acronyms)]
    GCS(PyGCSStore),
    Http(PyHttpStore),
    Local(PyLocalStore),
    S3(PyS3Store),
}

impl<'py> FromPyObject<'py> for PyExternalObjectStoreInner {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        // Check for object-store instance from other library
        let cls_name = ob
            .getattr(intern!(py, "__class__"))?
            .getattr(intern!(py, "__name__"))?
            .extract::<PyBackedStr>()?;

        if cls_name == PyAzureStore::NAME {
            let (args, kwargs): (Bound<PyTuple>, Bound<PyDict>) = ob
                .call_method0(intern!(py, "__getnewargs_ex__"))?
                .extract()?;
            let store = PyAzureStore::type_object(py)
                .call(args, Some(&kwargs))?
                .downcast::<PyAzureStore>()?
                .get()
                .clone();
            return Ok(Self::Azure(store));
        }

        if cls_name == PyGCSStore::NAME {
            let (args, kwargs): (Bound<PyTuple>, Bound<PyDict>) = ob
                .call_method0(intern!(py, "__getnewargs_ex__"))?
                .extract()?;
            let store = PyGCSStore::type_object(py)
                .call(args, Some(&kwargs))?
                .downcast::<PyGCSStore>()?
                .get()
                .clone();
            return Ok(Self::GCS(store));
        }

        if cls_name == PyHttpStore::NAME {
            let (args, kwargs): (Bound<PyTuple>, Bound<PyDict>) = ob
                .call_method0(intern!(py, "__getnewargs_ex__"))?
                .extract()?;
            let store = PyHttpStore::type_object(py)
                .call(args, Some(&kwargs))?
                .downcast::<PyHttpStore>()?
                .get()
                .clone();
            return Ok(Self::Http(store));
        }

        if cls_name == PyLocalStore::NAME {
            let (args, kwargs): (Bound<PyTuple>, Bound<PyDict>) = ob
                .call_method0(intern!(py, "__getnewargs_ex__"))?
                .extract()?;
            let store = PyLocalStore::type_object(py)
                .call(args, Some(&kwargs))?
                .downcast::<PyLocalStore>()?
                .get()
                .clone();
            return Ok(Self::Local(store));
        }

        if cls_name == PyS3Store::NAME {
            let (args, kwargs): (Bound<PyTuple>, Bound<PyDict>) = ob
                .call_method0(intern!(py, "__getnewargs_ex__"))?
                .extract()?;
            let store = PyS3Store::type_object(py)
                .call(args, Some(&kwargs))?
                .downcast::<PyS3Store>()?
                .get()
                .clone();
            return Ok(Self::S3(store));
        }

        Err(PyValueError::new_err(format!(
            "Expected an object store-compatible instance, got {}",
            ob.repr()?
        )))
    }
}

/// A wrapper around a Rust [ObjectStore] instance that will extract and recreate an ObjectStore
/// instance out of a Python object.
///
/// This will accept [ObjectStore] instances from **any** Python library exporting store classes
/// from `pyo3-object_store`.
///
/// ## Caveats
///
/// - This will extract the configuration of the store and **recreate** the store instance in the
///   current module. This means that no connection pooling will be reused from the original
///   library. Also, there is a slight overhead to this as configuration parsing will need to
///   happen from scratch.
///
///   This will work best when the store is created once and used multiple times.
///
/// - This relies on the public Python API (`__getnewargs_ex__` and `__init__`) of the store
///   classes to extract the configuration. If the public API changes in a non-backwards compatible
///   way, this store conversion may fail.
///
/// - While this reuses `__getnewargs_ex__` (from the pickle implementation) to extract arguments
///   to pass into `__init__`, it does not actually _use_ pickle, and so even non-pickleable
///   credential providers should work.
///
/// - This will not work for `PyMemoryStore` because we can't clone the internal state of the
///   store.
#[derive(Debug, Clone)]
pub struct PyExternalObjectStore(PyExternalObjectStoreInner);

impl From<PyExternalObjectStore> for Arc<dyn ObjectStore> {
    fn from(value: PyExternalObjectStore) -> Self {
        match value.0 {
            PyExternalObjectStoreInner::Azure(store) => store.into_inner(),
            PyExternalObjectStoreInner::GCS(store) => store.into_inner(),
            PyExternalObjectStoreInner::Http(store) => store.into_inner(),
            PyExternalObjectStoreInner::Local(store) => store.into_inner(),
            PyExternalObjectStoreInner::S3(store) => store.into_inner(),
        }
    }
}

impl PyExternalObjectStore {
    /// Consume self and return a reference-counted [`ObjectStore`].
    pub fn into_dyn(self) -> Arc<dyn ObjectStore> {
        self.into()
    }
}

impl<'py> FromPyObject<'py> for PyExternalObjectStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        match ob.extract() {
            Ok(inner) => {
                let py = ob.py();

                let warnings_mod = py.import(intern!(py, "warnings"))?;
                let warning = PyRuntimeWarning::new_err(
                    "Successfully reconstructed a store defined in another Python module. Connection pooling will not be shared across store instances.",
                );
                let args = PyTuple::new(py, vec![warning])?;
                warnings_mod.call_method1(intern!(py, "warn"), args)?;

                Ok(Self(inner))
            }
            Err(err) => Err(err),
        }
    }
}

/// A convenience wrapper around native and external ObjectStore instances.
///
/// Note that there may be performance differences between accepted variants here. If you wish to
/// only permit the highest-performance stores, use [`PyObjectStore`] directly as the parameter in
/// your signature.
#[derive(FromPyObject)]
pub enum AnyObjectStore {
    /// A wrapper around a [`PyObjectStore`].
    PyObjectStore(PyObjectStore),
    /// A wrapper around a [`PyExternalObjectStore`].
    PyExternalObjectStore(PyExternalObjectStore),
}

impl From<AnyObjectStore> for Arc<dyn ObjectStore> {
    fn from(value: AnyObjectStore) -> Self {
        match value {
            AnyObjectStore::PyObjectStore(store) => store.into(),
            AnyObjectStore::PyExternalObjectStore(store) => store.into(),
        }
    }
}

impl AnyObjectStore {
    /// Consume self and return a reference-counted [`ObjectStore`].
    pub fn into_dyn(self) -> Arc<dyn ObjectStore> {
        self.into()
    }
}
