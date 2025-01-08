use std::sync::Arc;

use pyo3::prelude::*;

use object_store::prefix::PrefixStore;
use object_store::ObjectStore;

use crate::PyObjectStore;

/// A Python-facing wrapper around a [`PrefixStore`].
#[pyclass(name = "PrefixStore", frozen)]
pub struct PyPrefixStore(Arc<PrefixStore<Arc<dyn ObjectStore>>>);

impl AsRef<Arc<PrefixStore<Arc<dyn ObjectStore>>>> for PyPrefixStore {
    fn as_ref(&self) -> &Arc<PrefixStore<Arc<dyn ObjectStore>>> {
        &self.0
    }
}

#[pymethods]
impl PyPrefixStore {
    #[new]
    fn new(store: PyObjectStore, prefix: String) -> Self {
        Self(Arc::new(PrefixStore::new(store.into_inner(), prefix)))
    }

    fn __repr__(&self) -> String {
        self.0.to_string()
    }
}
