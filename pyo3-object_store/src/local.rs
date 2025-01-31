use std::fs::create_dir_all;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStoreScheme;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;
use url::Url;

use crate::error::PyObjectStoreResult;

/// A Python-facing wrapper around a [`LocalFileSystem`].
#[pyclass(name = "LocalStore", frozen)]
pub struct PyLocalStore(Arc<LocalFileSystem>);

impl AsRef<Arc<LocalFileSystem>> for PyLocalStore {
    fn as_ref(&self) -> &Arc<LocalFileSystem> {
        &self.0
    }
}

impl PyLocalStore {
    /// Consume self and return the underlying [`LocalFileSystem`].
    pub fn into_inner(self) -> Arc<LocalFileSystem> {
        self.0
    }
}

#[pymethods]
impl PyLocalStore {
    #[new]
    #[pyo3(signature = (prefix=None, *, automatic_cleanup=false, mkdir=false))]
    fn py_new(
        prefix: Option<std::path::PathBuf>,
        automatic_cleanup: bool,
        mkdir: bool,
    ) -> PyObjectStoreResult<Self> {
        let fs = if let Some(prefix) = prefix {
            if mkdir {
                create_dir_all(&prefix)?;
            }
            LocalFileSystem::new_with_prefix(prefix)?
        } else {
            LocalFileSystem::new()
        };
        let fs = fs.with_automatic_cleanup(automatic_cleanup);
        Ok(Self(Arc::new(fs)))
    }

    #[classmethod]
    fn from_url(_cls: &Bound<PyType>, url: &str) -> PyObjectStoreResult<Self> {
        let url = Url::parse(url).map_err(|err| PyValueError::new_err(err.to_string()))?;
        let (scheme, path) = ObjectStoreScheme::parse(&url).map_err(object_store::Error::from)?;

        if !matches!(scheme, ObjectStoreScheme::Local) {
            return Err(PyValueError::new_err("Not a `file://` URL").into());
        }

        // The path returned by `ObjectStoreScheme::parse` strips the initial `/`, so we join it
        // onto a root
        // Hopefully this also works on Windows.
        let root = std::path::Path::new("/");
        let full_path = root.join(path.as_ref());
        let fs = LocalFileSystem::new_with_prefix(full_path)?;
        Ok(Self(Arc::new(fs)))
    }

    fn __repr__(&self) -> String {
        let repr = self.0.to_string();
        repr.replacen("LocalFileSystem", "LocalStore", 1)
    }
}
