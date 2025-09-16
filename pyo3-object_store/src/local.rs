use std::fs::create_dir_all;
use std::sync::Arc;

use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::local::LocalFileSystem;
use object_store::{ListResult, ObjectStore, ObjectStoreScheme};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::error::PyObjectStoreResult;
use crate::PyUrl;

#[derive(Clone, Debug, PartialEq)]
struct LocalConfig {
    prefix: Option<std::path::PathBuf>,
    automatic_cleanup: bool,
    mkdir: bool,
}

impl LocalConfig {
    fn __getnewargs_ex__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let args = PyTuple::new(py, vec![self.prefix.clone()])?.into_bound_py_any(py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(intern!(py, "automatic_cleanup"), self.automatic_cleanup)?;
        kwargs.set_item(intern!(py, "mkdir"), self.mkdir)?;
        PyTuple::new(py, [args, kwargs.into_bound_py_any(py)?])
    }
}

/// A Python-facing wrapper around a [`LocalFileSystem`].
#[derive(Debug, Clone)]
#[pyclass(name = "LocalStore", frozen, subclass)]
pub struct PyLocalStore {
    store: Arc<LocalFileSystem>,
    config: LocalConfig,
}

impl AsRef<Arc<LocalFileSystem>> for PyLocalStore {
    fn as_ref(&self) -> &Arc<LocalFileSystem> {
        &self.store
    }
}

impl PyLocalStore {
    /// Consume self and return the underlying [`LocalFileSystem`].
    pub fn into_inner(self) -> Arc<LocalFileSystem> {
        self.store
    }
}

#[pymethods]
impl PyLocalStore {
    #[new]
    #[pyo3(signature = (prefix=None, *, automatic_cleanup=false, mkdir=false))]
    fn new(
        prefix: Option<std::path::PathBuf>,
        automatic_cleanup: bool,
        mkdir: bool,
    ) -> PyObjectStoreResult<Self> {
        let fs = if let Some(prefix) = &prefix {
            if mkdir {
                create_dir_all(prefix)?;
            }
            LocalFileSystem::new_with_prefix(prefix)?
        } else {
            LocalFileSystem::new()
        };
        let fs = fs.with_automatic_cleanup(automatic_cleanup);
        Ok(Self {
            store: Arc::new(fs),
            config: LocalConfig {
                prefix,
                automatic_cleanup,
                mkdir,
            },
        })
    }

    #[classmethod]
    #[pyo3(signature = (url, *, automatic_cleanup=false, mkdir=false))]
    pub(crate) fn from_url<'py>(
        cls: &Bound<'py, PyType>,
        url: PyUrl,
        automatic_cleanup: bool,
        mkdir: bool,
    ) -> PyObjectStoreResult<Bound<'py, PyAny>> {
        let url = url.into_inner();
        let (scheme, path) = ObjectStoreScheme::parse(&url).map_err(object_store::Error::from)?;

        if !matches!(scheme, ObjectStoreScheme::Local) {
            return Err(PyValueError::new_err("Not a `file://` URL").into());
        }

        // The path returned by `ObjectStoreScheme::parse` strips the initial `/`, so we join it
        // onto a root
        // Hopefully this also works on Windows.
        let root = std::path::Path::new("/");
        let full_path = root.join(path.as_ref());

        // Note: we pass **back** through Python so that if cls is a subclass, we instantiate the
        // subclass
        let kwargs = PyDict::new(cls.py());
        kwargs.set_item("prefix", full_path)?;
        kwargs.set_item("automatic_cleanup", automatic_cleanup)?;
        kwargs.set_item("mkdir", mkdir)?;
        Ok(cls.call((), Some(&kwargs))?)
    }

    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        // Ensure we never error on __eq__ by returning false if the other object is not the same
        // type
        other
            .downcast::<PyLocalStore>()
            .map(|other| self.config == other.get().config)
            .unwrap_or(false)
    }

    fn __getnewargs_ex__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        self.config.__getnewargs_ex__(py)
    }

    fn __repr__(&self) -> String {
        if let Some(prefix) = &self.config.prefix {
            format!("LocalStore(\"{}\")", prefix.display())
        } else {
            "LocalStore".to_string()
        }
    }

    #[getter]
    fn prefix<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Note: returning a std::path::Path or std::path::PathBuf converts back to a Python _str_
        // not a Python _pathlib.Path_.
        // So we manually convert to a pathlib.Path
        if let Some(prefix) = &self.config.prefix {
            let pathlib_mod = py.import(intern!(py, "pathlib"))?;
            pathlib_mod.call_method1(intern!(py, "Path"), PyTuple::new(py, vec![prefix])?)
        } else {
            py.None().into_bound_py_any(py)
        }
    }
}

/// A custom implementation of PaginatedListStore for LocalFileSystem
///
/// PaginatedListStore is not implemented in upstream for LocalFileSystem because there's no way to
/// get a stable offset in local FS APIs.
/// https://github.com/apache/arrow-rs-object-store/issues/388
///
/// Instead, we collect _all_ results and filter them in memory with the provided substring.
#[async_trait::async_trait]
impl PaginatedListStore for PyLocalStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        _opts: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        // Split a path like "some/prefix/abc" into (Some(Path("some/prefix")), Some("abc"))
        // This allows us to do a substring prefix match after the / delimiter
        let (list_path, list_prefix_match): (Option<object_store::path::Path>, Option<String>) =
            if let Some(list_prefix) = prefix {
                if let Some((list_path, list_prefix_match)) = list_prefix.rsplit_once('/') {
                    // There's a / in the prefix, so we assume the part before the last / is a
                    // path, and the end is a substring match
                    (
                        Some(object_store::path::Path::parse(list_path)?),
                        Some(list_prefix_match.to_string()),
                    )
                } else {
                    // No / in prefix, so we assume it's a substring
                    (None, Some(list_prefix.to_string()))
                }
            } else {
                (None, None)
            };

        let list_result = self.store.list_with_delimiter(list_path.as_ref()).await?;

        // Filter list result to include only results with the given prefix after the / delimiter
        let filtered_list_result = if let Some(list_prefix_match) = list_prefix_match {
            let filtered_common_prefixes = list_result
                .common_prefixes
                .into_iter()
                .filter(|p| p.as_ref().starts_with(&list_prefix_match))
                .collect();
            let filtered_objects = list_result
                .objects
                .into_iter()
                .filter(|obj| obj.location.as_ref().starts_with(&list_prefix_match))
                .collect();
            ListResult {
                common_prefixes: filtered_common_prefixes,
                objects: filtered_objects,
            }
        } else {
            list_result
        };

        Ok(PaginatedListResult {
            result: filtered_list_result,
            // Local FS does not support pagination
            page_token: None,
        })
    }
}
