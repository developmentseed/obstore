//! Contains the [`PyObjectStoreError`], the Error returned by most fallible functions in this
//! crate.

use pyo3::exceptions::{
    PyException, PyFileNotFoundError, PyIOError, PyNotImplementedError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::DowncastError;
use thiserror::Error;

/// The Error variants returned by this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PyObjectStoreError {
    /// A wrapped [object_store::Error]
    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    /// A wrapped [PyErr]
    #[error(transparent)]
    PyErr(#[from] PyErr),

    /// A wrapped [std::io::Error]
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

impl From<PyObjectStoreError> for PyErr {
    fn from(error: PyObjectStoreError) -> Self {
        match error {
            PyObjectStoreError::PyErr(err) => err,
            PyObjectStoreError::ObjectStoreError(ref err) => match err {
                object_store::Error::NotFound { path: _, source: _ } => {
                    PyFileNotFoundError::new_err(err.to_string())
                }
                object_store::Error::NotImplemented => {
                    PyNotImplementedError::new_err(err.to_string())
                }
                _ => PyException::new_err(err.to_string()),
            },
            PyObjectStoreError::IOError(err) => PyIOError::new_err(err.to_string()),
        }
    }
}

impl<'a, 'py> From<DowncastError<'a, 'py>> for PyObjectStoreError {
    fn from(other: DowncastError<'a, 'py>) -> Self {
        Self::PyErr(PyValueError::new_err(format!(
            "Could not downcast: {}",
            other
        )))
    }
}

/// A type wrapper around `Result<T, PyObjectStoreError>`.
pub type PyObjectStoreResult<T> = Result<T, PyObjectStoreError>;
