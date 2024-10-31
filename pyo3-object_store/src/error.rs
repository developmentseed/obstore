//! Contains the [`PyObjectStoreError`], the Error returned by most fallible functions in this
//! crate.

#![allow(missing_docs)]

use pyo3::exceptions::{PyFileNotFoundError, PyIOError, PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::{create_exception, DowncastError};
use thiserror::Error;

// Base exception
create_exception!(
    pyo3_object_store,
    ObstoreError,
    pyo3::exceptions::PyException
);

// Subclasses from base exception
create_exception!(pyo3_object_store, GenericError, ObstoreError);
create_exception!(pyo3_object_store, NotFoundError, ObstoreError);
create_exception!(pyo3_object_store, InvalidPathError, ObstoreError);
create_exception!(pyo3_object_store, JoinError, ObstoreError);
create_exception!(pyo3_object_store, NotSupportedError, ObstoreError);
create_exception!(pyo3_object_store, AlreadyExistsError, ObstoreError);
create_exception!(pyo3_object_store, PreconditionError, ObstoreError);
create_exception!(pyo3_object_store, NotModifiedError, ObstoreError);
create_exception!(pyo3_object_store, PermissionDeniedError, ObstoreError);
create_exception!(pyo3_object_store, UnauthenticatedError, ObstoreError);
create_exception!(
    pyo3_object_store,
    UnknownConfigurationKeyError,
    ObstoreError
);

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
                object_store::Error::Generic {
                    store: _,
                    source: _,
                } => GenericError::new_err(err.to_string()),
                object_store::Error::NotFound { path: _, source: _ } => {
                    PyFileNotFoundError::new_err(err.to_string())
                }
                object_store::Error::InvalidPath { source: _ } => {
                    InvalidPathError::new_err(err.to_string())
                }
                object_store::Error::JoinError { source: _ } => JoinError::new_err(err.to_string()),
                object_store::Error::NotSupported { source: _ } => {
                    NotSupportedError::new_err(err.to_string())
                }
                object_store::Error::AlreadyExists { path: _, source: _ } => {
                    AlreadyExistsError::new_err(err.to_string())
                }
                object_store::Error::Precondition { path: _, source: _ } => {
                    PreconditionError::new_err(err.to_string())
                }
                object_store::Error::NotModified { path: _, source: _ } => {
                    NotModifiedError::new_err(err.to_string())
                }
                object_store::Error::NotImplemented => {
                    PyNotImplementedError::new_err(err.to_string())
                }
                object_store::Error::PermissionDenied { path: _, source: _ } => {
                    PermissionDeniedError::new_err(err.to_string())
                }
                object_store::Error::Unauthenticated { path: _, source: _ } => {
                    UnauthenticatedError::new_err(err.to_string())
                }
                object_store::Error::UnknownConfigurationKey { store: _, key: _ } => {
                    UnknownConfigurationKeyError::new_err(err.to_string())
                }
                _ => GenericError::new_err(err.to_string()),
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
