//! Contains the [`PyObjectStoreError`], the error enum returned by all fallible functions in this
//! crate.

use pyo3::exceptions::{PyFileNotFoundError, PyIOError, PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::{create_exception, DowncastError};
use thiserror::Error;

// Base exception
create_exception!(
    pyo3_object_store,
    ObstoreError,
    pyo3::exceptions::PyException,
    "The base Python-facing exception from which all other errors subclass."
);

// Subclasses from base exception
create_exception!(
    pyo3_object_store,
    GenericError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::Generic]."
);
create_exception!(
    pyo3_object_store,
    NotFoundError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::NotFound]."
);
create_exception!(
    pyo3_object_store,
    InvalidPathError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::InvalidPath]."
);
create_exception!(
    pyo3_object_store,
    JoinError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::JoinError]."
);
create_exception!(
    pyo3_object_store,
    NotSupportedError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::NotSupported]."
);
create_exception!(
    pyo3_object_store,
    AlreadyExistsError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::AlreadyExists]."
);
create_exception!(
    pyo3_object_store,
    PreconditionError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::Precondition]."
);
create_exception!(
    pyo3_object_store,
    NotModifiedError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::NotModified]."
);
create_exception!(
    pyo3_object_store,
    PermissionDeniedError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::PermissionDenied]."
);
create_exception!(
    pyo3_object_store,
    UnauthenticatedError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::Unauthenticated]."
);
create_exception!(
    pyo3_object_store,
    UnknownConfigurationKeyError,
    ObstoreError,
    "A Python-facing exception wrapping [object_store::Error::UnknownConfigurationKey]."
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
        // #? gives "pretty-printing" in the errors
        // https://doc.rust-lang.org/std/fmt/trait.Debug.html
        match error {
            PyObjectStoreError::PyErr(err) => err,
            PyObjectStoreError::ObjectStoreError(ref err) => match err {
                object_store::Error::Generic {
                    store: _,
                    source: _,
                } => GenericError::new_err(format!("{err:#?}")),
                object_store::Error::NotFound { path: _, source: _ } => {
                    PyFileNotFoundError::new_err(format!("{err:#?}"))
                }
                object_store::Error::InvalidPath { source: _ } => {
                    InvalidPathError::new_err(format!("{err:#?}"))
                }
                object_store::Error::JoinError { source: _ } => {
                    JoinError::new_err(format!("{err:#?}"))
                }
                object_store::Error::NotSupported { source: _ } => {
                    NotSupportedError::new_err(format!("{err:#?}"))
                }
                object_store::Error::AlreadyExists { path: _, source: _ } => {
                    AlreadyExistsError::new_err(format!("{err:#?}"))
                }
                object_store::Error::Precondition { path: _, source: _ } => {
                    PreconditionError::new_err(format!("{err:#?}"))
                }
                object_store::Error::NotModified { path: _, source: _ } => {
                    NotModifiedError::new_err(format!("{err:#?}"))
                }
                object_store::Error::NotImplemented => {
                    PyNotImplementedError::new_err(format!("{err:#?}"))
                }
                object_store::Error::PermissionDenied { path: _, source: _ } => {
                    PermissionDeniedError::new_err(format!("{err:#?}"))
                }
                object_store::Error::Unauthenticated { path: _, source: _ } => {
                    UnauthenticatedError::new_err(format!("{err:#?}"))
                }
                object_store::Error::UnknownConfigurationKey { store: _, key: _ } => {
                    UnknownConfigurationKeyError::new_err(format!("{err:#?}"))
                }
                _ => GenericError::new_err(format!("{err:#?}")),
            },
            PyObjectStoreError::IOError(err) => PyIOError::new_err(format!("{err:#?}")),
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
