use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use pyo3::import_exception;
use pyo3_object_store::PyObjectStoreError;

import_exception!(obspec.exceptions, BaseError);

import_exception!(obspec.exceptions, GenericError);
import_exception!(obspec.exceptions, NotFoundError);
import_exception!(obspec.exceptions, InvalidPathError);
import_exception!(obspec.exceptions, JoinError);
import_exception!(obspec.exceptions, NotSupportedError);
import_exception!(obspec.exceptions, AlreadyExistsError);
import_exception!(obspec.exceptions, PreconditionError);
import_exception!(obspec.exceptions, NotModifiedError);
import_exception!(obspec.exceptions, NotImplementedError);
import_exception!(obspec.exceptions, PermissionDeniedError);
import_exception!(obspec.exceptions, UnauthenticatedError);
import_exception!(obspec.exceptions, UnknownConfigurationKeyError);

pub struct PyObstoreError(PyObjectStoreError);

impl From<PyObstoreError> for PyErr {
    fn from(value: PyObstoreError) -> Self {
        match value.0 {
            PyObjectStoreError::PyErr(err) => err,
            PyObjectStoreError::ObjectStoreError(ref err) => match err {
                object_store::Error::Generic {
                    store: _,
                    source: _,
                } => GenericError::new_err(print_with_debug(err)),
                object_store::Error::NotFound { path: _, source: _ } => {
                    NotFoundError::new_err(print_with_debug(err))
                }
                object_store::Error::InvalidPath { source: _ } => {
                    InvalidPathError::new_err(print_with_debug(err))
                }
                object_store::Error::JoinError { source: _ } => {
                    JoinError::new_err(print_with_debug(err))
                }
                object_store::Error::NotSupported { source: _ } => {
                    NotSupportedError::new_err(print_with_debug(err))
                }
                object_store::Error::AlreadyExists { path: _, source: _ } => {
                    AlreadyExistsError::new_err(print_with_debug(err))
                }
                object_store::Error::Precondition { path: _, source: _ } => {
                    PreconditionError::new_err(print_with_debug(err))
                }
                object_store::Error::NotModified { path: _, source: _ } => {
                    NotModifiedError::new_err(print_with_debug(err))
                }
                object_store::Error::NotImplemented => {
                    NotImplementedError::new_err(print_with_debug(err))
                }
                object_store::Error::PermissionDenied { path: _, source: _ } => {
                    PermissionDeniedError::new_err(print_with_debug(err))
                }
                object_store::Error::Unauthenticated { path: _, source: _ } => {
                    UnauthenticatedError::new_err(print_with_debug(err))
                }
                object_store::Error::UnknownConfigurationKey { store: _, key: _ } => {
                    UnknownConfigurationKeyError::new_err(print_with_debug(err))
                }
                _ => GenericError::new_err(print_with_debug(err)),
            },
            PyObjectStoreError::IOError(err) => PyIOError::new_err(err),
            err => GenericError::new_err(format!("{err}\n\nDebug source:\n{err:#?}")),
        }
    }
}

fn print_with_debug(err: &object_store::Error) -> String {
    // #? gives "pretty-printing" for debug
    // https://doc.rust-lang.org/std/fmt/trait.Debug.html
    format!("{err}\n\nDebug source:\n{err:#?}")
}
