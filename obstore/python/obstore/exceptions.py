"""Exceptions."""

import obspec.exceptions as obspec_exc

from obstore._obstore import _exceptions as pyo3_exc


class BaseError(obspec_exc.BaseError, pyo3_exc.BaseError):
    """The base Python-facing exception from which all other errors subclass."""


class GenericError(obspec_exc.GenericError, pyo3_exc.GenericError):
    """A fallback error type when no variant matches."""


class NotFoundError(obspec_exc.NotFoundError, pyo3_exc.NotFoundError):
    """Error when the object is not found at given location."""


class InvalidPathError(obspec_exc.InvalidPathError, pyo3_exc.InvalidPathError):
    """Error for invalid path."""


class JoinError(obspec_exc.JoinError, pyo3_exc.JoinError):
    """Error when tokio::spawn failed."""


class NotSupportedError(obspec_exc.NotSupportedError, pyo3_exc.NotSupportedError):
    """Error when the attempted operation is not supported."""


class AlreadyExistsError(obspec_exc.AlreadyExistsError, pyo3_exc.AlreadyExistsError):
    """Error when the object already exists."""


class PreconditionError(obspec_exc.PreconditionError, pyo3_exc.PreconditionError):
    """Error when the required conditions failed for the operation."""


class NotModifiedError(obspec_exc.NotModifiedError, pyo3_exc.NotModifiedError):
    """Error when the object at the location isn't modified."""


class NotImplementedError(obspec_exc.NotImplementedError):  # noqa: A001
    """Error when an operation is not implemented.

    Subclasses from the built-in [NotImplementedError][].
    """


class PermissionDeniedError(
    obspec_exc.PermissionDeniedError,
    pyo3_exc.PermissionDeniedError,
):
    """Error when the used credentials don't have enough permission to perform the requested operation."""  # noqa: E501


class UnauthenticatedError(
    obspec_exc.UnauthenticatedError,
    pyo3_exc.UnauthenticatedError,
):
    """Error when the used credentials lack valid authentication."""


class UnknownConfigurationKeyError(
    obspec_exc.UnknownConfigurationKeyError,
    pyo3_exc.UnknownConfigurationKeyError,
):
    """Error when a configuration key is invalid for the store used."""
