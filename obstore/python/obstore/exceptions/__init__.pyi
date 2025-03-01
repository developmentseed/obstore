# Note: This should be able to be an `exceptions.pyi` file one level above, however
# pylance isn't able to find that. So this is an exceptions module with only
# `__init__.pyi` to work around pylance's bug.

class BaseError(Exception):
    """The base exception class."""

class GenericError(BaseError):
    """A fallback error type when no variant matches."""

class NotFoundError(BaseError):
    """Error when the object is not found at given location."""

class InvalidPathError(BaseError):
    """Error for invalid path."""

class JoinError(BaseError):
    """Error when `tokio::spawn` failed."""

class NotSupportedError(BaseError):
    """Error when the attempted operation is not supported."""

class AlreadyExistsError(BaseError):
    """Error when the object already exists."""

class PreconditionError(BaseError):
    """Error when the required conditions failed for the operation."""

class NotModifiedError(BaseError):
    """Error when the object at the location isn't modified."""

class PermissionDeniedError(BaseError):
    """Permission denied.

    Error when the used credentials don't have enough permission to perform the
    requested operation.
    """

class UnauthenticatedError(BaseError):
    """Error when the used credentials lack valid authentication."""

class UnknownConfigurationKeyError(BaseError):
    """Error when a configuration key is invalid for the store used."""
