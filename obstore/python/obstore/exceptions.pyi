class ObstoreError(Exception):
    """The base exception class"""

class GenericError(ObstoreError):
    """A fallback error type when no variant matches."""

class NotFoundError(ObstoreError):
    """Error when the object is not found at given location."""

class InvalidPathError(ObstoreError):
    """Error for invalid path."""

class JoinError(ObstoreError):
    """Error when `tokio::spawn` failed."""

class NotSupportedError(ObstoreError):
    """Error when the attempted operation is not supported."""

class AlreadyExistsError(ObstoreError):
    """Error when the object already exists."""

class PreconditionError(ObstoreError):
    """Error when the required conditions failed for the operation."""

class NotModifiedError(ObstoreError):
    """Error when the object at the location isn't modified."""

class PermissionDeniedError(ObstoreError):
    """
    Error when the used credentials don't have enough permission
    to perform the requested operation
    """

class UnauthenticatedError(ObstoreError):
    """Error when the used credentials lack valid authentication."""

class UnknownConfigurationKeyError(ObstoreError):
    """Error when a configuration key is invalid for the store used."""
