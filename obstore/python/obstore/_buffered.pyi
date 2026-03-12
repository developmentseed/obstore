from ._attributes import Attributes
from ._buffered_types import (
    AsyncReadableFile,
    AsyncWritableFile,
    ReadableFile,
    WritableFile,
)
from ._store import ObjectStore

def open_reader(
    store: ObjectStore,
    path: str,
    *,
    buffer_size: int = 1024 * 1024,
) -> ReadableFile:
    """Open a readable file object from the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Keyword Args:
        buffer_size: The minimum number of bytes to read in a single request. Up to `buffer_size` bytes will be buffered in memory.

    Returns:
        ReadableFile

    """

async def open_reader_async(
    store: ObjectStore,
    path: str,
    *,
    buffer_size: int = 1024 * 1024,
) -> AsyncReadableFile:
    """Call `open_reader` asynchronously, returning a readable file object with asynchronous operations.

    Refer to the documentation for [open_reader][obstore.open_reader].
    """

def open_writer(
    store: ObjectStore,
    path: str,
    *,
    attributes: Attributes | None = None,
    buffer_size: int = 10 * 1024 * 1024,
    tags: dict[str, str] | None = None,
    max_concurrency: int = 12,
) -> WritableFile:
    """Open a writable file object at the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Keyword Args:
        attributes: Provide a set of `Attributes`. Defaults to `None`.
        buffer_size: The underlying buffer size to use. Up to `buffer_size` bytes will be buffered in memory. If `buffer_size` is exceeded, data will be uploaded as a multipart upload in chunks of `buffer_size`.
        tags: Provide tags for this object. Defaults to `None`.
        max_concurrency: The maximum number of chunks to upload concurrently. Defaults to 12.

    Returns:
        ReadableFile

    """

def open_writer_async(
    store: ObjectStore,
    path: str,
    *,
    attributes: Attributes | None = None,
    buffer_size: int = 10 * 1024 * 1024,
    tags: dict[str, str] | None = None,
    max_concurrency: int = 12,
) -> AsyncWritableFile:
    """Open an **asynchronous** writable file object at the specified location.

    Refer to the documentation for [open_writer][obstore.open_writer].
    """
