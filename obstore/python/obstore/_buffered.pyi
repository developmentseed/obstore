import os
import sys
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Dict, List, Self

from ._attributes import Attributes
from ._bytes import Bytes
from .store import ObjectStore

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    from typing_extensions import Buffer

def open_reader(store: ObjectStore, path: str) -> ReadableFile:
    """Open a readable file object from the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Returns:
        ReadableFile
    """

async def open_reader_async(store: ObjectStore, path: str) -> AsyncReadableFile:
    """Call `open_reader` asynchronously, returning a readable file object with asynchronous operations.

    Refer to the documentation for [open_reader][obstore.open_reader].
    """

class ReadableFile:
    """A readable file object with synchronous operations.

    This implements a similar interface as a generic readable Python binary file-like
    object.
    """

    def close(self) -> None:
        """Close the current file.

        This is currently a no-op.
        """

    def read(self, size: int | None = None, /) -> Bytes:
        """
        Read up to `size` bytes from the object and return them. As a convenience, if
        size is unspecified or `None`, all bytes until EOF are returned.
        """

    def readall(self) -> Bytes:
        """
        Read and return all the bytes from the stream until EOF, using multiple calls to
        the stream if necessary.
        """

    def readline(self) -> Bytes:
        """Read a single line of the file, up until the next newline character."""

    def readlines(self, hint: int = -1, /) -> List[Bytes]:
        """Read all remaining lines into a list of buffers"""

    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> int:
        """
        Change the stream position to the given byte _offset_, interpreted relative to
        the position indicated by _whence_, and return the new absolute position. Values
        for _whence_ are:

        - [`os.SEEK_SET`][] or 0: start of the stream (the default); `offset` should be zero or positive
        - [`os.SEEK_CUR`][] or 1: current stream position; `offset` may be negative
        - [`os.SEEK_END`][] or 2: end of the stream; `offset` is usually negative
        """

    def seekable(self) -> bool:
        """Return True if the stream supports random access."""

    def tell(self) -> int:
        """Return the current stream position."""

class AsyncReadableFile:
    """A readable file object with **asynchronous** operations."""

    def close(self) -> None:
        """Close the current file.

        This is currently a no-op.
        """

    async def read(self, size: int | None = None, /) -> Bytes:
        """
        Read up to `size` bytes from the object and return them. As a convenience, if
        size is unspecified or `None`, all bytes until EOF are returned.
        """

    async def readall(self) -> Bytes:
        """
        Read and return all the bytes from the stream until EOF, using multiple calls to
        the stream if necessary.
        """

    async def readline(self) -> Bytes:
        """Read a single line of the file, up until the next newline character."""

    async def readlines(self, hint: int = -1, /) -> List[Bytes]:
        """Read all remaining lines into a list of buffers"""

    async def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> int:
        """
        Change the stream position to the given byte _offset_, interpreted relative to
        the position indicated by _whence_, and return the new absolute position. Values
        for _whence_ are:

        - [`os.SEEK_SET`][] or 0: start of the stream (the default); `offset` should be zero or positive
        - [`os.SEEK_CUR`][] or 1: current stream position; `offset` may be negative
        - [`os.SEEK_END`][] or 2: end of the stream; `offset` is usually negative
        """

    def seekable(self) -> bool:
        """Return True if the stream supports random access."""

    async def tell(self) -> int:
        """Return the current stream position."""

def open_writer(
    store: ObjectStore,
    path: str,
    *,
    attributes: Attributes | None = None,
    buffer_size: int = 10 * 1024 * 1024,
    tags: Dict[str, str] | None = None,
    max_concurrency: int = 12,
) -> WritableFile:
    """Open a writable file object at the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Keyword args:
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
    tags: Dict[str, str] | None = None,
    max_concurrency: int = 12,
) -> AsyncWritableFile:
    """Open an **asynchronous** writable file object at the specified location.

    Refer to the documentation for [open_writer][obstore.open_writer].
    """

class WritableFile(AbstractContextManager):
    """A buffered writable file object with synchronous operations.

    This implements a similar interface as a Python
    [`BufferedWriter`][io.BufferedWriter].
    """

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...
    def close(self) -> None:
        """Close the current file."""

    def closed(self) -> bool:
        """Returns `True` if the current file has already been closed.

        Note that this is a method, not an attribute.
        """

    def flush(self) -> None:
        """
        Flushes this output stream, ensuring that all intermediately buffered contents reach their destination.
        """

    def write(self, buffer: bytes | Buffer, /) -> int:
        """
        Write the [bytes-like object](https://docs.python.org/3/glossary.html#term-bytes-like-object), `buffer`, and return the number of bytes written.
        """

class AsyncWritableFile(AbstractAsyncContextManager):
    """A buffered writable file object with **asynchronous** operations."""

    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type, exc_value, traceback) -> None: ...
    async def close(self) -> None:
        """Close the current file."""

    async def closed(self) -> bool:
        """Returns `True` if the current file has already been closed.

        Note that this is an async method, not an attribute.
        """

    async def flush(self) -> None:
        """
        Flushes this output stream, ensuring that all intermediately buffered contents reach their destination.
        """

    async def write(self, buffer: bytes | Buffer, /) -> int:
        """
        Write the [bytes-like object](https://docs.python.org/3/glossary.html#term-bytes-like-object), `buffer`, and return the number of bytes written.
        """
