import sys
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Self

from ._attributes import Attributes
from ._bytes import Bytes
from ._list import ObjectMeta
from ._store import ObjectStore

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    from typing_extensions import Buffer

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

class ReadableFile:
    """A synchronous-buffered reader that implements a similar interface as a Python
    [`BufferedReader`][io.BufferedReader].

    Internally this maintains a buffer of the requested size, and uses
    [`get_range`][obstore.get_range] to populate its internal buffer once depleted. This
    buffer is cleared on seek.

    Whilst simple, this interface will typically be outperformed by the native `obstore`
    methods that better map to the network APIs. This is because most object stores have
    very [high first-byte latencies], on the order of 100-200ms, and so avoiding
    unnecessary round-trips is critical to throughput.

    Systems looking to sequentially scan a file should instead consider using
    [`get`][obstore.get], or [`get_range`][obstore.get_range] to read a particular
    range.

    Systems looking to read multiple ranges of a file should instead consider using
    [`get_ranges`][obstore.get_ranges], which will optimise the vectored IO.

    [high first-byte latencies]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
    """  # noqa: D205

    def close(self) -> None:
        """Close the current file.

        This is currently a no-op.
        """

    @property
    def meta(self) -> ObjectMeta:
        """Access the metadata of the underlying file."""

    def read(self, size: int | None = None, /) -> Bytes:
        """Read up to `size` bytes from the object and return them.

        As a convenience, if size is unspecified or `None`, all bytes until EOF are
        returned.
        """

    def readall(self) -> Bytes:
        """Read and return all the bytes from the stream until EOF."""

    def readline(self) -> Bytes:
        """Read a single line of the file, up until the next newline character."""

    def readlines(self, hint: int = -1, /) -> list[Bytes]:
        """Read all remaining lines into a list of buffers."""

    def seek(self, offset: int, whence: int = ..., /) -> int:
        """Change the stream position.

        Change the stream position to the given byte `offset`, interpreted relative to
        the position indicated by `whence`, and return the new absolute position. Values
        for `whence` are:

        - [`os.SEEK_SET`][] or 0: start of the stream (the default); `offset` should be zero or positive
        - [`os.SEEK_CUR`][] or 1: current stream position; `offset` may be negative
        - [`os.SEEK_END`][] or 2: end of the stream; `offset` is usually negative
        """

    def seekable(self) -> bool:
        """Return True if the stream supports random access."""

    @property
    def size(self) -> int:
        """The size in bytes of the object."""

    def tell(self) -> int:
        """Return the current stream position."""

class AsyncReadableFile:
    """An async-buffered reader that implements a similar interface as a Python
    [`BufferedReader`][io.BufferedReader].

    Internally this maintains a buffer of the requested size, and uses
    [`get_range`][obstore.get_range] to populate its internal buffer once depleted. This
    buffer is cleared on seek.

    Whilst simple, this interface will typically be outperformed by the native `obstore`
    methods that better map to the network APIs. This is because most object stores have
    very [high first-byte latencies], on the order of 100-200ms, and so avoiding
    unnecessary round-trips is critical to throughput.

    Systems looking to sequentially scan a file should instead consider using
    [`get`][obstore.get], or [`get_range`][obstore.get_range] to read a particular
    range.

    Systems looking to read multiple ranges of a file should instead consider using
    [`get_ranges`][obstore.get_ranges], which will optimise the vectored IO.

    [high first-byte latencies]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
    """  # noqa: D205

    def close(self) -> None:
        """Close the current file.

        This is currently a no-op.
        """

    @property
    def meta(self) -> ObjectMeta:
        """Access the metadata of the underlying file."""

    async def read(self, size: int | None = None, /) -> Bytes:
        """Read up to `size` bytes from the object and return them.

        As a convenience, if size is unspecified or `None`, all bytes until EOF are
        returned.
        """

    async def readall(self) -> Bytes:
        """Read and return all the bytes from the stream until EOF."""

    async def readline(self) -> Bytes:
        """Read a single line of the file, up until the next newline character."""

    async def readlines(self, hint: int = -1, /) -> list[Bytes]:
        """Read all remaining lines into a list of buffers."""

    async def seek(self, offset: int, whence: int = ..., /) -> int:
        """Change the stream position.

        Change the stream position to the given byte `offset`, interpreted relative to
        the position indicated by `whence`, and return the new absolute position. Values
        for `whence` are:

        - [`os.SEEK_SET`][] or 0: start of the stream (the default); `offset` should be zero or positive
        - [`os.SEEK_CUR`][] or 1: current stream position; `offset` may be negative
        - [`os.SEEK_END`][] or 2: end of the stream; `offset` is usually negative
        """

    def seekable(self) -> bool:
        """Return True if the stream supports random access."""

    @property
    def size(self) -> int:
        """The size in bytes of the object."""

    async def tell(self) -> int:
        """Return the current stream position."""

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

class WritableFile(AbstractContextManager):
    """A buffered writable file object with synchronous operations.

    This implements a similar interface as a Python
    [`BufferedWriter`][io.BufferedWriter].
    """

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...  # noqa: ANN001
    def close(self) -> None:
        """Close the current file."""

    def closed(self) -> bool:
        """Check whether this file has been closed.

        Note that this is a method, not an attribute.
        """

    def flush(self) -> None:
        """Flushes this output stream, ensuring that all intermediately buffered contents reach their destination."""

    def write(self, buffer: bytes | Buffer, /) -> int:
        """Write the [bytes-like object](https://docs.python.org/3/glossary.html#term-bytes-like-object), `buffer`, and return the number of bytes written."""

class AsyncWritableFile(AbstractAsyncContextManager):
    """A buffered writable file object with **asynchronous** operations."""

    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type, exc_value, traceback) -> None: ...  # noqa: ANN001
    async def close(self) -> None:
        """Close the current file."""

    async def closed(self) -> bool:
        """Check whether this file has been closed.

        Note that this is an async method, not an attribute.
        """

    async def flush(self) -> None:
        """Flushes this output stream, ensuring that all intermediately buffered contents reach their destination."""

    async def write(self, buffer: bytes | Buffer, /) -> int:
        """Write the [bytes-like object](https://docs.python.org/3/glossary.html#term-bytes-like-object), `buffer`, and return the number of bytes written."""
