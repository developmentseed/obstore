import os
from typing import List

from ._bytes import Bytes
from .store import ObjectStore

def open(store: ObjectStore, path: str) -> ReadableFile:
    """Open a file object from the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Returns:
        ReadableFile
    """

async def open_async(store: ObjectStore, path: str) -> AsyncReadableFile:
    """Call `open` asynchronously, returning a file object with asynchronous operations.

    Refer to the documentation for [open][obstore.open].
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
