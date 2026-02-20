from collections.abc import Sequence

from ._bytes import Bytes
from ._get_types import GetOptions, GetResult
from .store import ObjectStore

def get(
    store: ObjectStore,
    path: str,
    *,
    options: GetOptions | None = None,
) -> GetResult:
    """Return the bytes that are stored at the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.
        options: options for accessing the file. Defaults to None.

    Returns:
        GetResult

    """

async def get_async(
    store: ObjectStore,
    path: str,
    *,
    options: GetOptions | None = None,
) -> GetResult:
    """Call `get` asynchronously.

    Refer to the documentation for [get][obstore.get].
    """

def get_range(
    store: ObjectStore,
    path: str,
    *,
    start: int,
    end: int | None = None,
    length: int | None = None,
) -> Bytes:
    """Return the bytes that are stored at the specified location in the given byte range.

    If the given range is zero-length or starts after the end of the object, an error
    will be returned. Additionally, if the range ends after the end of the object, the
    entire remainder of the object will be returned. Otherwise, the exact requested
    range will be returned.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Keyword Args:
        start: The start of the byte range.
        end: The end of the byte range (exclusive). Either `end` or `length` must be non-None.
        length: The number of bytes of the byte range. Either `end` or `length` must be non-None.

    Returns:
        A `Bytes` object implementing the Python buffer protocol, allowing
            zero-copy access to the underlying memory provided by Rust.

    """

async def get_range_async(
    store: ObjectStore,
    path: str,
    *,
    start: int,
    end: int | None = None,
    length: int | None = None,
) -> Bytes:
    """Call `get_range` asynchronously.

    Refer to the documentation for [get_range][obstore.get_range].
    """

def get_ranges(
    store: ObjectStore,
    path: str,
    *,
    starts: Sequence[int],
    ends: Sequence[int] | None = None,
    lengths: Sequence[int] | None = None,
) -> list[Bytes]:
    """Return the bytes stored at the specified location in the given byte ranges.

    To improve performance this will:

    - Transparently combine ranges less than 1MB apart into a single underlying request
    - Make multiple `fetch` requests in parallel (up to maximum of 10)

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Other Args:
        starts: A sequence of `int` where each offset starts.
        ends: A sequence of `int` where each offset ends (exclusive). Either `ends` or `lengths` must be non-None.
        lengths: A sequence of `int` with the number of bytes of each byte range. Either `ends` or `lengths` must be non-None.

    Returns:
        A sequence of `Bytes`, one for each range. This `Bytes` object implements the
            Python buffer protocol, allowing zero-copy access to the underlying memory
            provided by Rust.

    """

async def get_ranges_async(
    store: ObjectStore,
    path: str,
    *,
    starts: Sequence[int],
    ends: Sequence[int] | None = None,
    lengths: Sequence[int] | None = None,
) -> list[Bytes]:
    """Call `get_ranges` asynchronously.

    Refer to the documentation for [get_ranges][obstore.get_ranges].
    """
