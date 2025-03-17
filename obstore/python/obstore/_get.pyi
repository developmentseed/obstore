from collections.abc import Sequence
from datetime import datetime
from typing import TypedDict

from ._attributes import Attributes
from ._bytes import Bytes
from ._list import ObjectMeta
from .store import ObjectStore

class OffsetRange(TypedDict):
    """Request all bytes starting from a given byte offset."""

    offset: int
    """The byte offset for the offset range request."""

class SuffixRange(TypedDict):
    """Request up to the last `n` bytes."""

    suffix: int
    """The number of bytes from the suffix to request."""

class GetOptions(TypedDict, total=False):
    """Options for a get request.

    All options are optional.
    """

    if_match: str | None
    """
    Request will succeed if the `ObjectMeta::e_tag` matches
    otherwise returning [`PreconditionError`][obstore.exceptions.PreconditionError].
    See <https://datatracker.ietf.org/doc/html/rfc9110#name-if-match>
    Examples:
    ```text
    If-Match: "xyzzy"
    If-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    If-Match: *
    ```
    """

    if_none_match: str | None
    """
    Request will succeed if the `ObjectMeta::e_tag` does not match
    otherwise returning [`NotModifiedError`][obstore.exceptions.NotModifiedError].
    See <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.2>
    Examples:
    ```text
    If-None-Match: "xyzzy"
    If-None-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    If-None-Match: *
    ```
    """

    if_unmodified_since: datetime | None
    """
    Request will succeed if the object has been modified since
    <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.3>
    """

    if_modified_since: datetime | None
    """
    Request will succeed if the object has not been modified since
    otherwise returning [`PreconditionError`][obstore.exceptions.PreconditionError].
    Some stores, such as S3, will only return `NotModified` for exact
    timestamp matches, instead of for any timestamp greater than or equal.
    <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.4>
    """

    range: tuple[int, int] | list[int] | OffsetRange | SuffixRange
    """
    Request transfer of only the specified range of bytes
    otherwise returning [`NotModifiedError`][obstore.exceptions.NotModifiedError].
    The semantics of this tuple are:
    - `(int, int)`: Request a specific range of bytes `(start, end)`.
        If the given range is zero-length or starts after the end of the object, an
        error will be returned. Additionally, if the range ends after the end of the
        object, the entire remainder of the object will be returned. Otherwise, the
        exact requested range will be returned.
        The `end` offset is _exclusive_.
    - `{"offset": int}`: Request all bytes starting from a given byte offset.
        This is equivalent to `bytes={int}-` as an HTTP header.
    - `{"suffix": int}`: Request the last `int` bytes. Note that here, `int` is _the
        size of the request_, not the byte offset. This is equivalent to `bytes=-{int}`
        as an HTTP header.
    <https://datatracker.ietf.org/doc/html/rfc9110#name-range>
    """

    version: str | None
    """
    Request a particular object version
    """

    head: bool
    """
    Request transfer of no content
    <https://datatracker.ietf.org/doc/html/rfc9110#name-head>
    """

class GetResult:
    """Result for a get request.

    You can materialize the entire buffer by using either `bytes` or `bytes_async`, or
    you can stream the result using `stream`. `__iter__` and `__aiter__` are implemented
    as aliases to `stream`, so you can alternatively call `iter()` or `aiter()` on
    `GetResult` to start an iterator.

    Using as an async iterator:
    ```py
    resp = await obs.get_async(store, path)
    # 5MB chunk size in stream
    stream = resp.stream(min_chunk_size=5 * 1024 * 1024)
    async for buf in stream:
        print(len(buf))
    ```

    Using as a sync iterator:
    ```py
    resp = obs.get(store, path)
    # 20MB chunk size in stream
    stream = resp.stream(min_chunk_size=20 * 1024 * 1024)
    for buf in stream:
        print(len(buf))
    ```

    Note that after calling `bytes`, `bytes_async`, or `stream`, you will no longer be
    able to call other methods on this object, such as the `meta` attribute.
    """

    @property
    def attributes(self) -> Attributes:
        """Additional object attributes.

        This must be accessed _before_ calling `stream`, `bytes`, or `bytes_async`.
        """

    def bytes(self) -> Bytes:
        """Collect the data into a `Bytes` object.

        This implements the Python buffer protocol. You can copy the buffer to Python
        memory by passing to [`bytes`][].
        """

    async def bytes_async(self) -> Bytes:
        """Collect the data into a `Bytes` object.

        This implements the Python buffer protocol. You can copy the buffer to Python
        memory by passing to [`bytes`][].
        """

    @property
    def meta(self) -> ObjectMeta:
        """The ObjectMeta for this object.

        This must be accessed _before_ calling `stream`, `bytes`, or `bytes_async`.
        """

    @property
    def range(self) -> tuple[int, int]:
        """The range of bytes returned by this request.

        Note that this is `(start, stop)` **not** `(start, length)`.

        This must be accessed _before_ calling `stream`, `bytes`, or `bytes_async`.
        """

    def stream(self, min_chunk_size: int = 10 * 1024 * 1024) -> BytesStream:
        r"""Return a chunked stream over the result's bytes.

        Args:
            min_chunk_size: The minimum size in bytes for each chunk in the returned
                `BytesStream`. All chunks except for the last chunk will be at least
                this size. Defaults to 10\*1024\*1024 (10MB).

        Returns:
            A chunked stream

        """

    def __aiter__(self) -> BytesStream:
        """Return a chunked stream over the result's bytes.

        Uses the default (10MB) chunk size.
        """

    def __iter__(self) -> BytesStream:
        """Return a chunked stream over the result's bytes.

        Uses the default (10MB) chunk size.
        """

class BytesStream:
    """An async stream of bytes.

    !!! note "Request timeouts"
        The underlying stream needs to stay alive until the last chunk is polled. If the
        file is large, it may exceed the default timeout of 30 seconds. In this case,
        you may see an error like:

        ```
        GenericError: Generic {
            store: "HTTP",
            source: reqwest::Error {
                kind: Decode,
                source: reqwest::Error {
                    kind: Body,
                    source: TimedOut,
                },
            },
        }
        ```

        To fix this, set the `timeout` parameter in the
        [`client_options`][obstore.store.ClientConfig] passed when creating the store.
    """

    def __aiter__(self) -> BytesStream:
        """Return `Self` as an async iterator."""

    def __iter__(self) -> BytesStream:
        """Return `Self` as an async iterator."""

    # Note: this returns bytes, not Bytes
    async def __anext__(self) -> bytes:
        """Return the next chunk of bytes in the stream."""

    # Note: this returns bytes, not Bytes
    def __next__(self) -> bytes:
        """Return the next chunk of bytes in the stream."""

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
