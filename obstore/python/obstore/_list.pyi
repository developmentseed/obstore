from datetime import datetime
from typing import Generic, List, Literal, Self, TypedDict, TypeVar, overload

from arro3.core import RecordBatch

from .store import ObjectStore

class ObjectMeta(TypedDict):
    """The metadata that describes an object."""

    path: str
    """The full path to the object"""

    last_modified: datetime
    """The last modified time"""

    size: int
    """The size in bytes of the object"""

    e_tag: str | None
    """The unique identifier for the object

    <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    """

    version: str | None
    """A version indicator for this object"""

class ListResult(TypedDict):
    """
    Result of a list call that includes objects, prefixes (directories) and a token for
    the next set of results. Individual result sets may be limited to 1,000 objects
    based on the underlying object storage's limitations.
    """

    common_prefixes: List[str]
    """Prefixes that are common (like directories)"""

    objects: List[ObjectMeta]
    """Object metadata for the listing"""

ChunkType = TypeVar("ChunkType", List[ObjectMeta], RecordBatch)

class ListStream(Generic[ChunkType]):
    """
    A stream of [ObjectMeta][obstore.ObjectMeta] that can be polled in a sync or
    async fashion.
    """
    def __aiter__(self) -> Self:
        """Return `Self` as an async iterator."""

    def __iter__(self) -> Self:
        """Return `Self` as an async iterator."""

    async def collect_async(self) -> ChunkType:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """

    def collect(self) -> ChunkType:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """

    async def __anext__(self) -> ChunkType:
        """Return the next chunk of ObjectMeta in the stream."""

    def __next__(self) -> ChunkType:
        """Return the next chunk of ObjectMeta in the stream."""

@overload
def list(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    offset: str | None = None,
    chunk_size: int = 50,
    return_arrow: Literal[True],
) -> ListStream[RecordBatch]: ...
@overload
def list(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    offset: str | None = None,
    chunk_size: int = 50,
    return_arrow: Literal[False] = False,
) -> ListStream[List[ObjectMeta]]: ...
def list(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    offset: str | None = None,
    chunk_size: int = 50,
    return_arrow: bool = False,
) -> ListStream[RecordBatch] | ListStream[List[ObjectMeta]]:
    """
    List all the objects with the given prefix.

    Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of
    `foo/bar/x` but not of `foo/bar_baz/x`. List is recursive, i.e. `foo/bar/more/x`
    will be included.

    **Examples**:

    Synchronously iterate through list results:

    ```py
    import obstore as obs
    from obstore.store import MemoryStore

    store = MemoryStore()
    for i in range(100):
        obs.put(store, f"file{i}.txt", b"foo")

    stream = obs.list(store, chunk_size=10)
    for list_result in stream:
        print(list_result[0])
        # {'path': 'file0.txt', 'last_modified': datetime.datetime(2024, 10, 23, 19, 19, 28, 781723, tzinfo=datetime.timezone.utc), 'size': 3, 'e_tag': '0', 'version': None}
        break
    ```

    Asynchronously iterate through list results. Just change `for` to `async for`:

    ```py
    stream = obs.list(store, chunk_size=10)
    async for list_result in stream:
        print(list_result[2])
        # {'path': 'file10.txt', 'last_modified': datetime.datetime(2024, 10, 23, 19, 21, 46, 224725, tzinfo=datetime.timezone.utc), 'size': 3, 'e_tag': '10', 'version': None}
        break
    ```

    Return large list results as [Arrow](https://arrow.apache.org/). This is most useful
    with large list operations. In this case you may want to increase the `chunk_size`
    parameter.

    ```py
    stream = obs.list(store, chunk_size=1000, return_arrow=True)
    # Stream is now an iterable/async iterable of `RecordBatch`es
    for batch in stream:
        print(batch.num_rows) # 100

        # If desired, convert to a pyarrow RecordBatch (zero-copy) with
        # `pyarrow.record_batch(batch)`
        break
    ```

    Collect all list results into a single Arrow `RecordBatch`.

    ```py
    stream = obs.list(store, return_arrow=True)
    batch = stream.collect()
    ```

    !!! note
        The order of returned [`ObjectMeta`][obstore.ObjectMeta] is not
        guaranteed

    !!! note
        There is no async version of this method, because `list` is not async under the
        hood, rather it only instantiates a stream, which can be polled in synchronous
        or asynchronous fashion. See [`ListStream`][obstore.ListStream].

    Args:
        store: The ObjectStore instance to use.
        prefix: The prefix within ObjectStore to use for listing. Defaults to None.

    Keyword Args:
        offset: If provided, list all the objects with the given prefix and a location greater than `offset`. Defaults to `None`.
        chunk_size: The number of items to collect per chunk in the returned
            (async) iterator. All chunks except for the last one will have this many
            items. This is ignored in the
            [`collect`][obstore.ListStream.collect] and
            [`collect_async`][obstore.ListStream.collect_async] methods of
            `ListStream`.
        return_arrow: If `True`, return each batch of list items as an Arrow
            `RecordBatch`, not as a list of Python `dict`s. Arrow removes serialization
            overhead between Rust and Python and so this can be significantly faster for
            large list operations. Defaults to `False`.

            If this is `True`, the `arro3-core` Python package must be installed.

    Returns:
        A ListStream, which you can iterate through to access list results.
    """

def list_with_delimiter(store: ObjectStore, prefix: str | None = None) -> ListResult:
    """
    List objects with the given prefix and an implementation specific
    delimiter. Returns common prefixes (directories) in addition to object
    metadata.

    Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of
    `foo/bar/x` but not of `foo/bar_baz/x`. List is not recursive, i.e. `foo/bar/more/x`
    will not be included.

    Args:
        store: The ObjectStore instance to use.
        prefix: The prefix within ObjectStore to use for listing. Defaults to None.

    Returns:
        ListResult
    """

async def list_with_delimiter_async(
    store: ObjectStore, prefix: str | None = None
) -> ListResult:
    """Call `list_with_delimiter` asynchronously.

    Refer to the documentation for
    [list_with_delimiter][obstore.list_with_delimiter].
    """
