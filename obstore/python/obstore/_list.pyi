# ruff: noqa: UP006
# ruff: noqa: UP035
# Use `list` instead of `List` for type annotation
# `typing.List` is deprecated, use `list` instead
# ruff: noqa: A001
# Variable `list` is shadowing a Python builtinRuff

from typing import Generic, List, Literal, Self, overload

from arro3.core import RecordBatch, Table

from .__list import ListChunkType, ListResult, ObjectMeta
from .store import ObjectStore

class ListStream(Generic[ListChunkType]):
    """A stream of [ObjectMeta][obstore.ObjectMeta] that can be polled in a sync or
    async fashion.
    """  # noqa: D205

    def __aiter__(self) -> Self:
        """Return `Self` as an async iterator."""

    def __iter__(self) -> Self:
        """Return `Self` as an async iterator."""

    async def collect_async(self) -> ListChunkType:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """

    def collect(self) -> ListChunkType:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """

    async def __anext__(self) -> ListChunkType:
        """Return the next chunk of ObjectMeta in the stream."""

    def __next__(self) -> ListChunkType:
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
    """List all the objects with the given prefix.

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

@overload
def list_with_delimiter(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: Literal[True],
) -> ListResult[Table]: ...
@overload
def list_with_delimiter(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: Literal[False] = False,
) -> ListResult[List[ObjectMeta]]: ...
def list_with_delimiter(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: bool = False,
) -> ListResult[Table] | ListResult[List[ObjectMeta]]:
    """List objects with the given prefix and an implementation specific
    delimiter.

    Returns common prefixes (directories) in addition to object
    metadata.

    Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of
    `foo/bar/x` but not of `foo/bar_baz/x`. This list is not recursive, i.e. `foo/bar/more/x` will **not** be included.

    !!! note
        Any prefix supplied to this `prefix` parameter will **not** be stripped off the
        paths in the result.

    Args:
        store: The ObjectStore instance to use.
        prefix: The prefix within ObjectStore to use for listing. Defaults to None.

    Keyword Args:
        return_arrow: If `True`, return list results as an Arrow
            `Table`, not as a list of Python `dict`s. Arrow removes serialization
            overhead between Rust and Python and so this can be significantly faster for
            large list operations. Defaults to `False`.

            If this is `True`, the `arro3-core` Python package must be installed.


    Returns:
        ListResult

    """  # noqa: D205

@overload
async def list_with_delimiter_async(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: Literal[True],
) -> ListResult[Table]: ...
@overload
async def list_with_delimiter_async(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: Literal[False] = False,
) -> ListResult[List[ObjectMeta]]: ...
async def list_with_delimiter_async(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: bool = False,
) -> ListResult[Table] | ListResult[List[ObjectMeta]]:
    """Call `list_with_delimiter` asynchronously.

    Refer to the documentation for
    [list_with_delimiter][obstore.list_with_delimiter].
    """
