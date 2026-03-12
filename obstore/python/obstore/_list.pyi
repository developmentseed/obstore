# ruff: noqa: A001, UP035

from typing import Literal, Sequence, overload

from arro3.core import RecordBatch, Table

from ._list_types import ListResult, ListStream, ObjectMeta
from ._store import ObjectStore

@overload
def list(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    offset: str | None = None,
    chunk_size: int = 50,
) -> ListStream[Sequence[ObjectMeta]]: ...
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
    return_arrow: Literal[False],
) -> ListStream[Sequence[ObjectMeta]]: ...
@overload
def list(
    store: ObjectStore,
    prefix: str | None = None,
    *,
    offset: str | None = None,
    chunk_size: int = 50,
    return_arrow: bool,
) -> ListStream[RecordBatch] | ListStream[Sequence[ObjectMeta]]: ...
def list(  # type: ignore[misc] # docstring in pyi file
    store: ObjectStore,
    prefix: str | None = None,
    *,
    offset: str | None = None,
    chunk_size: int = 50,
    return_arrow: bool = False,
) -> ListStream[RecordBatch] | ListStream[Sequence[ObjectMeta]]:
    """List all the objects with the given prefix.

    Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of
    `foo/bar/x` but not of `foo/bar_baz/x`. List is recursive, i.e. `foo/bar/more/x`
    will be included.

    **Examples**:

    Synchronously iterate through list results:

    ```py
    from obstore.store import MemoryStore

    store = MemoryStore()
    for i in range(100):
        store.put(f"file{i}.txt", b"foo")

    stream = store.list(chunk_size=10)
    for list_result in stream:
        print(list_result[0])
        # {'path': 'file0.txt', 'last_modified': datetime.datetime(2024, 10, 23, 19, 19, 28, 781723, tzinfo=datetime.timezone.utc), 'size': 3, 'e_tag': '0', 'version': None}
        break
    ```

    Asynchronously iterate through list results. Just change `for` to `async for`:

    ```py
    stream = store.list(chunk_size=10)
    async for list_result in stream:
        print(list_result[2])
        # {'path': 'file10.txt', 'last_modified': datetime.datetime(2024, 10, 23, 19, 21, 46, 224725, tzinfo=datetime.timezone.utc), 'size': 3, 'e_tag': '10', 'version': None}
        break
    ```

    Return large list results as [Arrow](https://arrow.apache.org/). This is only a
    performance optimization; it returns the same information in columnar table form.
    This is most useful with large list operations (you may also want to
    increase the `chunk_size` parameter to increase the number of items per emitted
    record batch).

    ```py
    stream = store.list(chunk_size=1000, return_arrow=True)
    # Stream is now an iterable/async iterable of `RecordBatch`es
    for batch in stream:
        print(batch.num_rows) # 100

        # If desired, convert to a pyarrow RecordBatch (zero-copy) with
        # `pyarrow.record_batch(batch)`
        break
    ```

    Collect all list results into a single Arrow `RecordBatch`.

    ```py
    stream = store.list(return_arrow=True)
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
        return_arrow: If `True`, return each batch of list items as an
            [Apache Arrow](https://arrow.apache.org/) `RecordBatch` instead of a list of
            Python `dict`s. This is a performance optimization. Arrow removes
            serialization overhead between Rust and Python and so setting
            `return_arrow=True` can significantly reduce Python interpreter overhead for
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
) -> ListResult[Sequence[ObjectMeta]]: ...
def list_with_delimiter(  # type: ignore[misc] # docstring in pyi file
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: bool = False,
) -> ListResult[Table] | ListResult[Sequence[ObjectMeta]]:
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
        return_arrow: If `True`, return each batch of list items as an
            [Apache Arrow](https://arrow.apache.org/) `RecordBatch` instead of a list of
            Python `dict`s. This is a performance optimization. Arrow removes
            serialization overhead between Rust and Python and so setting
            `return_arrow=True` can significantly reduce Python interpreter overhead for
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
) -> ListResult[Sequence[ObjectMeta]]: ...
async def list_with_delimiter_async(  # type: ignore[misc] # docstring in pyi file
    store: ObjectStore,
    prefix: str | None = None,
    *,
    return_arrow: bool = False,
) -> ListResult[Table] | ListResult[Sequence[ObjectMeta]]:
    """Call `list_with_delimiter` asynchronously.

    Refer to the documentation for
    [list_with_delimiter][obstore.list_with_delimiter].
    """
