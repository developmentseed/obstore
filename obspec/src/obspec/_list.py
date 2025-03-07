from __future__ import annotations

from typing import Generic, Literal, Protocol, Self, TypedDict, TypeVar, overload

from ._meta import ObjectMeta
from .arrow import ArrowArrayExportable, ArrowStreamExportable

ListChunkType_co = TypeVar(
    "ListChunkType_co",
    list[ObjectMeta],
    ArrowArrayExportable,
    ArrowStreamExportable,
    covariant=True,
)
"""The data structure used for holding list results.

By default, listing APIs return a `list` of [`ObjectMeta`][obspec.ObjectMeta]. However
for improved performance when listing large buckets, you can pass `return_arrow=True`.
Then an Arrow `RecordBatch` will be returned instead.
"""


class ListResult(TypedDict, Generic[ListChunkType_co]):
    """Result of a list call.

    Includes objects, prefixes (directories) and a token for the next set of results.
    Individual result sets may be limited to 1,000 objects based on the underlying
    object storage's limitations.
    """

    common_prefixes: list[str]
    """Prefixes that are common (like directories)"""

    objects: ListChunkType_co
    """Object metadata for the listing"""


class ListStream(Protocol[ListChunkType_co]):
    """A stream of [ObjectMeta][obspec.ObjectMeta] that can be polled in a sync or
    async fashion.
    """  # noqa: D205

    def __aiter__(self) -> Self:
        """Return `Self` as an async iterator."""
        ...

    def __iter__(self) -> Self:
        """Return `Self` as an async iterator."""
        ...

    async def collect_async(self) -> ListChunkType_co:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """
        ...

    def collect(self) -> ListChunkType_co:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """
        ...

    async def __anext__(self) -> ListChunkType_co:
        """Return the next chunk of ObjectMeta in the stream."""
        ...

    def __next__(self) -> ListChunkType_co:
        """Return the next chunk of ObjectMeta in the stream."""
        ...


class List(Protocol[ListChunkType_co]):
    @overload
    def list(
        self,
        prefix: str | None = None,
        *,
        offset: str | None = None,
        chunk_size: int = 50,
        return_arrow: Literal[True],
    ) -> ListStream[ArrowArrayExportable]: ...
    @overload
    def list(
        self,
        prefix: str | None = None,
        *,
        offset: str | None = None,
        chunk_size: int = 50,
        return_arrow: Literal[False] = False,
    ) -> ListStream[list[ObjectMeta]]: ...
    def list(
        self,
        prefix: str | None = None,
        *,
        offset: str | None = None,
        chunk_size: int = 50,
        return_arrow: bool = False,
    ) -> ListStream[ArrowArrayExportable] | ListStream[list[ObjectMeta]]:
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

        Return large list results as [Arrow](https://arrow.apache.org/). This is most
        useful with large list operations. In this case you may want to increase the
        `chunk_size` parameter.

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
            The order of returned [`ObjectMeta`][obspec.ObjectMeta] is not
            guaranteed

        !!! note
            There is no async version of this method, because `list` is not async under
            the hood, rather it only instantiates a stream, which can be polled in
            synchronous or asynchronous fashion. See [`ListStream`][obspec.ListStream].

        Args:
            prefix: The prefix within ObjectStore to use for listing. Defaults to None.

        Keyword Args:
            offset: If provided, list all the objects with the given prefix and a
                location greater than `offset`. Defaults to `None`.
            chunk_size: The number of items to collect per chunk in the returned
                (async) iterator. All chunks except for the last one will have this many
                items. This is ignored in the
                [`collect`][obspec.ListStream.collect] and
                [`collect_async`][obspec.ListStream.collect_async] methods of
                `ListStream`.
            return_arrow: If `True`, return each batch of list items as an Arrow
                `RecordBatch`, not as a list of Python `dict`s. Arrow removes
                serialization overhead between Rust and Python and so this can be
                significantly faster for large list operations. Defaults to `False`.

                If this is `True`, the `arro3-core` Python package must be installed.

        Returns:
            A ListStream, which you can iterate through to access list results.

        """  # noqa: E501
        ...


class ListWithDelimiter(Protocol[ListChunkType_co]):
    @overload
    def list_with_delimiter(
        self,
        prefix: str | None = None,
        *,
        return_arrow: Literal[True],
    ) -> ListResult[ArrowStreamExportable]: ...
    @overload
    def list_with_delimiter(
        self,
        prefix: str | None = None,
        *,
        return_arrow: Literal[False] = False,
    ) -> ListResult[list[ObjectMeta]]: ...
    def list_with_delimiter(
        self,
        prefix: str | None = None,
        *,
        return_arrow: bool = False,
    ) -> ListResult[ArrowStreamExportable] | ListResult[list[ObjectMeta]]:
        """List objects with the given prefix and an implementation specific
        delimiter.

        Returns common prefixes (directories) in addition to object
        metadata.

        Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of
        `foo/bar/x` but not of `foo/bar_baz/x`. This list is not recursive, i.e.
        `foo/bar/more/x` will **not** be included.

        !!! note

            Any prefix supplied to this `prefix` parameter will **not** be stripped off
            the paths in the result.

        Args:
            prefix: The prefix within ObjectStore to use for listing. Defaults to None.

        Keyword Args:
            return_arrow: If `True`, return list results as an Arrow
                `Table`, not as a list of Python `dict`s. Arrow removes serialization
                overhead between Rust and Python and so this can be significantly faster
                for large list operations. Defaults to `False`.


        Returns:
            ListResult

        """  # noqa: D205
        ...


class ListWithDelimiterAsync(Protocol[ListChunkType_co]):
    @overload
    async def list_with_delimiter_async(
        self,
        prefix: str | None = None,
        *,
        return_arrow: Literal[True],
    ) -> ListResult[ArrowStreamExportable]: ...
    @overload
    async def list_with_delimiter_async(
        self,
        prefix: str | None = None,
        *,
        return_arrow: Literal[False] = False,
    ) -> ListResult[list[ObjectMeta]]: ...
    async def list_with_delimiter_async(
        self,
        prefix: str | None = None,
        *,
        return_arrow: bool = False,
    ) -> ListResult[ArrowStreamExportable] | ListResult[list[ObjectMeta]]:
        """Call `list_with_delimiter` asynchronously.

        Refer to the documentation for
        [ListWithDelimiter][obspec.ListWithDelimiter].
        """
        ...
