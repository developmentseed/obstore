from datetime import datetime
from typing import Generic, List, Literal, TypedDict, TypeVar, overload

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

ListType = TypeVar("ListType")

class ListStream(Generic[ListType]):
    """
    A stream of [ObjectMeta][object_store_rs.ObjectMeta] that can be polled in a sync or
    async fashion.
    """
    def __aiter__(self) -> ListStream[ListType]:
        """Return `Self` as an async iterator."""

    def __iter__(self) -> ListStream[ListType]:
        """Return `Self` as an async iterator."""

    async def collect_async(self) -> ListType:
        """Collect all remaining ObjectMeta objects in the stream."""

    def collect(self) -> ListType:
        """Collect all remaining ObjectMeta objects in the stream."""

    async def __anext__(self) -> ListType:
        """Return the next chunk of ObjectMeta in the stream."""

    def __next__(self) -> ListType:
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

    !!! note
        The order of returned [`ObjectMeta`][object_store_rs.ObjectMeta] is not
        guaranteed

    !!! note
        There is no async version of this method, because `list` is not async under the
        hood, rather it only instantiates a stream, which can be polled in synchronous
        or asynchronous fashion. See [`ListStream`][object_store_rs.ListStream].

    Args:
        store: The ObjectStore instance to use.
        prefix: The prefix within ObjectStore to use for listing. Defaults to None.

    Keyword Args:
        offset: If provided, list all the objects with the given prefix and a location greater than `offset`. Defaults to `None`.
        chunk_size: The number of items to collect per chunk in the returned
            (async) iterator. All chunks except for the last one will have this many items.
        return_arrow: If `True`, return each batch of list items as an Arrow
            `RecordBatch`, not as a list of Python `dict`s. Arrow removes serialization
            overhead between Rust and Python and so this can be significantly faster for
            large list operations. Defaults to `False`.

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
    [list_with_delimiter][object_store_rs.list_with_delimiter].
    """
