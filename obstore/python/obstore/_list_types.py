from __future__ import annotations

# ruff: noqa: UP035
import sys
from typing import TYPE_CHECKING, Generic, Protocol, Sequence, TypedDict, TypeVar

if TYPE_CHECKING:
    from datetime import datetime

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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


# We removed constraints here so that ListStream types work even when arro3-core is not
# installed. https://github.com/developmentseed/obstore/issues/572
ListChunkType = TypeVar("ListChunkType", covariant=True)  # noqa: PLC0105
"""The data structure used for holding list results.

By default, listing APIs return a `list` of [`ObjectMeta`][obstore.ObjectMeta]. However
for improved performance when listing large buckets, you can pass `return_arrow=True`.
Then an [Arrow `RecordBatch`][arro3.core.RecordBatch] will be returned instead, with
columns containing the same information as would be contained in the Python
[`ObjectMeta`][obstore.ObjectMeta].
"""


class ListResult(TypedDict, Generic[ListChunkType]):
    """Result of a list call.

    Includes objects, prefixes (directories) and a token for the next set of results.
    Individual result sets may be limited to 1,000 objects based on the underlying
    object storage's limitations.

    This implements [`obstore.ListResult`][].
    """

    common_prefixes: Sequence[str]
    """Prefixes that are common (like directories)"""

    objects: ListChunkType
    """Object metadata for the listing"""


# Note: the public API exposes a Protocol, not the literal class exported from
# Rust because we don't want users to rely on nominal subtyping.
class ListStream(Protocol[ListChunkType]):
    """A stream of [ObjectMeta][obstore.ObjectMeta] that can be polled in a sync or
    async fashion.

    This implements [`obstore.ListStream`][].
    """  # noqa: D205

    def __aiter__(self) -> Self:
        """Return `Self` as an async iterator."""
        ...

    def __iter__(self) -> Self:
        """Return `Self` as an async iterator."""
        ...

    async def collect_async(self) -> ListChunkType:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """
        ...

    def collect(self) -> ListChunkType:
        """Collect all remaining ObjectMeta objects in the stream.

        This ignores the `chunk_size` parameter from the `list` call and collects all
        remaining data into a single chunk.
        """
        ...

    async def __anext__(self) -> ListChunkType:
        """Return the next chunk of ObjectMeta in the stream."""
        ...

    def __next__(self) -> ListChunkType:
        """Return the next chunk of ObjectMeta in the stream."""
        ...
