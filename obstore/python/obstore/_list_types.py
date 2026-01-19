from __future__ import annotations

# ruff: noqa: UP035
from typing import TYPE_CHECKING, Generic, Sequence, TypedDict, TypeVar

if TYPE_CHECKING:
    from datetime import datetime


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
