from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypedDict, TypeVar

from arro3.core import RecordBatch, Table

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


ListChunkType = TypeVar("ListChunkType", list[ObjectMeta], RecordBatch, Table)
"""The data structure used for holding list results.

By default, listing APIs return a `list` of [`ObjectMeta`][obstore.ObjectMeta]. However
for improved performance when listing large buckets, you can pass `return_arrow=True`.
Then an Arrow `RecordBatch` will be returned instead.
"""


class ListResult(TypedDict, Generic[ListChunkType]):
    """Result of a list call.

    Includes objects, prefixes (directories) and a token for the next set of results.
    Individual result sets may be limited to 1,000 objects based on the underlying
    object storage's limitations.
    """

    common_prefixes: list[str]
    """Prefixes that are common (like directories)"""

    objects: ListChunkType
    """Object metadata for the listing"""
