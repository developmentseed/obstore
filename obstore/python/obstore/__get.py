"""Type hints for get requests, that are actually Python classes."""

from datetime import datetime
from typing import TypedDict


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
