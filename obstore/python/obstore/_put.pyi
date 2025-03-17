import sys
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from pathlib import Path
from typing import IO, Literal, TypedDict

from ._attributes import Attributes
from .store import ObjectStore

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    from typing_extensions import Buffer

class UpdateVersion(TypedDict, total=False):
    """Uniquely identifies a version of an object to update.

    Stores will use differing combinations of `e_tag` and `version` to provide
    conditional updates, and it is therefore recommended applications preserve both
    """

    e_tag: str | None
    """The unique identifier for the newly created object.
    <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    """

    version: str | None
    """A version indicator for the newly created object."""

PutMode: TypeAlias = Literal["create", "overwrite"] | UpdateVersion
"""Configure preconditions for the put operation
There are three modes:
- Overwrite: Perform an atomic write operation, overwriting any object present at the
  provided path.
- Create: Perform an atomic write operation, returning
  [`AlreadyExistsError`][obstore.exceptions.AlreadyExistsError] if an object already
  exists at the provided path.
- Update: Perform an atomic write operation if the current version of the object matches
  the provided [`UpdateVersion`][obstore.UpdateVersion], returning
  [`PreconditionError`][obstore.exceptions.PreconditionError] otherwise.
If a string is provided, it must be one of:
- `"overwrite"`
- `"create"`
If a `dict` is provided, it must meet the criteria of
[`UpdateVersion`][obstore.UpdateVersion].
"""

class PutResult(TypedDict):
    """Result for a put request."""

    e_tag: str | None
    """
    The unique identifier for the newly created object
    <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    """

    version: str | None
    """A version indicator for the newly created object."""

def put(
    store: ObjectStore,
    path: str,
    file: IO[bytes] | Path | bytes | Buffer | Iterator[Buffer] | Iterable[Buffer],
    *,
    attributes: Attributes | None = None,
    tags: dict[str, str] | None = None,
    mode: PutMode | None = None,
    use_multipart: bool | None = None,
    chunk_size: int = ...,
    max_concurrency: int = 12,
) -> PutResult:
    """Save the provided bytes to the specified location.

    The operation is guaranteed to be atomic, it will either successfully write the
    entirety of `file` to `location`, or fail. No clients should be able to observe a
    partially written object.

    !!! warning "Aborted multipart uploads"
        This function will automatically use [multipart
        uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
        under the hood for large file objects (whenever the length of the file is
        greater than `chunk_size`) or for iterable or async iterable input.

        Multipart uploads have a variety of advantages, including performance and
        reliability.

        However, aborted or incomplete multipart uploads can leave partial content in a
        hidden state in your bucket, silently adding to your storage costs. It's
        recommended to configure lifecycle rules to automatically delete aborted
        multipart uploads. See
        [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html)
        for the AWS S3 documentation, for example.

        You can turn off multipart uploads by passing `use_multipart=False`.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore for where to save the file.
        file: The object to upload. Supports various input:

            - A file-like object opened in binary read mode
            - A [`Path`][pathlib.Path] to a local file
            - A [`bytes`][] object.
            - Any object implementing the Python [buffer
              protocol](https://docs.python.org/3/c-api/buffer.html) (includes `bytes`
              but also `memoryview`, numpy arrays, and more).
            - An iterator or iterable of objects implementing the Python buffer
              protocol.

    Keyword Args:
        mode: Configure the [`PutMode`][obstore.PutMode] for this operation. Refer to the [`PutMode`][obstore.PutMode] docstring for more information.

            If this provided and is not `"overwrite"`, a non-multipart upload will be performed. Defaults to `"overwrite"`.
        attributes: Provide a set of `Attributes`. Defaults to `None`.
        tags: Provide tags for this object. Defaults to `None`.
        use_multipart: Whether to use a multipart upload under the hood. Defaults using a multipart upload if the length of the file is greater than `chunk_size`. When `use_multipart` is `False`, the entire input will be materialized in memory as part of the upload.
        chunk_size: The size of chunks to use within each part of the multipart upload. Defaults to 5 MB.
        max_concurrency: The maximum number of chunks to upload concurrently. Defaults to 12.

    """

async def put_async(
    store: ObjectStore,
    path: str,
    file: IO[bytes]
    | Path
    | bytes
    | Buffer
    | AsyncIterator[Buffer]
    | AsyncIterable[Buffer]
    | Iterator[Buffer]
    | Iterable[Buffer],
    *,
    attributes: Attributes | None = None,
    tags: dict[str, str] | None = None,
    mode: PutMode | None = None,
    use_multipart: bool | None = None,
    chunk_size: int = ...,
    max_concurrency: int = 12,
) -> PutResult:
    """Call `put` asynchronously.

    Refer to the documentation for [`put`][obstore.put]. In addition to what the
    synchronous `put` allows for the `file` parameter, this **also supports an async
    iterator or iterable** of objects implementing the Python buffer protocol.

    This means, for example, you can pass the result of `get_async` directly to
    `put_async`, and the request will be streamed through Python during the put
    operation:

    ```py
    import obstore as obs

    # This only constructs the stream, it doesn't materialize the data in memory
    resp = await obs.get_async(store1, path1)
    # A streaming upload is created to copy the file to path2
    await obs.put_async(store2, path2)
    ```
    """
