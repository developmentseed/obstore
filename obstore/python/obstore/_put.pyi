from pathlib import Path
from typing import IO, Dict, Literal, TypedDict

from ._attributes import Attributes
from .store import ObjectStore

class UpdateVersion(TypedDict, total=False):
    """
    Uniquely identifies a version of an object to update

    Stores will use differing combinations of `e_tag` and `version` to provide
    conditional updates, and it is therefore recommended applications preserve both
    """

    e_tag: str | None
    """The unique identifier for the newly created object.

    <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    """

    version: str | None
    """A version indicator for the newly created object."""

PutMode = Literal["create", "overwrite"] | UpdateVersion
"""Configure preconditions for the put operation

If a string is provided, it must be one of:

- `"overwrite"`: Perform an atomic write operation, overwriting any object present at the provided path.
- `"create"`: Perform an atomic write operation, returning [`AlreadyExistsError`][obstore.exceptions.AlreadyExistsError] if an object already exists at the provided path

If a `dict` is provided, it must meet the criteria of `UpdateVersion`. In this case,
perform an atomic write operation if the current version of the object matches the
provided [`UpdateVersion`][obstore.UpdateVersion], returning
[`PreconditionError`][obstore.exceptions.PreconditionError] otherwise.
"""

class PutResult(TypedDict):
    """
    Result for a put request.
    """

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
    file: IO[bytes] | Path | bytes,
    *,
    attributes: Attributes | None = None,
    tags: Dict[str, str] | None = None,
    mode: PutMode | None = None,
    use_multipart: bool | None = None,
    chunk_size: int = 5 * 1024 * 1024,
    max_concurrency: int = 12,
) -> PutResult:
    """Save the provided bytes to the specified location

    The operation is guaranteed to be atomic, it will either successfully write the
    entirety of `file` to `location`, or fail. No clients should be able to observe a
    partially written object.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore for where to save the file.
        file: The object to upload. Can either be file-like, a `Path` to a local file,
            or a `bytes` object.

    Keyword args:
        mode: Configure the `PutMode` for this operation. If this provided and is not `"overwrite"`, a non-multipart upload will be performed. Defaults to `"overwrite"`.
        attributes: Provide a set of `Attributes`. Defaults to `None`.
        tags: Provide tags for this object. Defaults to `None`.
        use_multipart: Whether to use a multipart upload under the hood. Defaults using a multipart upload if the length of the file is greater than `chunk_size`.
        chunk_size: The size of chunks to use within each part of the multipart upload. Defaults to 5 MB.
        max_concurrency: The maximum number of chunks to upload concurrently. Defaults to 12.
    """

async def put_async(
    store: ObjectStore,
    path: str,
    file: IO[bytes] | Path | bytes,
    *,
    attributes: Attributes | None = None,
    tags: Dict[str, str] | None = None,
    mode: PutMode | None = None,
    use_multipart: bool | None = None,
    chunk_size: int = 5 * 1024 * 1024,
    max_concurrency: int = 12,
) -> PutResult:
    """Call `put` asynchronously.

    Refer to the documentation for [put][obstore.put].
    """
