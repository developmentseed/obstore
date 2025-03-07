"""Interface for constructing cloud storage classes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypeAlias, overload

import obstore as obs
from obstore._store import (
    AzureAccessKey,  # noqa: F401
    AzureBearerToken,  # noqa: F401
    AzureConfig,  # noqa: F401
    AzureCredential,  # noqa: F401
    AzureCredentialProvider,  # noqa: F401
    AzureSASToken,  # noqa: F401
    BackoffConfig,  # noqa: F401
    ClientConfig,  # noqa: F401
    GCSConfig,  # noqa: F401
    GCSCredential,  # noqa: F401
    GCSCredentialProvider,  # noqa: F401
    RetryConfig,  # noqa: F401
    S3Config,  # noqa: F401
    S3Credential,  # noqa: F401
    S3CredentialProvider,  # noqa: F401
    from_url,
)
from obstore._store import AzureStore as _AzureStore
from obstore._store import GCSStore as _GCSStore
from obstore._store import HTTPStore as _HTTPStore
from obstore._store import LocalStore as _LocalStore
from obstore._store import MemoryStore as _MemoryStore
from obstore._store import S3Store as _S3Store

if TYPE_CHECKING:
    import sys
    from collections.abc import (
        AsyncIterable,
        AsyncIterator,
        Iterable,
        Iterator,
        Sequence,
    )
    from pathlib import Path
    from typing import IO, Literal

    from arro3.core import RecordBatch, Table

    from obstore import (
        Attributes,
        Bytes,
        GetOptions,
        GetResult,
        ListResult,
        ListStream,
        ObjectMeta,
        PutMode,
        PutResult,
    )

    if sys.version_info >= (3, 12):
        from collections.abc import Buffer
    else:
        from typing_extensions import Buffer


__all__ = [
    "AzureStore",
    "GCSStore",
    "HTTPStore",
    "LocalStore",
    "MemoryStore",
    "S3Store",
    "from_url",
]


class _ObjectStoreMixin:
    def copy(self, from_: str, to: str, *, overwrite: bool = True) -> None:
        """Copy an object from one path to another in the same object store.

        Refer to the documentation for [copy][obstore.copy].
        """
        return obs.copy(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            from_,
            to,
            overwrite=overwrite,
        )

    async def copy_async(
        self,
        from_: str,
        to: str,
        *,
        overwrite: bool = True,
    ) -> None:
        """Call `copy` asynchronously.

        Refer to the documentation for [copy][obstore.copy].
        """
        return await obs.copy_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            from_,
            to,
            overwrite=overwrite,
        )

    def delete(self, paths: str | Sequence[str]) -> None:
        """Delete the object at the specified location(s).

        Refer to the documentation for [delete][obstore.delete].
        """
        return obs.delete(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            paths,
        )

    async def delete_async(self, paths: str | Sequence[str]) -> None:
        """Call `delete` asynchronously.

        Refer to the documentation for [delete][obstore.delete].
        """
        return await obs.delete_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            paths,
        )

    def get(
        self,
        path: str,
        *,
        options: GetOptions | None = None,
    ) -> GetResult:
        """Return the bytes that are stored at the specified location.

        Refer to the documentation for [get][obstore.get].
        """
        return obs.get(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            options=options,
        )

    async def get_async(
        self,
        path: str,
        *,
        options: GetOptions | None = None,
    ) -> GetResult:
        """Call `get` asynchronously.

        Refer to the documentation for [get][obstore.get].
        """
        return await obs.get_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            options=options,
        )

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Bytes:
        """Return the bytes stored at the specified location in the given byte range.

        Refer to the documentation for [get_range][obstore.get_range].
        """
        return obs.get_range(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            start=start,
            end=end,
            length=length,
        )

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Bytes:
        """Call `get_range` asynchronously.

        Refer to the documentation for [get_range][obstore.get_range].
        """
        return await obs.get_range_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            start=start,
            end=end,
            length=length,
        )

    def get_ranges(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> list[Bytes]:
        """Return the bytes stored at the specified location in the given byte ranges.

        Refer to the documentation for [get_ranges][obstore.get_ranges].
        """
        return obs.get_ranges(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            starts=starts,
            ends=ends,
            lengths=lengths,
        )

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> list[Bytes]:
        """Call `get_ranges` asynchronously.

        Refer to the documentation for [get_ranges][obstore.get_ranges].
        """
        return await obs.get_ranges_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            starts=starts,
            ends=ends,
            lengths=lengths,
        )

    def head(self, path: str) -> ObjectMeta:
        """Return the metadata for the specified location.

        Refer to the documentation for [head][obstore.head].
        """
        return obs.head(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
        )

    async def head_async(self, path: str) -> ObjectMeta:
        """Call `head` asynchronously.

        Refer to the documentation for [head_async][obstore.head_async].
        """
        return await obs.head_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
        )

    @overload
    def list(
        self,
        prefix: str | None = None,
        *,
        offset: str | None = None,
        chunk_size: int = 50,
        return_arrow: Literal[True],
    ) -> ListStream[RecordBatch]: ...
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
    ) -> ListStream[RecordBatch] | ListStream[list[ObjectMeta]]:
        """List all the objects with the given prefix.

        Refer to the documentation for [list][obstore.list].
        """
        return obs.list(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            prefix,
            offset=offset,
            chunk_size=chunk_size,
            return_arrow=return_arrow,
        )

    @overload
    def list_with_delimiter(
        self,
        prefix: str | None = None,
        *,
        return_arrow: Literal[True],
    ) -> ListResult[Table]: ...
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
    ) -> ListResult[Table] | ListResult[list[ObjectMeta]]:
        """List objects with the given prefix and an implementation specific
        delimiter.

        Refer to the documentation for
        [list_with_delimiter][obstore.list_with_delimiter].
        """  # noqa: D205
        return obs.list_with_delimiter(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            prefix,
            return_arrow=return_arrow,
        )

    @overload
    async def list_with_delimiter_async(
        self,
        prefix: str | None = None,
        *,
        return_arrow: Literal[True],
    ) -> ListResult[Table]: ...
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
    ) -> ListResult[Table] | ListResult[list[ObjectMeta]]:
        """Call `list_with_delimiter` asynchronously.

        Refer to the documentation for
        [list_with_delimiter][obstore.list_with_delimiter].
        """
        return await obs.list_with_delimiter_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            prefix,
            return_arrow=return_arrow,
        )

    def put(  # noqa: PLR0913
        self,
        path: str,
        file: IO[bytes] | Path | bytes | Buffer | Iterator[Buffer] | Iterable[Buffer],
        *,
        attributes: Attributes | None = None,
        tags: dict[str, str] | None = None,
        mode: PutMode | None = None,
        use_multipart: bool | None = None,
        chunk_size: int = 5 * 1024 * 1024,
        max_concurrency: int = 12,
    ) -> PutResult:
        """Save the provided bytes to the specified location.

        Refer to the documentation for [put][obstore.put].
        """
        return obs.put(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            file,
            attributes=attributes,
            tags=tags,
            mode=mode,
            use_multipart=use_multipart,
            chunk_size=chunk_size,
            max_concurrency=max_concurrency,
        )

    async def put_async(  # noqa: PLR0913
        self,
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
        chunk_size: int = 5 * 1024 * 1024,
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
        return await obs.put_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            path,
            file,
            attributes=attributes,
            tags=tags,
            mode=mode,
            use_multipart=use_multipart,
            chunk_size=chunk_size,
            max_concurrency=max_concurrency,
        )

    def rename(self, from_: str, to: str, *, overwrite: bool = True) -> None:
        """Move an object from one path to another in the same object store.

        Refer to the documentation for [rename][obstore.rename].
        """
        return obs.rename(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            from_,
            to,
            overwrite=overwrite,
        )

    async def rename_async(
        self,
        from_: str,
        to: str,
        *,
        overwrite: bool = True,
    ) -> None:
        """Call `rename` asynchronously.

        Refer to the documentation for [rename][obstore.rename].
        """
        return await obs.rename_async(
            self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
            from_,
            to,
            overwrite=overwrite,
        )


class AzureStore(_ObjectStoreMixin, _AzureStore):
    """Interface to a Microsoft Azure Blob Storage container.

    All constructors will check for environment variables. Refer to
    [`AzureConfig`][obstore.store.AzureConfig] for valid environment variables.
    """


class GCSStore(_ObjectStoreMixin, _GCSStore):
    """Interface to Google Cloud Storage.

    All constructors will check for environment variables. Refer to
    [`GCSConfig`][obstore.store.GCSConfig] for valid environment variables.

    If no credentials are explicitly provided, they will be sourced from the environment
    as documented
    [here](https://cloud.google.com/docs/authentication/application-default-credentials).
    """


class HTTPStore(_ObjectStoreMixin, _HTTPStore):
    """Configure a connection to a generic HTTP server.

    **Example**

    Accessing the number of stars for a repo:

    ```py
    import json

    import obstore as obs
    from obstore.store import HTTPStore

    store = HTTPStore.from_url("https://api.github.com")
    resp = obs.get(store, "repos/developmentseed/obstore")
    data = json.loads(resp.bytes())
    print(data["stargazers_count"])
    ```
    """


class LocalStore(_ObjectStoreMixin, _LocalStore):
    """An ObjectStore interface to local filesystem storage.

    Can optionally be created with a directory prefix.

    ```py
    from pathlib import Path

    store = LocalStore()
    store = LocalStore(prefix="/path/to/directory")
    store = LocalStore(prefix=Path("."))
    ```
    """


class MemoryStore(_ObjectStoreMixin, _MemoryStore):
    """A fully in-memory implementation of ObjectStore.

    Create a new in-memory store:
    ```py
    store = MemoryStore()
    ```
    """


class S3Store(_ObjectStoreMixin, _S3Store):
    """Interface to an Amazon S3 bucket.

    All constructors will check for environment variables. Refer to
    [`S3Config`][obstore.store.S3Config] for valid environment variables.

    **Examples**:

    **Using requester-pays buckets**:

    Pass `request_payer=True` as a keyword argument or have `AWS_REQUESTER_PAYS=True`
    set in the environment.

    **Anonymous requests**:

    Pass `skip_signature=True` as a keyword argument or have `AWS_SKIP_SIGNATURE=True`
    set in the environment.
    """


ObjectStore: TypeAlias = (
    AzureStore | GCSStore | HTTPStore | S3Store | LocalStore | MemoryStore
)
"""All supported ObjectStore implementations."""
