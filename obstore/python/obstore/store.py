"""Interface for constructing cloud storage classes."""

# ruff: noqa: F401

from __future__ import annotations

from typing import TYPE_CHECKING, Union, overload

import obstore as obs
from obstore._obstore import _store
from obstore._obstore import parse_scheme as _parse_scheme
from obstore.exceptions import BaseError

if TYPE_CHECKING:
    import sys
    from collections.abc import (
        AsyncIterable,
        AsyncIterator,
        Callable,
        Iterable,
        Iterator,
        Sequence,
    )
    from pathlib import Path
    from typing import IO, Any, Literal

    from arro3.core import RecordBatch, Table

    from obstore import (
        Attributes,
        GetOptions,
        ListResult,
        ListStream,
        ObjectMeta,
        PutMode,
        PutResult,
    )
    from obstore._obstore import Bytes, GetResult
    from obstore._store import (
        AzureAccessKey,
        AzureBearerToken,
        AzureConfig,
        AzureCredential,
        AzureCredentialProvider,
        AzureSASToken,
        BackoffConfig,
        ClientConfig,
        GCSConfig,
        GCSCredential,
        GCSCredentialProvider,
        RetryConfig,
        S3Config,
        S3Credential,
        S3CredentialProvider,
    )

    if sys.version_info >= (3, 10):
        from typing import TypeAlias
    else:
        from typing_extensions import TypeAlias

    if sys.version_info >= (3, 11):
        from typing import Unpack
    else:
        from typing_extensions import Unpack

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
        # Splitting these fixes the typing issue with the `return_arrow` parameter, by
        # converting from a bool to a Literal[True] or Literal[False]
        if return_arrow:
            return obs.list(
                self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
                prefix,
                offset=offset,
                chunk_size=chunk_size,
                return_arrow=return_arrow,
            )

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
        # Splitting these fixes the typing issue with the `return_arrow` parameter, by
        # converting from a bool to a Literal[True] or Literal[False]
        if return_arrow:
            return obs.list_with_delimiter(
                self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
                prefix,
                return_arrow=return_arrow,
            )

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
        # Splitting these fixes the typing issue with the `return_arrow` parameter, by
        # converting from a bool to a Literal[True] or Literal[False]
        if return_arrow:
            return await obs.list_with_delimiter_async(
                self,  # type: ignore (Argument of type "Self@_ObjectStoreMixin" cannot be assigned to parameter "store")
                prefix,
                return_arrow=return_arrow,
            )

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


class AzureStore(_ObjectStoreMixin, _store.AzureStore):
    """Interface to a Microsoft Azure Blob Storage container.

    All constructors will check for environment variables. Refer to
    [`AzureConfig`][obstore.store.AzureConfig] for valid environment variables.
    """


class GCSStore(_ObjectStoreMixin, _store.GCSStore):
    """Interface to Google Cloud Storage.

    All constructors will check for environment variables. Refer to
    [`GCSConfig`][obstore.store.GCSConfig] for valid environment variables.

    If no credentials are explicitly provided, they will be sourced from the environment
    as documented
    [here](https://cloud.google.com/docs/authentication/application-default-credentials).
    """


class HTTPStore(_ObjectStoreMixin, _store.HTTPStore):
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


class LocalStore(_ObjectStoreMixin, _store.LocalStore):
    """An ObjectStore interface to local filesystem storage.

    Can optionally be created with a directory prefix.

    ```py
    from pathlib import Path

    store = LocalStore()
    store = LocalStore(prefix="/path/to/directory")
    store = LocalStore(prefix=Path("."))
    ```
    """


class MemoryStore(_ObjectStoreMixin, _store.MemoryStore):
    """A fully in-memory implementation of ObjectStore.

    Create a new in-memory store:
    ```py
    store = MemoryStore()
    ```
    """


class S3Store(_ObjectStoreMixin, _store.S3Store):
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


ObjectStore: TypeAlias = Union[
    AzureStore,
    GCSStore,
    HTTPStore,
    S3Store,
    LocalStore,
    MemoryStore,
]
"""All supported ObjectStore implementations."""


# Note: we define `from_url` again so that we can instantiate the **subclasses**.
@overload
def from_url(
    url: str,
    *,
    config: S3Config | None = None,
    client_options: ClientConfig | None = None,
    retry_config: RetryConfig | None = None,
    credential_provider: S3CredentialProvider | None = None,
    **kwargs: Unpack[S3Config],
) -> ObjectStore: ...
@overload
def from_url(
    url: str,
    *,
    config: GCSConfig | None = None,
    client_options: ClientConfig | None = None,
    retry_config: RetryConfig | None = None,
    credential_provider: GCSCredentialProvider | None = None,
    **kwargs: Unpack[GCSConfig],
) -> ObjectStore: ...
@overload
def from_url(
    url: str,
    *,
    config: AzureConfig | None = None,
    client_options: ClientConfig | None = None,
    retry_config: RetryConfig | None = None,
    credential_provider: AzureCredentialProvider | None = None,
    **kwargs: Unpack[AzureConfig],
) -> ObjectStore: ...
@overload
def from_url(  # type: ignore (parameter overlap)
    url: str,
    *,
    config: None = None,
    client_options: None = None,
    retry_config: None = None,
    automatic_cleanup: bool = False,
    mkdir: bool = False,
) -> ObjectStore: ...
def from_url(  # noqa: C901
    url: str,
    *,
    config: S3Config | GCSConfig | AzureConfig | None = None,
    client_options: ClientConfig | None = None,
    retry_config: RetryConfig | None = None,
    credential_provider: Callable | None = None,
    **kwargs: Any,
) -> ObjectStore:
    """Easy construction of store by URL, identifying the relevant store.

    This will defer to a store-specific `from_url` constructor based on the provided
    `url`. E.g. passing `"s3://bucket/path"` will defer to
    [`S3Store.from_url`][obstore.store.S3Store.from_url].

    Supported formats:

    - `file:///path/to/my/file` -> [`LocalStore`][obstore.store.LocalStore]
    - `memory:///` -> [`MemoryStore`][obstore.store.MemoryStore]
    - `s3://bucket/path` -> [`S3Store`][obstore.store.S3Store] (also supports `s3a`)
    - `gs://bucket/path` -> [`GCSStore`][obstore.store.GCSStore]
    - `az://account/container/path` -> [`AzureStore`][obstore.store.AzureStore] (also
      supports `adl`, `azure`, `abfs`, `abfss`)
    - `http://mydomain/path` -> [`HTTPStore`][obstore.store.HTTPStore]
    - `https://mydomain/path` -> [`HTTPStore`][obstore.store.HTTPStore]

    There are also special cases for AWS and Azure for `https://{host?}/path` paths:

    - `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`,
      `blob.fabric.microsoft.com` -> [`AzureStore`][obstore.store.AzureStore]
    - `amazonaws.com` -> [`S3Store`][obstore.store.S3Store]
    - `r2.cloudflarestorage.com` -> [`S3Store`][obstore.store.S3Store]

    !!! note
        For best static typing, use the constructors on individual store classes
        directly.

    Args:
        url: well-known storage URL.

    Keyword Args:
        config: per-store Configuration. Values in this config will override values
            inferred from the url. Defaults to None.
        client_options: HTTP Client options. Defaults to None.
        retry_config: Retry configuration. Defaults to None.
        credential_provider: A callback to provide custom credentials to the underlying
            store classes.
        kwargs: per-store configuration passed down to store-specific builders.

    """
    scheme = _parse_scheme(url)
    if scheme == "s3":
        return S3Store.from_url(
            url,
            config=config,  # type: ignore (config narrowing)
            client_options=client_options,
            retry_config=retry_config,
            credential_provider=credential_provider,
            **kwargs,
        )
    if scheme == "gcs":
        return GCSStore.from_url(
            url,
            config=config,  # type: ignore (config narrowing)
            client_options=client_options,
            retry_config=retry_config,
            credential_provider=credential_provider,
            **kwargs,
        )
    if scheme == "azure":
        return AzureStore.from_url(
            url,
            config=config,  # type: ignore (config narrowing)
            client_options=client_options,
            retry_config=retry_config,
            credential_provider=credential_provider,
            **kwargs,
        )
    if scheme == "http":
        if config or kwargs:
            msg = "HTTPStore does not accept any configuration"
            raise BaseError(msg)

        return HTTPStore.from_url(
            url,
            client_options=client_options,
            retry_config=retry_config,
        )
    if scheme == "local":
        automatic_cleanup = False
        mkdir = False
        if "automatic_cleanup" in kwargs:
            automatic_cleanup = kwargs.pop("automatic_cleanup")
        if "mkdir" in kwargs:
            mkdir = kwargs.pop("mkdir")

        return LocalStore.from_url(
            url,
            automatic_cleanup=automatic_cleanup,
            mkdir=mkdir,
        )
    if scheme == "memory":
        if config or kwargs:
            msg = "MemoryStore does not accept any configuration"
            raise BaseError(msg)

        return MemoryStore()

    msg = f"Unknown scheme: {url}"
    raise BaseError(msg)
