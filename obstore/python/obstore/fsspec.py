"""Integration with the [fsspec] library.

[fsspec]: https://github.com/fsspec/filesystem_spec

The fsspec integration is **best effort** and not the primary API of `obstore`. This
integration may not be as stable and may not provide the same performance as the rest of
the library. Changes may be made even in patch releases to align better with fsspec
expectations. If you find any bugs, please [file an
issue](https://github.com/developmentseed/obstore/issues/new/choose).

The underlying `object_store` Rust crate
[cautions](https://docs.rs/object_store/latest/object_store/#why-not-a-filesystem-interface)
against relying too strongly on stateful filesystem representations of object stores:

> The ObjectStore interface is designed to mirror the APIs of object stores and not
> filesystems, and thus has stateless APIs instead of cursor based interfaces such as
> Read or Seek available in filesystems.
>
> This design provides the following advantages:
>
> - All operations are atomic, and readers cannot observe partial and/or failed writes
> - Methods map directly to object store APIs, providing both efficiency and
>   predictability
> - Abstracts away filesystem and operating system specific quirks, ensuring portability
> - Allows for functionality not native to filesystems, such as operation preconditions
>   and atomic multipart uploads

Where possible, implementations should use the underlying `obstore` APIs
directly. Only where this is not possible should users fall back to this fsspec
integration.
"""

# ruff: noqa: ANN401, PTH123, FBT001, FBT002

from __future__ import annotations

import asyncio
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload
from urllib.parse import urlparse

import fsspec.asyn
import fsspec.spec

import obstore as obs
from obstore import Bytes
from obstore.store import from_url

if TYPE_CHECKING:
    from collections.abc import Coroutine, Iterable

    from obstore import Bytes
    from obstore.store import (
        AzureConfig,
        AzureConfigInput,
        ClientConfig,
        GCSConfig,
        GCSConfigInput,
        ObjectStore,
        RetryConfig,
        S3Config,
        S3ConfigInput,
    )


class AsyncFsspecStore(fsspec.asyn.AsyncFileSystem):
    """An fsspec implementation based on a obstore Store.

    You should be able to pass an instance of this class into any API that expects an
    fsspec-style object.
    """

    # https://github.com/fsspec/filesystem_spec/blob/56054c0a30ceedab4c0e6a0f7e429666773baf6d/docs/source/features.rst#instance-caching
    cachable = True

    def __init__(  # noqa: PLR0913
        self,
        *args: Any,
        protocol: str | None = None,
        config: (
            S3Config
            | S3ConfigInput
            | GCSConfig
            | GCSConfigInput
            | AzureConfig
            | AzureConfigInput
            | None
        ) = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        asynchronous: bool = False,
        max_cache_size: int = 10,
        loop: Any = None,
        batch_size: int | None = None,
    ) -> None:
        """Construct a new AsyncFsspecStore.

        Args:
            protocol: The storage protocol to use, such as "s3",
                "gcs", or "abfs". If `None`, the default class-level protocol
                is used. Default to None.
            config: Configuration for the cloud storage provider, which can be one of
                S3Config, S3ConfigInput, GCSConfig, GCSConfigInput, AzureConfig,
                or AzureConfigInput. Any of these values will be applied after checking
                for environment variables. If `None`, no cloud storage configuration is
                applied beyond what is found in environment variables.
            client_options: Additional options for configuring the client.
            retry_config: Configuration for handling request errors.
            args: positional arguments passed on to the `fsspec.asyn.AsyncFileSystem`
                constructor.

        Keyword Args:
            asynchronous: Set to `True` if this instance is meant to be be called using
                the fsspec async API. This should only be set to true when running
                within a coroutine.
            max_cache_size (int, optional): The maximum number of stores the cache
                should keep. A cached store is kept internally for each bucket name.
                Defaults to 10.
            loop: since both fsspec/python and tokio/rust may be using loops, this
                should be kept `None` for now, and will not be used.
            batch_size: some operations on many files will batch their requests; if you
                are seeing timeouts, you may want to set this number smaller than the
                defaults, which are determined in `fsspec.asyn._get_batch_size`.

        Example:
        ```py
        from obstore.fsspec import AsyncFsspecStore
        from obstore.store import HTTPStore

        store = AsyncFsspecStore(
            protocol="https",
        )
        resp = store.cat("https://example.com")
        assert resp.startswith(b"<!doctype html>")
        ```

        """
        if protocol is None:
            self._protocol = self.protocol
        else:
            self._protocol = protocol
        self.config = config
        self.client_options = client_options
        self.retry_config = retry_config

        # https://stackoverflow.com/a/68550238
        self._construct_store = lru_cache(maxsize=max_cache_size)(self._construct_store)

        super().__init__(
            *args,
            asynchronous=asynchronous,
            loop=loop,
            batch_size=batch_size,
        )

    def _split_path(self, path: str) -> tuple[str, str]:
        """Split bucket and file path.

        Args:
            path: Input path, like `s3://mybucket/path/to/file`

        Returns:
            (bucket name, file path inside the bucket)

        Examples:
            >>> split_path("s3://mybucket/path/to/file")
            ['mybucket', 'path/to/file']

        """
        protocol_with_bucket = ["s3", "s3a", "gcs", "gs", "abfs", "https", "http"]

        if self._protocol not in protocol_with_bucket:
            # no bucket name in path
            return "", path

        res = urlparse(path)
        if res.scheme:
            # path is in url format
            if res.scheme != self._protocol:
                err_msg = f"Expect protocol to be {self._protocol}. Got {res.scheme}"
                raise ValueError(err_msg)
            return (res.netloc, res.path)

        # path not in url format
        if "/" not in path:
            return path, ""
        path_li = path.split("/")
        bucket = path_li[0]
        file_path = "/".join(path_li[1:])
        return (bucket, file_path)

    def _construct_store(self, bucket: str) -> ObjectStore:
        return from_url(
            url=f"{self._protocol}://{bucket}",
            config=self.config,
            client_options=self.client_options,
            retry_config=self.retry_config or None,
        )

    async def _rm_file(self, path: str, **_kwargs: Any) -> None:
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)
        return await obs.delete_async(store, path)

    async def _cp_file(self, path1: str, path2: str, **_kwargs: Any) -> None:
        bucket1, path1_no_bucket = self._split_path(path1)
        bucket2, path2_no_bucket = self._split_path(path2)

        if bucket1 != bucket2:
            err_msg = (
                f"Bucket mismatch: Source bucket '{bucket1}' and "
                f"destination bucket '{bucket2}' must be the same."
            )
            raise ValueError(err_msg)

        store = self._construct_store(bucket1)

        return await obs.copy_async(store, path1_no_bucket, path2_no_bucket)

    async def _pipe_file(
        self,
        path: str,
        value: Any,
        mode: str = "overwrite",  # noqa: ARG002
        **_kwargs: Any,
    ) -> Any:
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)
        return await obs.put_async(store, path, value)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **_kwargs: Any,
    ) -> bytes:
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)

        if start is None and end is None:
            resp = await obs.get_async(store, path)
            return (await resp.bytes_async()).to_bytes()

        if start is None or end is None:
            raise NotImplementedError(
                "cat_file not implemented for start=None xor end=None",
            )

        range_bytes = await obs.get_range_async(store, path, start=start, end=end)
        return range_bytes.to_bytes()

    async def _cat(
        self,
        path: str,
        recursive: bool = False,
        on_error: str = "raise",
        batch_size: int | None = None,
        **_kwargs: Any,
    ) -> bytes | dict[str, bytes]:
        paths = await self._expand_path(path, recursive=recursive)

        # Filter out directories
        files = [p for p in paths if not await self._isdir(p)]

        if not files:
            err_msg = f"No valid files found in {path}"
            raise FileNotFoundError(err_msg)

        # Call the original _cat only on files
        return await super()._cat(
            files,
            recursive=False,
            on_error=on_error,
            batch_size=batch_size,
            **_kwargs,
        )

    async def _cat_ranges(  # noqa: PLR0913
        self,
        paths: list[str],
        starts: list[int] | int,
        ends: list[int] | int,
        max_gap=None,  # noqa: ANN001, ARG002
        batch_size=None,  # noqa: ANN001, ARG002
        on_error="return",  # noqa: ANN001, ARG002
        **_kwargs: Any,
    ) -> list[bytes]:
        if isinstance(starts, int):
            starts = [starts] * len(paths)
        if isinstance(ends, int):
            ends = [ends] * len(paths)
        if not len(paths) == len(starts) == len(ends):
            raise ValueError

        per_file_requests: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
        for idx, (path, start, end) in enumerate(
            zip(paths, starts, ends, strict=False),
        ):
            per_file_requests[path].append((start, end, idx))

        futs: list[Coroutine[Any, Any, list[Bytes]]] = []
        for path, ranges in per_file_requests.items():
            bucket, path_no_bucket = self._split_path(path)
            store = self._construct_store(bucket)

            offsets = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            fut = obs.get_ranges_async(store, path_no_bucket, starts=offsets, ends=ends)
            futs.append(fut)

        result = await asyncio.gather(*futs)

        output_buffers: list[bytes] = [b""] * len(paths)
        for per_file_request, buffers in zip(
            per_file_requests.items(),
            result,
            strict=True,
        ):
            path, ranges = per_file_request
            for buffer, ranges_ in zip(buffers, ranges, strict=True):
                initial_index = ranges_[2]
                output_buffers[initial_index] = buffer.to_bytes()

        return output_buffers

    async def _put_file(
        self,
        lpath: str,
        rpath: str,
        mode: str = "overwrite",  # noqa: ARG002
        **_kwargs: Any,
    ) -> None:
        if not Path(lpath).is_file():
            err_msg = f"File {lpath} not found in local"
            raise FileNotFoundError(err_msg)

        # TODO: convert to use async file system methods using LocalStore
        # Async functions should not open files with blocking methods like `open`
        rbucket, rpath = self._split_path(rpath)

        # Should construct the store instance by rbucket, which is the target path
        store = self._construct_store(rbucket)

        with open(lpath, "rb") as f:  # noqa: ASYNC230
            await obs.put_async(store, rpath, f)

    async def _get_file(self, rpath: str, lpath: str, **_kwargs: Any) -> None:
        res = urlparse(lpath)
        if res.scheme or Path(lpath).is_dir():
            # lpath need to be local file and cannot contain scheme
            return

        # TODO: convert to use async file system methods using LocalStore
        # Async functions should not open files with blocking methods like `open`
        rbucket, rpath = self._split_path(rpath)

        # Should construct the store instance by rbucket, which is the target path
        store = self._construct_store(rbucket)

        with open(lpath, "wb") as f:  # noqa: ASYNC230
            resp = await obs.get_async(store, rpath)
            async for buffer in resp.stream():
                f.write(buffer)

    async def _info(self, path: str, **_kwargs: Any) -> dict[str, Any]:
        bucket, path_no_bucket = self._split_path(path)
        store = self._construct_store(bucket)

        try:
            head = await obs.head_async(store, path_no_bucket)
            return {
                # Required of `info`: (?)
                "name": head["path"],
                "size": head["size"],
                "type": "directory" if head["path"].endswith("/") else "file",
                # Implementation-specific keys
                "e_tag": head["e_tag"],
                "last_modified": head["last_modified"],
                "version": head["version"],
            }
        except FileNotFoundError:
            # use info in fsspec.AbstractFileSystem
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, super().info, path, **_kwargs)

    @staticmethod
    def _fill_bucket_name(path: str, bucket: str) -> str:
        return f"{bucket}/{path}"

    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[False],
        **_kwargs: Any,
    ) -> list[str]: ...
    @overload
    async def _ls(
        self,
        path: str,
        detail: Literal[True] = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]]: ...
    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)

        result = await obs.list_with_delimiter_async(store, path)
        objects = result["objects"]
        prefs = result["common_prefixes"]
        files = [
            {
                "name": self._fill_bucket_name(obj["path"], bucket),
                "size": obj["size"],
                "type": "file",
                "e_tag": obj["e_tag"],
            }
            for obj in objects
        ] + [
            {
                "name": self._fill_bucket_name(pref, bucket),
                "size": 0,
                "type": "directory",
            }
            for pref in prefs
        ]
        if not files:
            raise FileNotFoundError(path)

        return files if detail else sorted(o["name"] for o in files)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Any = None,  # noqa: ARG002
        autocommit: Any = True,  # noqa: ARG002
        cache_options: Any = None,  # noqa: ARG002
        **kwargs: Any,
    ) -> BufferedFileSimple:
        """Return raw bytes-mode file-like from the file-system."""
        return BufferedFileSimple(self, path, mode, **kwargs)


class BufferedFileSimple(fsspec.spec.AbstractBufferedFile):
    """Implementation of buffered file around `fsspec.spec.AbstractBufferedFile`."""

    def __init__(
        self,
        fs: AsyncFsspecStore,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> None:
        """Create new buffered file."""
        if mode != "rb":
            raise ValueError("Only 'rb' mode is currently supported")
        super().__init__(fs, path, mode, **kwargs)

    def read(self, length: int = -1) -> Any:
        """Return bytes from the remote file.

        Args:
            length: if positive, returns up to this many bytes; if negative, return all
                remaining bytes.

        """
        if length < 0:
            data = self.fs.cat_file(self.path, self.loc, self.size)
            self.loc = self.size
        else:
            data = self.fs.cat_file(self.path, self.loc, self.loc + length)
            self.loc += length
        return data


def register(protocol: str | Iterable[str], *, asynchronous: bool = False) -> None:
    """Dynamically register a subclass of AsyncFsspecStore for the given protocol(s).

    This function creates a new subclass of AsyncFsspecStore with the specified
    protocol and registers it with fsspec. If multiple protocols are provided,
    the function registers each one individually.

    Args:
        protocol (str | list[str]): A single protocol (e.g., "s3", "gcs", "abfs") or
            a list of protocols to register AsyncFsspecStore for.
        asynchronous (bool, optional): If True, the registered store will support
            asynchronous operations. Defaults to False.

    Example:
        >>> register("s3")
        >>> register("s3", asynchronous=True)  # Registers an async store for "s3"
        >>> register(["gcs", "abfs"])  # Registers both "gcs" and "abfs"

    Notes:
        - Each protocol gets a dynamically generated subclass named
          `AsyncFsspecStore_<protocol>`.
        - This avoids modifying the original AsyncFsspecStore class.

    """
    if not protocol:
        raise ValueError(
            "Protocol must be a non-empty string or list",
        )
    if isinstance(protocol, str):
        _register(protocol, asynchronous=asynchronous)
        return

    for p in protocol:
        _register(p, asynchronous=asynchronous)


def _register(protocol: str, *, asynchronous: bool) -> None:
    if not protocol:
        raise ValueError(
            "Protocol must be a non-empty string",
        )
    if not isinstance(protocol, str):
        err_msg = f"Protocol must be a string, got {type(protocol).__name__}"
        raise TypeError(err_msg)

    fsspec.register_implementation(
        protocol,
        type(
            f"AsyncFsspecStore_{protocol}",  # Unique class name
            (AsyncFsspecStore,),  # Base class
            {
                "protocol": protocol,
                "asynchronous": asynchronous,
            },  # Assign protocol dynamically
        ),
        clobber=False,
    )
