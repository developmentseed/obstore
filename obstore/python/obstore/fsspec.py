"""Integration with the [fsspec] library.

[fsspec]: https://github.com/fsspec/filesystem_spec

The fsspec integration is best effort and may not provide the same performance as
the rest of obstore. If you find any bugs with this integration, please [file an
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

# ruff: noqa: ANN401, EM102, PTH123, FBT001, FBT002, S101

from __future__ import annotations

import asyncio
import warnings
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload
from urllib.parse import urlparse

import fsspec.asyn
import fsspec.spec

import obstore as obs
from obstore import open_reader, open_writer
from obstore.store import from_url

if TYPE_CHECKING:
    import sys
    from collections.abc import Coroutine, Iterable
    from typing import Any

    from obstore import Attributes, Bytes, ReadableFile, WritableFile
    from obstore.store import (
        AzureConfig,
        ClientConfig,
        GCSConfig,
        ObjectStore,
        RetryConfig,
        S3Config,
    )

    if sys.version_info >= (3, 11):
        from typing import Unpack
    else:
        from typing_extensions import Unpack


__all__ = [
    "BufferedFile",
    "FsspecStore",
    "register",
]

SUPPORTED_PROTOCOLS: set[str] = {
    "abfs",
    "abfss",
    "adl",
    "az",
    "azure",
    "file",
    "gcs",
    "gs",
    "http",
    "https",
    "memory",
    "s3",
    "s3a",
}
"""All supported protocols."""

SUPPORTED_PROTOCOLS_T = Literal[
    "abfs",
    "abfss",
    "adl",
    "az",
    "azure",
    "file",
    "gcs",
    "gs",
    "http",
    "https",
    "memory",
    "s3",
    "s3a",
]
"""A type hint for all supported protocols."""


class FsspecStore(fsspec.asyn.AsyncFileSystem):
    """An fsspec implementation based on a obstore Store.

    You should be able to pass an instance of this class into any API that expects an
    fsspec-style object.
    """

    # https://github.com/fsspec/filesystem_spec/blob/56054c0a30ceedab4c0e6a0f7e429666773baf6d/docs/source/features.rst#instance-caching
    cachable = True

    @overload
    def __init__(
        self,
        protocol: Literal["s3", "s3a"],
        *args: Any,
        config: S3Config | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        asynchronous: bool = False,
        max_cache_size: int = 10,
        loop: Any = None,
        batch_size: int | None = None,
        **kwargs: Unpack[S3Config],
    ) -> None: ...
    @overload
    def __init__(
        self,
        protocol: Literal["gs"],
        *args: Any,
        config: GCSConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        asynchronous: bool = False,
        max_cache_size: int = 10,
        loop: Any = None,
        batch_size: int | None = None,
        **kwargs: Unpack[GCSConfig],
    ) -> None: ...
    @overload
    def __init__(
        self,
        protocol: Literal["az", "adl", "azure", "abfs", "abfss"],
        *args: Any,
        config: AzureConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        asynchronous: bool = False,
        max_cache_size: int = 10,
        loop: Any = None,
        batch_size: int | None = None,
        **kwargs: Unpack[AzureConfig],
    ) -> None: ...
    def __init__(  # noqa: PLR0913
        self,
        protocol: SUPPORTED_PROTOCOLS_T | str | None = None,
        *args: Any,
        config: S3Config | GCSConfig | AzureConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        asynchronous: bool = False,
        max_cache_size: int = 10,
        loop: Any = None,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Construct a new FsspecStore.

        Args:
            protocol: The storage protocol to use, such as "s3",
                "gcs", or "abfs". If `None`, the default class-level protocol
                is used. Default to None.
            config: Configuration for the cloud storage provider, which can be one of
                S3Config, GCSConfig, AzureConfig,
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
            kwargs: per-store configuration passed down to store-specific builders.

        **Examples:**

        ```py
        from obstore.fsspec import FsspecStore

        store = FsspecStore("https")
        resp = store.cat_file("https://raw.githubusercontent.com/developmentseed/obstore/refs/heads/main/README.md")
        assert resp.startswith(b"# obstore")
        ```

        """
        # TODO: support multiple protocols as input
        # We need to assign to self.protocol so that other libraries can see that this
        # is a concrete implementation. E.g. `duckdb.register_filesystem` fails when
        # `self.protocol` is the default `"abstract"`.
        if protocol is not None:
            self.protocol = protocol  # type: ignore (incorrect typing as ClassVar)

        if self.protocol not in SUPPORTED_PROTOCOLS:
            warnings.warn(
                f"Unknown protocol: {self.protocol}; requests may fail.",
                stacklevel=2,
            )

        self.config = config
        self.client_options = client_options
        self.retry_config = retry_config
        self.config_kwargs = kwargs

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
        protocol_without_bucket = {"file", "memory"}

        # Parse the path as a URL
        parsed = urlparse(path)

        # If the protocol doesn't require buckets, return empty bucket and full path
        if self.protocol in protocol_without_bucket:
            return (
                "",
                f"{parsed.netloc}/{parsed.path.lstrip('/')}" if parsed.scheme else path,
            )

        if parsed.scheme:
            if isinstance(self.protocol, str) and parsed.scheme != self.protocol:
                err_msg = (
                    f"Expected protocol to be {self.protocol}. Got {parsed.scheme}"
                )
                raise ValueError(err_msg)

            if parsed.scheme not in self.protocol:
                err_msg = (
                    f"Expected protocol to be one of {self.protocol}. "
                    f"Got {parsed.scheme}"
                )
                raise ValueError(err_msg)

            return (parsed.netloc, parsed.path.lstrip("/"))

        # path not in url format
        path_li = path.split("/", 1)
        if len(path_li) == 1:
            return path, ""

        return (path_li[0], path_li[1])

    def _construct_store(self, bucket: str) -> ObjectStore:
        protocol = self.protocol if isinstance(self.protocol, str) else self.protocol[0]
        return from_url(
            url=f"{protocol}://{bucket}",
            config=self.config,
            client_options=self.client_options,
            retry_config=self.retry_config,
            **self.config_kwargs,
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
        # When upgrading to Python 3.10, use strict=True
        for idx, (path, start, end) in enumerate(zip(paths, starts, ends)):
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
        # When upgrading to Python 3.10, use strict=True
        for per_file_request, buffers in zip(per_file_requests.items(), result):
            path, ranges = per_file_request
            # When upgrading to Python 3.10, use strict=True
            for buffer, ranges_ in zip(buffers, ranges):
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
            pass

        # Ref: https://github.com/fsspec/s3fs/blob/01b9c4b838b81375093ae1d78562edf6bdc616ea/s3fs/core.py#L1471-L1492
        # We check to see if the path is a directory by attempting to list its
        # contexts. If anything is found, it is indeed a directory
        out = await self._ls(path, detail=True)
        if len(out) > 0:
            return {
                "name": f"{bucket}/{path_no_bucket}",
                "type": "directory",
                "size": 0,
            }
        raise FileNotFoundError(path)

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
        # This is a change from the base class, but it seems like every fsspec
        # implementation overrides this 🤷‍♂️
        # E.g. https://github.com/fsspec/s3fs/issues/945
        detail: bool = False,
        **_kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)

        result = await obs.list_with_delimiter_async(store, path)
        objects = result["objects"]
        prefs = result["common_prefixes"]
        files = [
            {
                "name": f"{bucket}/{obj['path']}",
                "size": obj["size"],
                "type": "file",
                "e_tag": obj["e_tag"],
            }
            for obj in objects
        ] + [
            {
                "name": f"{bucket}/{pref}",
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
    ) -> BufferedFile:
        """Return raw bytes-mode file-like from the file-system."""
        if mode not in ("wb", "rb"):
            err_msg = f"Only 'rb' and 'wb' modes supported, got: {mode}"
            raise ValueError(err_msg)

        return BufferedFile(self, path, mode, **kwargs)


class BufferedFile(fsspec.spec.AbstractBufferedFile):
    """A buffered readable or writable file.

    This is a wrapper around [`obstore.ReadableFile`][] and [`obstore.WritableFile`][].
    If you don't have a need to use the fsspec integration, you may be better served by
    using [`open_reader`][obstore.open_reader] or [`open_writer`][obstore.open_writer]
    directly.
    """

    mode: Literal["rb", "wb"]
    _reader: ReadableFile
    _writer: WritableFile
    _writer_loc: int
    """Stream position.

    Only defined for writers. We use the underlying rust stream position for reading.
    """

    @overload
    def __init__(
        self,
        fs: FsspecStore,
        path: str,
        mode: Literal["rb"] = "rb",
        *,
        buffer_size: int = 1024 * 1024,
        **kwargs: Any,
    ) -> None: ...
    @overload
    def __init__(
        self,
        fs: FsspecStore,
        path: str,
        mode: Literal["wb"],
        *,
        buffer_size: int = 10 * 1024 * 1024,
        attributes: Attributes | None = None,
        tags: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None: ...
    def __init__(  # noqa: PLR0913
        self,
        fs: FsspecStore,
        path: str,
        mode: Literal["rb", "wb"] = "rb",
        *,
        buffer_size: int | None = None,
        attributes: Attributes | None = None,
        tags: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Create new buffered file.

        Args:
            fs: The underlying fsspec store to read from.
            path: The path within the store to use.
            mode: `"rb"` for a readable binary file or `"wb"` for a writable binary
                file. Defaults to "rb".

        Keyword Args:
            attributes: Provide a set of `Attributes`. Only used when writing. Defaults
                to `None`.
            buffer_size: Up to `buffer_size` bytes will be buffered in memory. **When
                reading:** The minimum number of bytes to read in a single request.
                **When writing:**  If `buffer_size` is exceeded, data will be uploaded
                as a multipart upload in chunks of `buffer_size`. Defaults to None.
            tags: Provide tags for this object. Only used when writing. Defaults to
                `None`.
            kwargs: Keyword arguments passed on to [`fsspec.spec.AbstractBufferedFile`][].

        """  # noqa: E501
        super().__init__(fs, path, mode, **kwargs)

        bucket, path = fs._split_path(path)  # noqa: SLF001
        store = fs._construct_store(bucket)  # noqa: SLF001

        self.mode = mode

        if self.mode == "rb":
            buffer_size = 1024 * 1024 if buffer_size is None else buffer_size
            self._reader = open_reader(store, path, buffer_size=buffer_size)
        elif self.mode == "wb":
            buffer_size = 10 * 1024 * 1024 if buffer_size is None else buffer_size
            self._writer = open_writer(
                store,
                path,
                attributes=attributes,
                buffer_size=buffer_size,
                tags=tags,
            )

            self._writer_loc = 0
        else:
            raise ValueError(f"Invalid mode: {mode}")

    def read(self, length: int = -1) -> bytes:
        """Return bytes from the remote file.

        Args:
            length: if positive, returns up to this many bytes; if negative, return all
                remaining bytes.

        Returns:
            Data in bytes

        """
        if self.mode != "rb":
            raise ValueError("File not in read mode")
        if length < 0:
            length = self.size - self.tell()
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if length == 0:
            # don't even bother calling fetch
            return b""

        out = self._reader.read(length)
        return out.to_bytes()

    def readline(self) -> bytes:
        """Read until first occurrence of newline character."""
        if self.mode != "rb":
            raise ValueError("File not in read mode")

        out = self._reader.readline()
        return out.to_bytes()

    def readlines(self) -> list[bytes]:
        """Return all data, split by the newline character."""
        if self.mode != "rb":
            raise ValueError("File not in read mode")

        out = self._reader.readlines()
        return [b.to_bytes() for b in out]

    def tell(self) -> int:
        """Get current file location."""
        if self.mode == "rb":
            return self._reader.tell()

        if self.mode == "wb":
            # There's no way to get the stream position from the underlying writer
            # because it's async. Here we happen to be using the async writer in a
            # synchronous way, so we keep our own stream position.
            assert self._writer_loc is not None
            return self._writer_loc

        raise ValueError(f"Unexpected mode {self.mode}")

    def seek(self, loc: int, whence: int = 0) -> int:
        """Set current file location.

        Args:
            loc: byte location
            whence: Either
                - `0`: from start of file
                - `1`: current location
                - `2`: end of file

        """
        if self.mode != "rb":
            raise ValueError("Seek only available in read mode.")

        return self._reader.seek(loc, whence)

    def write(self, data: bytes) -> int:
        """Write data to buffer.

        Args:
            data: Set of bytes to be written.

        """
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.forced:
            raise ValueError("This file has been force-flushed, can only close")

        num_written = self._writer.write(data)
        self._writer_loc += num_written

        return num_written

    def flush(
        self,
        force: bool = False,  # noqa: ARG002
    ) -> None:
        """Write buffered data to backend store.

        Writes the current buffer, if it is larger than the block-size, or if
        the file is being closed.

        Args:
            force: Unused.

        """
        if self.closed:
            raise ValueError("Flush on closed file")

        if self.readable():
            # no-op to flush on read-mode
            return

        self._writer.flush()

    def close(self) -> None:
        """Close file. Ensure flushing the buffer."""
        if self.closed:
            return

        try:
            if self.mode == "rb":
                self._reader.close()
            else:
                self.flush(force=True)
                self._writer.close()
        finally:
            self.closed = True

    @property
    def loc(self) -> int:
        """Get current file location."""
        # Note, we override the `loc` attribute, because for the reader we manage that
        # state in Rust.
        return self.tell()

    @loc.setter
    def loc(self, value: int) -> None:
        if value != 0:
            raise ValueError("Cannot set `.loc`. Use `seek` instead.")


def register(
    protocol: (
        SUPPORTED_PROTOCOLS_T
        | str
        | Iterable[SUPPORTED_PROTOCOLS_T]
        | Iterable[str]
        | None
    ) = None,
    *,
    asynchronous: bool = False,
) -> None:
    """Dynamically register a subclass of FsspecStore for the given protocol(s).

    This function creates a new subclass of FsspecStore with the specified
    protocol and registers it with fsspec. If multiple protocols are provided,
    the function registers each one individually.

    Args:
        protocol: A single protocol (e.g., "s3", "gcs", "abfs") or
            a list of protocols to register FsspecStore for.

            Defaults to `None`, which will register `obstore` as the provider for all
            [supported protocols][obstore.fsspec.SUPPORTED_PROTOCOLS] **except** for
            `file://` and `memory://`. If you wish to use `obstore` via fsspec for
            `file://` or `memory://` URLs, list them explicitly.
        asynchronous: If `True`, the registered store will support
            asynchronous operations. Defaults to `False`.

    Example:
    ```py
    # Register obstore as the default handler for all supported protocols except for
    # `memory://` and `file://`
    register()

    register("s3")

    # Registers an async store for "s3"
    register("s3", asynchronous=True)

    # Registers both "gcs" and "abfs"
    register(["gcs", "abfs"])
    ```

    Notes:
        - Each protocol gets a dynamically generated subclass named
          `FsspecStore_<protocol>`. This avoids modifying the original
          FsspecStore class.

    """
    if protocol is None:
        protocol = SUPPORTED_PROTOCOLS - {"file", "memory"}

    if isinstance(protocol, str):
        _register(protocol, asynchronous=asynchronous)
        return

    for p in protocol:
        _register(p, asynchronous=asynchronous)


def _register(protocol: str, *, asynchronous: bool) -> None:
    fsspec.register_implementation(
        protocol,
        type(
            f"FsspecStore_{protocol}",  # Unique class name
            (FsspecStore,),  # Base class
            {
                "protocol": protocol,
                "asynchronous": asynchronous,
            },  # Assign protocol dynamically
        ),
        # Override any existing implementations of the same protocol
        clobber=True,
    )
