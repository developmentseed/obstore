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

# ruff: noqa: ANN401
# Dynamically typed expressions (typing.Any) are disallowed
# ruff: noqa: PTH123
# `open()` should be replaced by `Path.open()`

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Literal, overload

import fsspec.asyn
import fsspec.spec

import obstore as obs

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from obstore import Bytes


class AsyncFsspecStore(fsspec.asyn.AsyncFileSystem):
    """An fsspec implementation based on a obstore Store.

    You should be able to pass an instance of this class into any API that expects an
    fsspec-style object.
    """

    cachable = False

    def __init__(
        self,
        store: obs.store.ObjectStore,
        *args: Any,
        asynchronous: bool = False,
        loop: Any = None,
        batch_size: int | None = None,
    ) -> None:
        """Construct a new AsyncFsspecStore.

        Args:
            store: a configured instance of one of the store classes in `obstore.store`.
            args: positional arguments passed on to the `fsspec.asyn.AsyncFileSystem`
                constructor.

        Keyword Args:
            asynchronous: Set to `True` if this instance is meant to be be called using
                the fsspec async API. This should only be set to true when running
                within a coroutine.
            loop: since both fsspec/python and tokio/rust may be using loops, this
                should be kept `None` for now, and will not be used.
            batch_size: some operations on many files will batch their requests; if you
                are seeing timeouts, you may want to set this number smaller than the
                defaults, which are determined in `fsspec.asyn._get_batch_size`.

        Example:
        ```py
        from obstore.fsspec import AsyncFsspecStore
        from obstore.store import HTTPStore

        store = HTTPStore.from_url("https://example.com")
        fsspec_store = AsyncFsspecStore(store)
        resp = fsspec_store.cat("/")
        assert resp.startswith(b"<!doctype html>")
        ```

        """
        self.store = store
        super().__init__(
            *args,
            asynchronous=asynchronous,
            loop=loop,
            batch_size=batch_size,
        )

    async def _rm_file(self, path: str, **_kwargs: Any) -> None:
        return await obs.delete_async(self.store, path)

    async def _cp_file(self, path1: str, path2: str, **_kwargs: Any) -> None:
        return await obs.copy_async(self.store, path1, path2)

    async def _pipe_file(
        self,
        path: str,
        value: Any,
        mode: str = "overwrite",  # noqa: ARG002
        **_kwargs: Any,
    ) -> Any:
        return await obs.put_async(self.store, path, value)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **_kwargs: Any,
    ) -> bytes:
        if start is None and end is None:
            resp = await obs.get_async(self.store, path)
            return (await resp.bytes_async()).to_bytes()

        if start is None or end is None:
            raise NotImplementedError(
                "cat_file not implemented for start=None xor end=None",
            )

        range_bytes = await obs.get_range_async(self.store, path, start=start, end=end)
        return range_bytes.to_bytes()

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
            offsets = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            fut = obs.get_ranges_async(self.store, path, starts=offsets, ends=ends)
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
        # TODO: convert to use async file system methods using LocalStore
        # Async functions should not open files with blocking methods like `open`
        with open(lpath, "rb") as f:  # noqa: ASYNC230
            await obs.put_async(self.store, rpath, f)

    async def _get_file(self, rpath: str, lpath: str, **_kwargs: Any) -> None:
        # TODO: convert to use async file system methods using LocalStore
        # Async functions should not open files with blocking methods like `open`
        with open(lpath, "wb") as f:  # noqa: ASYNC230
            resp = await obs.get_async(self.store, rpath)
            async for buffer in resp.stream():
                f.write(buffer)

    async def _info(self, path: str, **_kwargs: Any) -> dict[str, Any]:
        head = await obs.head_async(self.store, path)
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
        detail: Literal[True] = True,  # noqa: FBT002
        **_kwargs: Any,
    ) -> list[dict[str, Any]]: ...
    async def _ls(
        self,
        path: str,
        detail: bool = True,  # noqa: FBT001, FBT002
        **_kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        result = await obs.list_with_delimiter_async(self.store, path)
        objects = result["objects"]
        prefs = result["common_prefixes"]
        if detail:
            return [
                {
                    "name": obj["path"],
                    "size": obj["size"],
                    "type": "file",
                    "e_tag": obj["e_tag"],
                }
                for obj in objects
            ] + [{"name": obj, "size": 0, "type": "directory"} for obj in prefs]
        return sorted([obj["path"] for obj in objects] + prefs)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Any = None,  # noqa: ARG002
        autocommit: Any = True,  # noqa: ARG002, FBT002
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
