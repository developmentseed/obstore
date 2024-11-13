"""Fsspec integration.

The underlying `object_store` Rust crate [cautions](https://docs.rs/object_store/latest/object_store/#why-not-a-filesystem-interface) against relying too strongly on stateful filesystem representations of object stores:

> The ObjectStore interface is designed to mirror the APIs of object stores and not filesystems, and thus has stateless APIs instead of cursor based interfaces such as Read or Seek available in filesystems.
>
> This design provides the following advantages:
>
> - All operations are atomic, and readers cannot observe partial and/or failed writes
> - Methods map directly to object store APIs, providing both efficiency and predictability
> - Abstracts away filesystem and operating system specific quirks, ensuring portability
> - Allows for functionality not native to filesystems, such as operation preconditions and atomic multipart uploads

Where possible, implementations should use the underlying `obstore` APIs
directly. Only where this is not possible should users fall back to this fsspec
integration.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Coroutine, Dict, List, Tuple

import fsspec.asyn
import fsspec.spec

import obstore as obs


class AsyncFsspecStore(fsspec.asyn.AsyncFileSystem):
    """An fsspec implementation based on a obstore Store"""

    cachable = False

    def __init__(
        self,
        store: obs.store.ObjectStore,
        *args,
        asynchronous: bool = False,
        loop=None,
        batch_size: int | None = None,
    ):
        """Construct a new AsyncFsspecStore

        store: a configured instance of one of the store classes in objstore.store
        asynchronous: id this instance meant to be be called using the async API? This
            should only be set to true when running within a coroutine
        loop: since both fsspec/python and tokio/rust may be using loops, this should
            be kept `None` for now, and will not be used.
        batch_size: some operations on many files will batch their requests; if you
            are seeing timeouts, you may want to set this number smaller than the defaults,
            which are determined in fsspec.asyn._get_batch_size
        """

        self.store = store
        super().__init__(
            *args, asynchronous=asynchronous, loop=loop, batch_size=batch_size
        )

    async def _rm_file(self, path, **kwargs):
        return await obs.delete_async(self.store, path)

    async def _cp_file(self, path1, path2, **kwargs):
        return await obs.copy_async(self.store, path1, path2)

    async def _pipe_file(self, path, value, **kwargs):
        return await obs.put_async(self.store, path, value)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        if start is None and end is None:
            resp = await obs.get_async(self.store, path)
            return await resp.bytes_async()

        return await obs.get_range_async(self.store, path, start=start, end=end)

    async def _cat_ranges(
        self,
        paths: List[str],
        starts: List[int] | int,
        ends: List[int] | int,
        max_gap=None,
        batch_size=None,
        on_error="return",
        **kwargs,
    ):
        if isinstance(starts, int):
            starts = [starts] * len(paths)
        if isinstance(ends, int):
            ends = [ends] * len(paths)
        if not len(paths) == len(starts) == len(ends):
            raise ValueError

        per_file_requests: Dict[str, List[Tuple[int, int, int]]] = defaultdict(list)
        for idx, (path, start, end) in enumerate(zip(paths, starts, ends)):
            per_file_requests[path].append((start, end, idx))

        futs: List[Coroutine[Any, Any, List[bytes]]] = []
        for path, ranges in per_file_requests.items():
            offsets = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            fut = obs.get_ranges_async(self.store, path, starts=offsets, ends=ends)
            futs.append(fut)

        result = await asyncio.gather(*futs)

        output_buffers: List[bytes] = [b""] * len(paths)
        for per_file_request, buffers in zip(per_file_requests.items(), result):
            path, ranges = per_file_request
            for buffer, ranges_ in zip(buffers, ranges):
                initial_index = ranges_[2]
                output_buffers[initial_index] = buffer.as_bytes()

        return output_buffers

    async def _put_file(self, lpath, rpath, **kwargs):
        with open(lpath, "rb") as f:
            await obs.put_async(self.store, rpath, f)

    async def _get_file(self, rpath, lpath, **kwargs):
        with open(lpath, "wb") as f:
            resp = await obs.get_async(self.store, rpath)
            async for buffer in resp.stream():
                f.write(buffer)

    async def _info(self, path, **kwargs):
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

    async def _ls(self, path, detail=True, **kwargs):
        result = await obs.list_with_delimiter_async(self.store, path)
        objects = result["objects"]
        prefs = result["common_prefixes"]
        if detail:
            return [
                {
                    "name": object["path"],
                    "size": object["size"],
                    "type": "file",
                    "e_tag": object["e_tag"],
                }
                for object in objects
            ] + [{"name": object, "size": 0, "type": "directory"} for object in prefs]
        else:
            return sorted([object["path"] for object in objects] + prefs)

    def _open(self, path, mode="rb", **kwargs):
        """Return raw bytes-mode file-like from the file-system"""
        return BufferedFileSimple(self, path, mode, **kwargs)


class BufferedFileSimple(fsspec.spec.AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError("Only 'rb' mode is currently supported")
        super().__init__(fs, path, mode, **kwargs)

    def read(self, length: int = -1):
        """Return bytes from the remote file

        length: if positive, returns up to this many bytes; if negative, return all
            remaining byets.
        """
        if length < 0:
            data = self.fs.cat_file(self.path, self.loc, self.size)
            self.loc = self.size
        else:
            data = self.fs.cat_file(self.path, self.loc, self.loc + length)
            self.loc += length
        return data
