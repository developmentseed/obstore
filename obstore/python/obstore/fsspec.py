"""Fsspec integration.

The underlying `object_store` Rust crate cautions against relying too strongly on stateful filesystem representations of object stores:

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
        **kwargs,
    ):
        """
        store: a configured instance of one of the store classes in objstore.store
        asynchronous: id this instance meant to be be called using the async API? This
            should only be set to true when running within a coroutine
        loop: since both fsspec/python and tokio/rust may be using loops, this should
            be kept None for now
        batch_size: some operations on many files will batch their requests; if you
            are seeing timeouts, you may want to set this number smaller than the defaults,
            which are determined in fsspec.asyn._get_batch_size
        kwargs: not currently supported; extra configuration for the backend should be
            done to the Store passed in the first argument.
        """

        self.store = store
        super().__init__(
            *args, asynchronous=asynchronous, loop=loop, batch_size=batch_size, **kwargs
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

        if start is not None and end is not None:
            return await obs.get_range_async(self.store, path, start=start, end=end)

        raise NotImplementedError("todo: handle open-ended ranges")

    async def _cat_ranges(
        self,
        paths: List[str],
        starts: List[int],
        ends: List[int],
        max_gap=None,
        batch_size=None,
        on_error="return",
        **kwargs,
    ):
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
        return fsspec.spec.AbstractBufferedFile(self, path, mode, **kwargs)
