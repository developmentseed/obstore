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

Where possible, implementations should use the underlying `object-store-rs` APIs
directly. Only where this is not possible should users fall back to this fsspec
integration.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Coroutine, Dict, List, Tuple

import fsspec.asyn

import object_store_rs as obs

if TYPE_CHECKING:
    from object_store_rs.store import ObjectStore


class AsyncFsspecStore(fsspec.asyn.AsyncFileSystem):
    store: ObjectStore

    def __init__(
        self,
        store: ObjectStore,
        *args,
        asynchronous=False,
        loop=None,
        batch_size=None,
        **kwargs,
    ):
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
            return await obs.get_range_async(
                self.store, path, offset=start, length=end - start
            )

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
        # TODO: need to go through this again and test it
        per_file_requests: Dict[str, List[Tuple[int, int, int]]] = defaultdict(list)
        for idx, (path, start, end) in enumerate(zip(paths, starts, ends)):
            per_file_requests[path].append((start, end, idx))

        futs: List[Coroutine[Any, Any, List[bytes]]] = []
        for path, ranges in per_file_requests.items():
            offsets = [r[0] for r in ranges]
            lengths = [r[1] - r[0] for r in ranges]
            fut = obs.get_ranges_async(
                self.store, path, offsets=offsets, lengths=lengths
            )
            futs.append(fut)

        result = await asyncio.gather(*futs)

        output_buffers: List[bytes] = [b""] * len(paths)
        for per_file_request, buffers in zip(per_file_requests.items(), result):
            path, ranges = per_file_request
            for buffer, ranges_ in zip(buffers, ranges):
                initial_index = ranges_[2]
                output_buffers[initial_index] = buffer

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
        if detail:
            raise NotImplementedError("Not sure how to format these dicts")

        result = await obs.list_with_delimiter_async(self.store, path)
        objects = result["objects"]
        return [object["path"] for object in objects]
