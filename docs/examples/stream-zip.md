# Streaming ZIP file creation

This example demonstrates how to create a zip archive from files in one store and upload it to another store using the [`stream_zip`](https://github.com/uktrade/stream-zip) library.

This never stores any entire source file or the target zip file in memory, so you can zip large files with low memory overhead.

## Example

!!! note

    This example is also [available on Github](https://github.com/developmentseed/obstore/blob/main/examples/stream-zip/README.md) if you'd like to test it out locally.

```py
from __future__ import annotations

import asyncio
from pathlib import Path
from stat import S_IFREG
from typing import TYPE_CHECKING

import stream_zip
from stream_zip import ZIP_32, AsyncMemberFile

from obstore.store import LocalStore, MemoryStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Iterable

    from obstore.store import ObjectStore


async def member_file(store: ObjectStore, path: str) -> AsyncMemberFile:
    """Create a member file for the zip archive."""
    resp = await store.get_async(path)
    last_modified = resp.meta["last_modified"]
    mode = S_IFREG | 0o644
    # Unclear why but we need to wrap the response in an async generator
    return (path, last_modified, mode, ZIP_32, (byte async for byte in resp.stream()))


async def member_files(
    store: ObjectStore,
    paths: Iterable[str],
) -> AsyncIterable[AsyncMemberFile]:
    """Create an async iterable of files for the zip archive."""
    for path in paths:
        yield await member_file(store, path)


async def zip_copy() -> None:
    """Copy files from one store into a zip archive that we upload to another store."""
    # Input store with source data
    input_store = MemoryStore()
    input_store.put("foo", b"hello")
    input_store.put("bar", b"world")

    # Output store where the zip file will be saved
    output_store = LocalStore(Path())

    # We can pass the streaming zip directly to `put`
    await output_store.put_async(
        "my.zip",
        stream_zip.async_stream_zip(
            member_files(input_store, ["foo", "bar"]),
            chunk_size=10 * 1024 * 1024,
        ),
    )
```

This creates a zip file in the current directory:

```
> unzip -l my.zip
Archive:  my.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        5  05-22-2025 13:37   foo
        5  05-22-2025 13:37   bar
---------                     -------
       10                     2 files
```

And we can read a file:

```
> unzip -p my.zip foo
hello
```
