from __future__ import annotations

from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import pytest

from obstore.store import LocalStore, MemoryStore, S3Store

if TYPE_CHECKING:
    from obstore.store import ClientConfig, S3Config


def test_delete_one():
    store = MemoryStore()

    store.put("file1.txt", b"foo")
    store.put("file2.txt", b"bar")
    store.put("file3.txt", b"baz")

    assert len(store.list().collect()) == 3
    store.delete("file1.txt")
    store.delete("file2.txt")
    store.delete("file3.txt")
    assert len(store.list().collect()) == 0


@pytest.mark.asyncio
async def test_delete_async():
    store = MemoryStore()

    await store.put_async("file1.txt", b"foo")
    result = await store.delete_async("file1.txt")
    assert result is None


def test_delete_many():
    store = MemoryStore()

    store.put("file1.txt", b"foo")
    store.put("file2.txt", b"bar")
    store.put("file3.txt", b"baz")

    assert len(store.list().collect()) == 3
    store.delete(
        ["file1.txt", "file2.txt", "file3.txt"],
    )
    assert len(store.list().collect()) == 0


# Local filesystem errors if the file does not exist.
def test_delete_one_local_fs():
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)

        store.put("file1.txt", b"foo")
        store.put("file2.txt", b"bar")
        store.put("file3.txt", b"baz")

        assert len(store.list().collect()) == 3
        store.delete("file1.txt")
        store.delete("file2.txt")
        store.delete("file3.txt")
        assert len(store.list().collect()) == 0

        with pytest.raises(FileNotFoundError):
            store.delete("file1.txt")


def test_delete_many_local_fs():
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)

        store.put("file1.txt", b"foo")
        store.put("file2.txt", b"bar")
        store.put("file3.txt", b"baz")

        assert len(store.list().collect()) == 3
        store.delete(
            ["file1.txt", "file2.txt", "file3.txt"],
        )

        with pytest.raises(FileNotFoundError):
            store.delete(
                ["file1.txt", "file2.txt", "file3.txt"],
            )


@pytest.mark.asyncio
def test_delete_prefix(minio_bucket: tuple[S3Config, ClientConfig]):
    # https://github.com/developmentseed/obstore/issues/628
    # We validate that prefix is set on both upload and delete
    store = S3Store(
        config=minio_bucket[0],
        client_options=minio_bucket[1],
        prefix="test-prefix/",
    )

    store.put("file1.txt", b"foo")

    assert len(store.list().collect()) == 1

    store.delete("file1.txt")
    assert len(store.list().collect()) == 0
