from tempfile import TemporaryDirectory

import pytest

import obstore as obs
from obstore.store import LocalStore, MemoryStore


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

    await obs.put_async(store, "file1.txt", b"foo")
    result = await obs.delete_async(store, "file1.txt")
    assert result is None


def test_delete_many():
    store = MemoryStore()

    store.put("file1.txt", b"foo")
    store.put("file2.txt", b"bar")
    store.put("file3.txt", b"baz")

    assert len(store.list().collect()) == 3
    obs.delete(
        store,
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
        obs.delete(store, "file1.txt")
        obs.delete(store, "file2.txt")
        obs.delete(store, "file3.txt")
        assert len(store.list().collect()) == 0

        with pytest.raises(FileNotFoundError):
            obs.delete(store, "file1.txt")


def test_delete_many_local_fs():
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)

        store.put("file1.txt", b"foo")
        store.put("file2.txt", b"bar")
        store.put("file3.txt", b"baz")

        assert len(store.list().collect()) == 3
        obs.delete(
            store,
            ["file1.txt", "file2.txt", "file3.txt"],
        )

        with pytest.raises(FileNotFoundError):
            obs.delete(
                store,
                ["file1.txt", "file2.txt", "file3.txt"],
            )
