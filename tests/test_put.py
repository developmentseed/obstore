import itertools

import pytest

import obstore as obs
from obstore.exceptions import AlreadyExistsError
from obstore.store import MemoryStore


def test_put_non_multipart():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo", use_multipart=False)
    assert obs.get(store, "file1.txt").bytes() == b"foo"


def test_put_multipart_one_chunk():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo", use_multipart=True)
    assert obs.get(store, "file1.txt").bytes() == b"foo"


def test_put_multipart_large():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    obs.put(store, path, data, use_multipart=True)
    assert obs.get(store, path).bytes() == data


def test_put_mode():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo")
    obs.put(store, "file1.txt", b"bar", mode="overwrite")

    with pytest.raises(AlreadyExistsError):
        obs.put(store, "file1.txt", b"foo", mode="create")

    assert obs.get(store, "file1.txt").bytes() == b"bar"


@pytest.mark.asyncio
async def test_put_async_iterable():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 50_000
    path = "big-data.txt"

    await obs.put_async(store, path, data)

    resp = await obs.get_async(store, path)
    stream = resp.stream(min_chunk_size=0)
    new_path = "new-path.txt"
    await obs.put_async(store, new_path, stream)

    assert obs.get(store, new_path).bytes() == data


def test_put_sync_iterable():
    store = MemoryStore()

    b = b"the quick brown fox jumps over the lazy dog,"
    iterator = itertools.repeat(b, 50_000)
    data = b * 50_000
    path = "big-data.txt"

    obs.put(store, path, iterator)

    assert obs.get(store, path).bytes() == data
