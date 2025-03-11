import itertools

import pytest

from obstore.exceptions import AlreadyExistsError
from obstore.store import MemoryStore


def test_put_non_multipart():
    store = MemoryStore()

    store.put("file1.txt", b"foo", use_multipart=False)
    assert store.get("file1.txt").bytes() == b"foo"


def test_put_non_multipart_sync_iterable():
    store = MemoryStore()

    b = b"the quick brown fox jumps over the lazy dog,"
    iterator = itertools.repeat(b, 5)
    store.put("file1.txt", iterator, use_multipart=False)
    assert store.get("file1.txt").bytes() == (b * 5)


@pytest.mark.asyncio
async def test_put_non_multipart_async_iterable():
    store = MemoryStore()

    b = b"the quick brown fox jumps over the lazy dog,"

    async def it():
        for _ in range(5):
            yield b"the quick brown fox jumps over the lazy dog,"

    await store.put_async("file1.txt", it(), use_multipart=False)
    assert store.get("file1.txt").bytes() == (b * 5)


def test_put_multipart_one_chunk():
    store = MemoryStore()

    store.put("file1.txt", b"foo", use_multipart=True)
    assert store.get("file1.txt").bytes() == b"foo"


def test_put_multipart_large():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    store.put(path, data, use_multipart=True)
    assert store.get(path).bytes() == data


def test_put_mode():
    store = MemoryStore()

    store.put("file1.txt", b"foo")
    store.put("file1.txt", b"bar", mode="overwrite")

    with pytest.raises(AlreadyExistsError):
        store.put("file1.txt", b"foo", mode="create")

    assert store.get("file1.txt").bytes() == b"bar"


@pytest.mark.asyncio
async def test_put_async_iterable():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 50_000
    path = "big-data.txt"

    await store.put_async(path, data)

    resp = await store.get_async(path)
    stream = resp.stream(min_chunk_size=0)
    new_path = "new-path.txt"
    await store.put_async(new_path, stream)

    assert store.get(new_path).bytes() == data


def test_put_sync_iterable():
    store = MemoryStore()

    b = b"the quick brown fox jumps over the lazy dog,"
    iterator = itertools.repeat(b, 50_000)
    data = b * 50_000
    path = "big-data.txt"

    store.put(path, iterator)

    assert store.get(path).bytes() == data
