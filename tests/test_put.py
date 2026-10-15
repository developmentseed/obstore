from __future__ import annotations

import itertools
from tempfile import TemporaryDirectory

import pytest

from obstore.exceptions import AlreadyExistsError
from obstore.store import LocalStore, MemoryStore


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


def test_put_sync_iterable_local_store():
    """Issue #450."""
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)

        b = b"the quick brown fox jumps over the lazy dog,"
        iterator = itertools.repeat(b, 50_000)
        data = b * 50_000
        path = "big-data.txt"

        store.put(path, iterator)

        assert store.get(path).bytes() == data


CHUNK_SIZE = 1024


@pytest.mark.parametrize("nbytes", [0, 1, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1])
def test_put_multipart_buffer_chunk_boundaries(nbytes: int):
    """Buffers are chunked correctly regardless of alignment to chunk_size."""
    store = MemoryStore()
    data = bytes(range(256)) * (nbytes // 256 + 1)
    data = data[:nbytes]

    store.put("file1.txt", data, use_multipart=True, chunk_size=CHUNK_SIZE)

    assert store.get("file1.txt").bytes() == data


@pytest.mark.parametrize("wrapper", [bytes, bytearray, memoryview])
def test_put_multipart_buffer_types(
    wrapper: type[bytes | bytearray | memoryview],
):
    """Any buffer-protocol input is uploaded verbatim."""
    store = MemoryStore()
    data = b"the quick brown fox jumps over the lazy dog," * 500

    store.put("file1.txt", wrapper(data), use_multipart=True, chunk_size=CHUNK_SIZE)

    assert store.get("file1.txt").bytes() == data


def test_put_multipart_buffer_slice():
    """A memoryview into the middle of a larger buffer uploads only its own bytes."""
    store = MemoryStore()
    backing = bytes(range(256)) * 40
    view = memoryview(backing)[1000:8000]

    store.put("file1.txt", view, use_multipart=True, chunk_size=CHUNK_SIZE)

    assert store.get("file1.txt").bytes() == backing[1000:8000]


def test_put_multipart_buffer_local_store():
    """The buffer multipart path round-trips through a store with real parts."""
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)
        data = b"the quick brown fox jumps over the lazy dog," * 500

        store.put("big-data.txt", data, use_multipart=True, chunk_size=CHUNK_SIZE)

        assert store.get("big-data.txt").bytes() == data
