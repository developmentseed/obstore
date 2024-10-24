import pytest

import obstore as obs
from obstore.store import MemoryStore


def test_stream_sync():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    obs.put(store, path, data)
    resp = obs.get(store, path)
    stream = resp.stream(min_chunk_size=0)

    # Note: it looks from manual testing that with the local store we're only getting
    # one chunk and not able to test the chunk sizing.
    pos = 0
    for chunk in stream:
        size = len(chunk)
        assert chunk == data[pos : pos + size]
        pos += size

    assert pos == len(data)


@pytest.mark.asyncio
async def test_stream_async():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    await obs.put_async(store, path, data)
    resp = await obs.get_async(store, path)
    stream = resp.stream(min_chunk_size=0)

    # Note: it looks from manual testing that with the local store we're only getting
    # one chunk and not able to test the chunk sizing.
    pos = 0
    async for chunk in stream:
        size = len(chunk)
        assert chunk == data[pos : pos + size]
        pos += size

    assert pos == len(data)


def test_get_with_options():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    obs.put(store, path, data)

    result = obs.get(store, path, options={"range": (5, 10)})
    assert result.range == (5, 10)
    buf = result.bytes()
    assert buf == data[5:10]


def test_get_range():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    obs.put(store, path, data)
    buffer = obs.get_range(store, path, 5, 10)
    view = memoryview(buffer)
    assert view == data[5:15]


def test_get_ranges():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    obs.put(store, path, data)
    offsets = [5, 10, 15, 20]
    lengths = [10, 10, 10, 10]
    buffers = obs.get_ranges(store, path, offsets, lengths)

    for offset, length, buffer in zip(offsets, lengths, buffers):
        assert memoryview(buffer) == data[offset : offset + length]
