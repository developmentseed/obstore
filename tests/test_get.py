import object_store_rs as obs
import pytest
from object_store_rs.store import MemoryStore


def test_stream_sync():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    obs.put_file(store, path, data)
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

    await obs.put_file_async(store, path, data)
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
