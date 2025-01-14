import pytest

import obstore as obs
from obstore.store import MemoryStore


def test_readable_file_sync():
    store = MemoryStore()

    line = b"the quick brown fox jumps over the lazy dog\n"
    data = line * 5000
    path = "big-data.txt"

    obs.put(store, path, data)

    file = obs.open(store, path)
    assert line == file.readline().to_bytes()

    file = obs.open(store, path)
    buffer = file.read()
    assert memoryview(data) == memoryview(buffer)

    file = obs.open(store, path)
    assert line == file.readline().to_bytes()

    file = obs.open(store, path)
    assert memoryview(data[:20]) == memoryview(file.read(20))


@pytest.mark.asyncio
async def test_readable_file_async():
    store = MemoryStore()

    line = b"the quick brown fox jumps over the lazy dog\n"
    data = line * 5000
    path = "big-data.txt"

    await obs.put_async(store, path, data)

    file = await obs.open_async(store, path)
    assert line == (await file.readline()).to_bytes()

    file = await obs.open_async(store, path)
    buffer = await file.read()
    assert memoryview(data) == memoryview(buffer)

    file = await obs.open_async(store, path)
    assert line == (await file.readline()).to_bytes()

    file = await obs.open_async(store, path)
    assert memoryview(data[:20]) == memoryview(await file.read(20))
