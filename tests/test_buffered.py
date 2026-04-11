from io import BytesIO

import pytest

import obstore as obs
from obstore.store import MemoryStore


def test_readable_file_sync():
    store = MemoryStore()

    line = b"the quick brown fox jumps over the lazy dog\n"
    data = line * 5000
    path = "big-data.txt"

    store.put(path, data)

    file = obs.open_reader(store, path)
    assert line == file.readline().to_bytes()

    file = obs.open_reader(store, path)
    buffer = file.read()
    assert memoryview(data) == memoryview(buffer)

    file = obs.open_reader(store, path)
    assert line == file.readline().to_bytes()

    file = obs.open_reader(store, path)
    assert memoryview(data[:20]) == memoryview(file.read(20))


@pytest.mark.asyncio
async def test_readable_file_async():
    store = MemoryStore()

    line = b"the quick brown fox jumps over the lazy dog\n"
    data = line * 5000
    path = "big-data.txt"

    await obs.put_async(store, path, data)

    file = await obs.open_reader_async(store, path)
    assert line == (await file.readline()).to_bytes()

    file = await obs.open_reader_async(store, path)
    buffer = await file.read()
    assert memoryview(data) == memoryview(buffer)

    file = await obs.open_reader_async(store, path)
    assert line == (await file.readline()).to_bytes()

    file = await obs.open_reader_async(store, path)
    assert memoryview(data[:20]) == memoryview(await file.read(20))


def test_writable_file_sync():
    store = MemoryStore()

    line = b"the quick brown fox jumps over the lazy dog\n"
    path = "big-data.txt"
    with obs.open_writer(store, path) as writer:
        for _ in range(50):
            writer.write(line)

    retour = obs.get(store, path).bytes()
    assert retour == line * 50


@pytest.mark.asyncio
async def test_writable_file_async():
    store = MemoryStore()

    line = b"the quick brown fox jumps over the lazy dog\n"
    path = "big-data.txt"
    async with obs.open_writer_async(store, path) as writer:
        for _ in range(50):
            await writer.write(line)

    resp = await obs.get_async(store, path)
    retour = await resp.bytes_async()
    assert retour == line * 50


def test_read_past_eof_sync():
    store = MemoryStore()

    data = b"Hello, World!"
    path = "greeting.txt"
    obs.put(store, path, data)

    file = obs.open_reader(store, path)
    buffer = file.read(20)
    assert memoryview(data) == memoryview(buffer)

    buf = BytesIO(data)
    expected = buf.read(20)
    assert memoryview(expected) == memoryview(buffer)


@pytest.mark.asyncio
async def test_read_past_eof_async():
    store = MemoryStore()

    data = b"Hello, World!"
    path = "greeting.txt"
    await obs.put_async(store, path, data)

    file = await obs.open_reader_async(store, path)
    buffer = await file.read(20)
    assert memoryview(data) == memoryview(buffer)

    buf = BytesIO(data)
    expected = buf.read(20)
    assert memoryview(expected) == memoryview(buffer)


def test_open_reader_size_hint_sync():
    store = MemoryStore()
    data = b"x" * 1000
    path = "sized.bin"
    obs.put(store, path, data)

    file = obs.open_reader(store, path, size=len(data))
    assert file.size == len(data)
    assert memoryview(data) == memoryview(file.read())


@pytest.mark.asyncio
async def test_open_reader_size_hint_async():
    store = MemoryStore()
    data = b"x" * 1000
    path = "sized.bin"
    await obs.put_async(store, path, data)

    file = await obs.open_reader_async(store, path, size=len(data))
    assert file.size == len(data)
    assert memoryview(data) == memoryview(await file.read())


def test_open_reader_size_hint_larger_than_actual_errors_on_read():
    store = MemoryStore()
    data = b"x" * 1000
    path = "sized.bin"
    obs.put(store, path, data)

    file = obs.open_reader(store, path, size=5000)
    assert file.size == 5000
    with pytest.raises(OSError, match="range"):
        file.read()


def test_open_reader_size_hint_smaller_than_actual_truncates():
    store = MemoryStore()
    data = b"x" * 1000
    path = "sized.bin"
    obs.put(store, path, data)

    file = obs.open_reader(store, path, size=500)
    assert file.size == 500
    buffer = file.read()
    assert memoryview(data[:500]) == memoryview(buffer)


def test_open_reader_size_hint_zero_byte_file():
    store = MemoryStore()
    path = "empty.bin"
    obs.put(store, path, b"")

    file = obs.open_reader(store, path, size=0)
    assert file.size == 0
    assert memoryview(b"") == memoryview(file.read())


def test_open_reader_meta_last_modified_depends_on_size_hint():
    store = MemoryStore()
    data = b"x" * 1000
    path = "sized.bin"
    obs.put(store, path, data)

    hinted = obs.open_reader(store, path, size=len(data))
    unhinted = obs.open_reader(store, path)

    assert "last_modified" not in hinted.meta
    assert "last_modified" in unhinted.meta
    assert hinted.meta["size"] == len(data)
    assert unhinted.meta["size"] == len(data)
    assert hinted.meta["e_tag"] is None
    assert hinted.meta["path"] == path
