from __future__ import annotations

import pytest

from obstore.store import MemoryStore


def test_stream_sync():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    store.put(path, data)
    resp = store.get(path)
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

    await store.put_async(path, data)
    resp = await store.get_async(path)
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

    store.put(path, data)

    result = store.get(path, options={"range": (5, 10)})
    assert result.range == (5, 10)
    buf = result.bytes()
    assert buf == data[5:10]

    # Test list input
    result = store.get(path, options={"range": [5, 10]})
    assert result.range == (5, 10)
    buf = result.bytes()
    assert buf == data[5:10]


def test_get_with_options_offset():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    store.put(path, data)

    result = store.get(path, options={"range": {"offset": 100}})
    result_range = result.range
    assert result_range == (100, 4400)
    buf = result.bytes()
    assert buf == data[result_range[0] : result_range[1]]


def test_get_with_options_suffix():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    store.put(path, data)

    result = store.get(path, options={"range": {"suffix": 100}})
    result_range = result.range
    assert result_range == (4300, 4400)
    buf = result.bytes()
    assert buf == data[result_range[0] : result_range[1]]


def test_get_range():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    store.put(path, data)
    buffer = store.get_range(path, start=5, end=15)
    view = memoryview(buffer)
    assert view == data[5:15]

    buffer = store.get_range(path, start=5, length=10)
    view = memoryview(buffer)
    assert view == data[5:15]


def test_get_ranges():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    store.put(path, data)

    starts = [5, 10, 15, 20]
    ends = [15, 20, 25, 30]
    buffers = store.get_ranges(path, starts=starts, ends=ends)

    # set strict=True when we upgrade to 3.10
    for start, end, buffer in zip(starts, ends, buffers):
        assert memoryview(buffer) == data[start:end]

    lengths = [10, 10, 10, 10]
    buffers = store.get_ranges(path, starts=starts, lengths=lengths)

    # set strict=True when we upgrade to 3.10
    for start, end, buffer in zip(starts, ends, buffers):
        assert memoryview(buffer) == data[start:end]


COALESCE_CASES = [
    # (starts, ends, coalesce) — close ranges
    ([5, 10, 15, 20], [15, 20, 25, 30], 0),
    ([5, 10, 15, 20], [15, 20, 25, 30], 1024 * 1024),
    # widely-spaced ranges
    ([0, 1000, 2000, 3000], [10, 1010, 2010, 3010], 0),
    ([0, 1000, 2000, 3000], [10, 1010, 2010, 3010], 500),
    ([0, 1000, 2000, 3000], [10, 1010, 2010, 3010], 2000),
]


@pytest.mark.parametrize(("starts", "ends", "coalesce"), COALESCE_CASES)
def test_get_ranges_with_coalesce(
    starts: list[int],
    ends: list[int],
    coalesce: int | None,
):
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    store.put(path, data)

    buffers = store.get_ranges(path, starts=starts, ends=ends, coalesce=coalesce)
    for start, end, buffer in zip(starts, ends, buffers):
        assert memoryview(buffer) == data[start:end]


@pytest.mark.asyncio
@pytest.mark.parametrize(("starts", "ends", "coalesce"), COALESCE_CASES)
async def test_get_ranges_async_with_coalesce(
    starts: list[int],
    ends: list[int],
    coalesce: int | None,
):
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"

    await store.put_async(path, data)

    buffers = await store.get_ranges_async(
        path,
        starts=starts,
        ends=ends,
        coalesce=coalesce,
    )
    for start, end, buffer in zip(starts, ends, buffers):
        assert memoryview(buffer) == data[start:end]


def test_get_range_invalid_range():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"
    store.put(path, data)

    with pytest.raises(ValueError, match="Invalid range"):
        store.get_range(path, start=10, end=10)

    with pytest.raises(ValueError, match="Invalid range"):
        store.get_range(path, start=10, end=8)

    with pytest.raises(ValueError, match="Invalid range"):
        store.get_range(path, start=10, length=0)


def test_get_ranges_invalid_range():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "big-data.txt"
    store.put(path, data)

    with pytest.raises(ValueError, match="Invalid range"):
        store.get_ranges(path, starts=[10], ends=[10])

    with pytest.raises(ValueError, match="Invalid range"):
        store.get_ranges(path, starts=[10, 20], ends=[18, 18])

    with pytest.raises(ValueError, match="Invalid range"):
        store.get_ranges(path, starts=[10, 20], lengths=[10, 0])


def test_access_getresult_attributes_after_reading_stream():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 100
    path = "data.txt"
    store.put(path, data)

    resp = store.get(path)

    _buffer = resp.bytes()
    # Validate that we can access the range _after_ consuming the buffer
    r = resp.range
    assert r == (0, 4400)
