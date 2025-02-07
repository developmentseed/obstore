import pytest
from arro3.core import RecordBatch, Table

import obstore as obs
from obstore.store import MemoryStore


def test_list():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo")
    obs.put(store, "file2.txt", b"bar")
    obs.put(store, "file3.txt", b"baz")

    stream = obs.list(store)
    result = stream.collect()
    assert len(result) == 3


def test_list_as_arrow():
    store = MemoryStore()

    for i in range(100):
        obs.put(store, f"file{i}.txt", b"foo")

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    yielded_batches = 0
    for batch in stream:
        assert isinstance(batch, RecordBatch)
        yielded_batches += 1
        assert batch.num_rows == 10

    assert yielded_batches == 10

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    batch = stream.collect()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 100


@pytest.mark.asyncio
async def test_list_stream_async():
    store = MemoryStore()

    for i in range(100):
        await obs.put_async(store, f"file{i}.txt", b"foo")

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    yielded_batches = 0
    async for batch in stream:
        assert isinstance(batch, RecordBatch)
        yielded_batches += 1
        assert batch.num_rows == 10

    assert yielded_batches == 10

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    batch = await stream.collect_async()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 100


def test_list_with_delimiter():
    store = MemoryStore()

    obs.put(store, "a/file1.txt", b"foo")
    obs.put(store, "a/file2.txt", b"bar")
    obs.put(store, "b/file3.txt", b"baz")

    list_result1 = obs.list_with_delimiter(store)
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert list_result1["objects"] == []

    list_result2 = obs.list_with_delimiter(store, "a")
    assert list_result2["common_prefixes"] == []
    assert list_result2["objects"][0]["path"] == "a/file1.txt"
    assert list_result2["objects"][1]["path"] == "a/file2.txt"

    list_result3 = obs.list_with_delimiter(store, "b")
    assert list_result3["common_prefixes"] == []
    assert list_result3["objects"][0]["path"] == "b/file3.txt"

    # Test returning arrow
    list_result1 = obs.list_with_delimiter(store, return_arrow=True)
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert list_result1["objects"].num_rows == 0
    assert isinstance(list_result1["objects"], Table)

    list_result2 = obs.list_with_delimiter(store, "a", return_arrow=True)
    assert list_result2["common_prefixes"] == []
    assert list_result2["objects"].num_rows == 2
    assert list_result2["objects"]["path"][0].as_py() == "a/file1.txt"
    assert list_result2["objects"]["path"][1].as_py() == "a/file2.txt"


@pytest.mark.asyncio
async def test_list_with_delimiter_async():
    store = MemoryStore()

    await obs.put_async(store, "a/file1.txt", b"foo")
    await obs.put_async(store, "a/file2.txt", b"bar")
    await obs.put_async(store, "b/file3.txt", b"baz")

    list_result1 = await obs.list_with_delimiter_async(store)
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert list_result1["objects"] == []

    list_result2 = await obs.list_with_delimiter_async(store, "a")
    assert list_result2["common_prefixes"] == []
    assert list_result2["objects"][0]["path"] == "a/file1.txt"
    assert list_result2["objects"][1]["path"] == "a/file2.txt"

    list_result3 = await obs.list_with_delimiter_async(store, "b")
    assert list_result3["common_prefixes"] == []
    assert list_result3["objects"][0]["path"] == "b/file3.txt"

    # Test returning arrow
    list_result1 = await obs.list_with_delimiter_async(store, return_arrow=True)
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert list_result1["objects"].num_rows == 0
    assert isinstance(list_result1["objects"], Table)

    list_result2 = await obs.list_with_delimiter_async(store, "a", return_arrow=True)
    assert list_result2["common_prefixes"] == []
    assert list_result2["objects"].num_rows == 2
    assert list_result2["objects"]["path"][0].as_py() == "a/file1.txt"
    assert list_result2["objects"]["path"][1].as_py() == "a/file2.txt"
