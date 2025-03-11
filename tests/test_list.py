import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from arro3.core import RecordBatch, Table

from obstore.store import MemoryStore


def test_list():
    store = MemoryStore()

    store.put("file1.txt", b"foo")
    store.put("file2.txt", b"bar")
    store.put("file3.txt", b"baz")

    stream = store.list()
    result = stream.collect()
    assert len(result) == 3


def test_list_as_arrow():
    store = MemoryStore()

    for i in range(100):
        store.put(f"file{i}.txt", b"foo")

    stream = store.list(return_arrow=True, chunk_size=10)
    yielded_batches = 0
    for batch in stream:
        assert isinstance(batch, RecordBatch)
        yielded_batches += 1
        assert batch.num_rows == 10

    assert yielded_batches == 10

    stream = store.list(return_arrow=True, chunk_size=10)
    batch = stream.collect()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 100


@pytest.mark.asyncio
async def test_list_stream_async():
    store = MemoryStore()

    for i in range(100):
        await store.put_async(f"file{i}.txt", b"foo")

    stream = store.list(return_arrow=True, chunk_size=10)
    yielded_batches = 0
    async for batch in stream:
        assert isinstance(batch, RecordBatch)
        yielded_batches += 1
        assert batch.num_rows == 10

    assert yielded_batches == 10

    stream = store.list(return_arrow=True, chunk_size=10)
    batch = await stream.collect_async()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 100


def test_list_with_delimiter():
    store = MemoryStore()

    store.put("a/file1.txt", b"foo")
    store.put("a/file2.txt", b"bar")
    store.put("b/file3.txt", b"baz")

    list_result1 = store.list_with_delimiter()
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert list_result1["objects"] == []

    list_result2 = store.list_with_delimiter("a")
    assert list_result2["common_prefixes"] == []
    assert list_result2["objects"][0]["path"] == "a/file1.txt"
    assert list_result2["objects"][1]["path"] == "a/file2.txt"

    list_result3 = store.list_with_delimiter("b")
    assert list_result3["common_prefixes"] == []
    assert list_result3["objects"][0]["path"] == "b/file3.txt"

    # Test returning arrow
    list_result1 = store.list_with_delimiter(return_arrow=True)
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert Table(list_result1["objects"]).num_rows == 0
    assert isinstance(list_result1["objects"], Table)

    list_result2 = store.list_with_delimiter("a", return_arrow=True)
    assert list_result2["common_prefixes"] == []
    objects = Table(list_result2["objects"])
    assert objects.num_rows == 2
    assert objects["path"][0].as_py() == "a/file1.txt"
    assert objects["path"][1].as_py() == "a/file2.txt"


@pytest.mark.asyncio
async def test_list_with_delimiter_async():
    store = MemoryStore()

    await store.put_async("a/file1.txt", b"foo")
    await store.put_async("a/file2.txt", b"bar")
    await store.put_async("b/file3.txt", b"baz")

    list_result1 = await store.list_with_delimiter_async()
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert list_result1["objects"] == []

    list_result2 = await store.list_with_delimiter_async("a")
    assert list_result2["common_prefixes"] == []
    assert list_result2["objects"][0]["path"] == "a/file1.txt"
    assert list_result2["objects"][1]["path"] == "a/file2.txt"

    list_result3 = await store.list_with_delimiter_async("b")
    assert list_result3["common_prefixes"] == []
    assert list_result3["objects"][0]["path"] == "b/file3.txt"

    # Test returning arrow
    list_result1 = await store.list_with_delimiter_async(return_arrow=True)
    assert list_result1["common_prefixes"] == ["a", "b"]
    assert Table(list_result1["objects"]).num_rows == 0
    assert isinstance(list_result1["objects"], Table)

    list_result2 = await store.list_with_delimiter_async("a", return_arrow=True)
    assert list_result2["common_prefixes"] == []
    objects = Table(list_result2["objects"])
    assert objects.num_rows == 2
    assert objects["path"][0].as_py() == "a/file1.txt"
    assert objects["path"][1].as_py() == "a/file2.txt"


def test_list_as_arrow_to_polars():
    store = MemoryStore()

    for i in range(100):
        store.put(f"file{i}.txt", b"foo")

    stream = store.list(return_arrow=True, chunk_size=10)
    _pl_df = pl.DataFrame(next(stream))
    _df = pa.record_batch(next(stream)).to_pandas(types_mapper=pd.ArrowDtype)
