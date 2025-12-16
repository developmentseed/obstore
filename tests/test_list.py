import tempfile
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from arro3.core import RecordBatch, Table

from obstore.store import LocalStore, MemoryStore


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


def test_list_substring_filtering():
    store = MemoryStore()

    # Add files with various patterns
    store.put("data/file1.txt", b"foo")
    store.put("data/test_file.txt", b"bar")
    store.put("data/another.csv", b"baz")
    store.put("data/test_data.json", b"qux")
    store.put("logs/test_log.txt", b"log")

    # Test substring filtering for files containing "test"
    result = store.list("data/test").collect()
    paths = [item["path"] for item in result]

    # Should match files with "test" in the filename within data/ directory
    assert "data/test_file.txt" in paths
    assert "data/test_data.json" in paths
    assert "data/file1.txt" not in paths
    assert "data/another.csv" not in paths
    assert "logs/test_log.txt" not in paths

    # Test with arrow format
    stream = store.list("data/test", return_arrow=True)
    batch = stream.collect()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 2


def test_list_substring_filtering_local_store():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        store = LocalStore(temp_dir_path)

        # Create directory structure
        data_dir = temp_dir_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # Write test files
        with (data_dir / "file1.txt").open("w") as f:
            f.write("foo")
        with (data_dir / "test_file.txt").open("w") as f:
            f.write("bar")
        with (data_dir / "another.csv").open("w") as f:
            f.write("baz")
        with (data_dir / "test_data.json").open("w") as f:
            f.write("qux")

        # Test substring filtering for files containing "test"
        result = store.list("data/test").collect()
        paths = [item["path"] for item in result]

        # Should match files with "test" in the filename within data/ directory
        assert "data/test_file.txt" in paths
        assert "data/test_data.json" in paths
        assert "data/file1.txt" not in paths
        assert "data/another.csv" not in paths


def test_list_as_arrow_to_polars():
    store = MemoryStore()

    for i in range(100):
        store.put(f"file{i}.txt", b"foo")

    stream = store.list(return_arrow=True, chunk_size=10)
    _pl_df = pl.DataFrame(next(stream))
    _df = pa.record_batch(next(stream))
