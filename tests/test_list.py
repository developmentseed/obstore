import tempfile
from pathlib import Path

import pyarrow as pa
import pytest
from arro3.core import RecordBatch, Table

from obstore.store import LocalStore, MemoryStore, ObjectStore, S3Store


def test_list():
    store = MemoryStore()

    store.put("file1.txt", b"foo")
    store.put("file2.txt", b"bar")
    store.put("file3.txt", b"baz")

    result = store.list().collect()
    assert len(result) == 3


def test_list_non_ascii():
    store = MemoryStore()

    name1 = "café.txt"
    name2 = "ümlaut.txt"
    name3 = "こんにちは世界.txt"
    store.put(name1, b"foo")
    store.put(name2, b"bar")
    store.put(name3, b"baz")

    result = store.list().collect()
    assert len(result) == 3
    assert result[0]["path"] == name1
    assert result[1]["path"] == name2
    assert result[2]["path"] == name3


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


def test_list_non_ascii_arrow():
    store = MemoryStore()

    name1 = "café.txt"
    name2 = "ümlaut.txt"
    name3 = "こんにちは世界.txt"
    store.put(name1, b"foo")
    store.put(name2, b"bar")
    store.put(name3, b"baz")

    result = store.list(return_arrow=True).collect()
    assert result.num_rows == 3
    assert result["path"][0].as_py() == name1
    assert result["path"][1].as_py() == name2
    assert result["path"][2].as_py() == name3


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


def test_list_substring_filtering_emulated():
    store = MemoryStore()

    # Add files with various patterns
    store.put("data/file1.txt", b"foo")
    store.put("data/test/file.txt", b"bar")
    store.put("data/test/deep/file.txt", b"bar")
    store.put("data/another/2.csv", b"baz")
    store.put("data/test_data.json", b"qux")
    store.put("logs/test_log.txt", b"log")

    # The prefix is a raw string prefix (not a whole path segment), and matching is
    # recursive: every key starting with "data/tes" is returned, including nested ones.
    result = store.list("data/tes").collect()
    paths = {item["path"] for item in result}
    assert paths == {
        "data/test/file.txt",
        "data/test/deep/file.txt",
        "data/test_data.json",
    }

    # Same filter, returned as arrow.
    batch = store.list("data/tes", return_arrow=True).collect()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 3


def test_list_substring_filtering_local_store():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        store = LocalStore(temp_dir_path)

        # Create directory structure, including a nested directory under a matching
        # prefix to verify recursive listing.
        data_dir = temp_dir_path / "data"
        (data_dir / "test_sub").mkdir(parents=True, exist_ok=True)

        (data_dir / "file1.txt").write_text("foo")
        (data_dir / "test_file.txt").write_text("bar")
        (data_dir / "another.csv").write_text("baz")
        (data_dir / "test_data.json").write_text("qux")
        (data_dir / "test_sub" / "deep.txt").write_text("deep")

        # Matching is recursive and on a raw string prefix, so the nested
        # "data/test_sub/deep.txt" is included.
        result = store.list("data/test").collect()
        paths = {item["path"] for item in result}
        assert paths == {
            "data/test_file.txt",
            "data/test_data.json",
            "data/test_sub/deep.txt",
        }


def _assert_substring_prefix_listing(store: ObjectStore):
    """Test `list` substring-prefix behavior across native and emulated backends."""
    for path in [
        "data/file1.txt",
        "data/test/file.txt",
        "data/test/deep/file.txt",
        "data/another/2.csv",
        "data/test_data.json",
        "logs/test_log.txt",
    ]:
        store.put(path, b"x")

    paths = {item["path"] for item in store.list("data/tes").collect()}
    assert paths == {
        "data/test/file.txt",
        "data/test/deep/file.txt",
        "data/test_data.json",
    }


def test_list_substring_prefix_emulated():
    _assert_substring_prefix_listing(MemoryStore())


def test_list_substring_prefix_native(minio_store: S3Store):
    # `minio_store` is an S3Store backed by a real (paginating) MinIO container.
    _assert_substring_prefix_listing(minio_store)


def test_list_as_arrow_to_pyarrow():
    store = MemoryStore()

    for i in range(100):
        store.put(f"file{i}.txt", b"foo")

    stream = store.list(return_arrow=True, chunk_size=10)

    # The RecordBatch yielded by the stream implements the Arrow PyCapsule interface,
    # so external Arrow libraries can consume it zero-copy.
    batch = next(stream)
    assert isinstance(batch, RecordBatch)

    pa_batch = pa.record_batch(batch)
    assert pa_batch.num_rows == 10
    assert "path" in pa_batch.column_names
