from __future__ import annotations

import gc
import os
from typing import TYPE_CHECKING
from unittest.mock import patch

import fsspec
import pyarrow.parquet as pq
import pytest
from fsspec.registry import _registry

from obstore.fsspec import AsyncFsspecStore, register
from tests.conftest import TEST_BUCKET_NAME

if TYPE_CHECKING:
    from obstore.store import S3Config


@pytest.fixture
def fs(s3_store_config: S3Config):
    register("s3")
    return fsspec.filesystem("s3", config=s3_store_config)


@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup function to run after each test."""
    yield  # Runs the test first

    # clear the registered implementations after each test
    _registry.clear()

    gc.collect()


def test_register():
    """Test if register() creates and registers a subclass for a given protocol."""
    register("s3")  # Register the "s3" protocol dynamically
    fs_class = fsspec.get_filesystem_class("s3")

    assert issubclass(
        fs_class,
        AsyncFsspecStore,
    ), "Registered class should be a subclass of AsyncFsspecStore"
    assert fs_class.protocol == "s3", (
        "Registered class should have the correct protocol"
    )

    # Ensure a new instance of the registered store can be created
    fs_instance = fs_class()
    assert isinstance(
        fs_instance,
        AsyncFsspecStore,
    ), "Registered class should be instantiable"

    # test register asynchronous
    register("gcs", asynchronous=True)  # Register the "s3" protocol dynamically
    fs_class = fsspec.get_filesystem_class("gcs")
    assert fs_class.asynchronous, "Registered class should be asynchronous"

    # test multiple registrations
    register(["file", "abfs"])
    assert issubclass(fsspec.get_filesystem_class("file"), AsyncFsspecStore)
    assert issubclass(fsspec.get_filesystem_class("abfs"), AsyncFsspecStore)


def test_register_invalid_types():
    """Test that register rejects invalid input types."""
    with pytest.raises(
        TypeError,
        match="'int' object is not iterable",
    ):
        register(123)

    with pytest.raises(TypeError, match="Protocol must be a string, got int"):
        register(["test", 42])  # List contains a non-string

    with pytest.raises(
        ValueError,
        match="Protocol must be a non-empty string",
    ):
        register(["test1", ""])  # List contains a non-string

    with pytest.raises(
        ValueError,
        match="Protocol must be a non-empty string or list",
    ):
        register(None)  # None is invalid

    with pytest.raises(
        ValueError,
        match="Protocol must be a non-empty string or list",
    ):
        register([])  # Empty list is invalid


def test_construct_store_cache_diff_bucket_name(s3_store_config: S3Config):
    register("s3")
    fs: AsyncFsspecStore = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        asynchronous=True,
        max_cache_size=5,
    )

    bucket_names = [f"bucket{i}" for i in range(20)]  # 20 unique buckets

    with patch.object(
        fs,
        "_construct_store",
        wraps=fs._construct_store,
    ) as mock_construct:
        for bucket in bucket_names:
            fs._construct_store(bucket)

        # Since the cache is set to 16, only the first 16 unique calls should be cached
        assert mock_construct.cache_info().currsize == 5, (
            "Cache should only store 5 cache"
        )
        assert mock_construct.cache_info().hits == 0, "Cache should hits 0 times"
        assert mock_construct.cache_info().misses == 20, "Cache should miss 20 times"

    # test garbage collector
    fs = None
    assert gc.collect() > 0


def test_construct_store_cache_same_bucket_name(s3_store_config: S3Config):
    register("s3")
    fs = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        asynchronous=True,
        max_cache_size=5,
    )

    bucket_names = ["bucket" for _ in range(20)]

    with patch.object(
        fs,
        "_construct_store",
        wraps=fs._construct_store,
    ) as mock_construct:
        for bucket in bucket_names:
            fs._construct_store(bucket)

        assert mock_construct.cache_info().currsize == 1, (
            "Cache should only store 1 cache"
        )
        assert mock_construct.cache_info().hits == 20 - 1, (
            "Cache should hits 20-1 times"
        )
        assert mock_construct.cache_info().misses == 1, "Cache should only miss once"

    # test garbage collector
    fs = None
    assert gc.collect() > 0


def test_fsspec_filesystem_cache(s3_store_config: S3Config):
    """Test caching behavior of fsspec.filesystem with the _Cached metaclass."""
    register("s3")

    # call fsspec.filesystem() multiple times with the same parameters
    fs1 = fsspec.filesystem("s3", config=s3_store_config)
    fs2 = fsspec.filesystem("s3", config=s3_store_config)

    # Same parameters should return the same instance
    assert fs1 is fs2, (
        "fsspec.filesystem() with the same parameters should return the cached instance"
    )

    # Changing parameters should create a new instance
    fs3 = fsspec.filesystem("s3", config=s3_store_config, asynchronous=True)
    assert fs1 is not fs3, (
        "fsspec.filesystem() with different parameters should return a new instance"
    )


def test_list(fs: AsyncFsspecStore):
    out = fs.ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [f"{TEST_BUCKET_NAME}/afile"]
    fs.pipe_file(f"{TEST_BUCKET_NAME}/dir/bfile", b"data")
    out = fs.ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [f"{TEST_BUCKET_NAME}/afile", f"{TEST_BUCKET_NAME}/dir"]
    out = fs.ls(f"{TEST_BUCKET_NAME}", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


@pytest.mark.asyncio
async def test_list_async(s3_store_config: S3Config):
    register("s3")
    fs = fsspec.filesystem("s3", config=s3_store_config, asynchronous=True)

    out = await fs._ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [f"{TEST_BUCKET_NAME}/afile"]
    await fs._pipe_file(f"{TEST_BUCKET_NAME}/dir/bfile", b"data")
    out = await fs._ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [f"{TEST_BUCKET_NAME}/afile", f"{TEST_BUCKET_NAME}/dir"]
    out = await fs._ls(f"{TEST_BUCKET_NAME}", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


@pytest.mark.network
def test_remote_parquet():
    register("https")
    fs = fsspec.filesystem("https")
    url = "github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"  # noqa: E501
    pq.read_metadata(url, filesystem=fs)

    # also test with full url
    url = "https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
    pq.read_metadata(url, filesystem=fs)


def test_multi_file_ops(fs: AsyncFsspecStore):
    data = {
        f"{TEST_BUCKET_NAME}/dir/test1": b"test data1",
        f"{TEST_BUCKET_NAME}/dir/test2": b"test data2",
    }
    fs.pipe(data)
    out = fs.cat(list(data))
    assert out == data
    out = fs.cat(f"{TEST_BUCKET_NAME}/dir", recursive=True)
    assert out == data
    fs.cp(f"{TEST_BUCKET_NAME}/dir", f"{TEST_BUCKET_NAME}/dir2", recursive=True)
    out = fs.find(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [
        f"{TEST_BUCKET_NAME}/afile",
        f"{TEST_BUCKET_NAME}/dir/test1",
        f"{TEST_BUCKET_NAME}/dir/test2",
        f"{TEST_BUCKET_NAME}/dir2/test1",
        f"{TEST_BUCKET_NAME}/dir2/test2",
    ]
    fs.rm([f"{TEST_BUCKET_NAME}/dir", f"{TEST_BUCKET_NAME}/dir2"], recursive=True)
    out = fs.find(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [f"{TEST_BUCKET_NAME}/afile"]


def test_cat_ranges_one(fs: AsyncFsspecStore):
    data1 = os.urandom(10000)
    fs.pipe_file(f"{TEST_BUCKET_NAME}/data1", data1)

    # single range
    out = fs.cat_ranges([f"{TEST_BUCKET_NAME}/data1"], [10], [20])
    assert out == [data1[10:20]]

    # range oob
    out = fs.cat_ranges([f"{TEST_BUCKET_NAME}/data1"], [0], [11000])
    assert out == [data1]

    # two disjoint ranges, one file
    out = fs.cat_ranges(
        [f"{TEST_BUCKET_NAME}/data1", f"{TEST_BUCKET_NAME}/data1"],
        [10, 40],
        [20, 60],
    )
    assert out == [data1[10:20], data1[40:60]]

    # two adjoining ranges, one file
    out = fs.cat_ranges(
        [f"{TEST_BUCKET_NAME}/data1", f"{TEST_BUCKET_NAME}/data1"],
        [10, 30],
        [20, 60],
    )
    assert out == [data1[10:20], data1[30:60]]

    # two overlapping ranges, one file
    out = fs.cat_ranges(
        [f"{TEST_BUCKET_NAME}/data1", f"{TEST_BUCKET_NAME}/data1"],
        [10, 15],
        [20, 60],
    )
    assert out == [data1[10:20], data1[15:60]]

    # completely overlapping ranges, one file
    out = fs.cat_ranges(
        [f"{TEST_BUCKET_NAME}/data1", f"{TEST_BUCKET_NAME}/data1"],
        [10, 0],
        [20, 60],
    )
    assert out == [data1[10:20], data1[0:60]]


def test_cat_ranges_two(fs: AsyncFsspecStore):
    data1 = os.urandom(10000)
    data2 = os.urandom(10000)
    fs.pipe({f"{TEST_BUCKET_NAME}/data1": data1, f"{TEST_BUCKET_NAME}/data2": data2})

    # single range in each file
    out = fs.cat_ranges(
        [f"{TEST_BUCKET_NAME}/data1", f"{TEST_BUCKET_NAME}/data2"],
        [10, 10],
        [20, 20],
    )
    assert out == [data1[10:20], data2[10:20]]


@pytest.mark.xfail(reason="negative and mixed ranges not implemented")
def test_cat_ranges_mixed(fs: AsyncFsspecStore):
    data1 = os.urandom(10000)
    data2 = os.urandom(10000)
    fs.pipe({"data1": data1, "data2": data2})

    # single range in each file
    out = fs.cat_ranges(["data1", "data1", "data2"], [-10, None, 10], [None, -10, -10])
    assert out == [data1[-10:], data1[:-10], data2[10:-10]]


@pytest.mark.xfail(reason="atomic writes not working on moto")
def test_atomic_write(fs: AsyncFsspecStore):
    fs.pipe_file("data1", b"data1")
    fs.pipe_file("data1", b"data1", mode="overwrite")
    with pytest.raises(ValueError):  # noqa: PT011
        fs.pipe_file("data1", b"data1", mode="create")


def test_cat_ranges_error(fs: AsyncFsspecStore):
    with pytest.raises(ValueError):  # noqa: PT011
        fs.cat_ranges([f"{TEST_BUCKET_NAME}/path"], [], [])
