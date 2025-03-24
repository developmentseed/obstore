from __future__ import annotations

import gc
import os
import sys
from typing import TYPE_CHECKING
from unittest.mock import patch

import fsspec
import pyarrow.parquet as pq
import pytest
from fsspec.registry import _registry

from obstore.fsspec import FsspecStore, register
from tests.conftest import TEST_BUCKET_NAME

if TYPE_CHECKING:
    from pathlib import Path

    from obstore.store import S3Config


if sys.version_info < (3, 10):
    pytest.skip("Moto doesn't seem to support Python 3.9", allow_module_level=True)


@pytest.fixture
def fs(s3_store_config: S3Config):
    register("s3")
    return fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
    )


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
        FsspecStore,
    ), "Registered class should be a subclass of FsspecStore"
    assert fs_class.protocol == "s3", (
        "Registered class should have the correct protocol"
    )

    # Ensure a new instance of the registered store can be created
    fs_instance = fs_class("s3")
    assert isinstance(
        fs_instance,
        FsspecStore,
    ), "Registered class should be instantiable"

    # test register asynchronous
    register("gcs", asynchronous=True)  # Register the "s3" protocol dynamically
    fs_class = fsspec.get_filesystem_class("gcs")
    assert fs_class.asynchronous, "Registered class should be asynchronous"

    # test multiple registrations
    register(["file", "abfs"])
    assert issubclass(fsspec.get_filesystem_class("file"), FsspecStore)
    assert issubclass(fsspec.get_filesystem_class("abfs"), FsspecStore)


def test_construct_store_cache_diff_bucket_name(s3_store_config: S3Config):
    register("s3")
    fs: FsspecStore = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
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
    del fs
    assert gc.collect() > 0


def test_construct_store_cache_same_bucket_name(s3_store_config: S3Config):
    register("s3")
    fs = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
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
    del fs
    assert gc.collect() > 0


def test_fsspec_filesystem_cache(s3_store_config: S3Config):
    """Test caching behavior of fsspec.filesystem with the _Cached metaclass."""
    register("s3")

    # call fsspec.filesystem() multiple times with the same parameters
    fs1 = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
    )
    fs2 = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
    )

    # Same parameters should return the same instance
    assert fs1 is fs2, (
        "fsspec.filesystem() with the same parameters should return the cached instance"
    )

    # Changing parameters should create a new instance
    fs3 = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
        asynchronous=True,
    )
    assert fs1 is not fs3, (
        "fsspec.filesystem() with different parameters should return a new instance"
    )


def test_split_path(fs: FsspecStore):
    # in url format, with bucket
    assert fs._split_path("s3://mybucket/path/to/file") == ("mybucket", "path/to/file")
    assert fs._split_path("s3://data-bucket/") == ("data-bucket", "")

    # path format, with bucket
    assert fs._split_path("mybucket/path/to/file") == ("mybucket", "path/to/file")
    assert fs._split_path("data-bucket/") == ("data-bucket", "")

    # url format, wrong protocol
    with pytest.raises(ValueError, match="Expected protocol to be s3. Got gs"):
        fs._split_path("gs://data-bucket/")

    # in url format, without bucket
    file_fs = FsspecStore("file")
    assert file_fs._split_path("file:///mybucket/path/to/file") == (
        "",
        "/mybucket/path/to/file",
    )
    assert file_fs._split_path("file:///data-bucket/") == ("", "/data-bucket/")

    # path format, without bucket
    assert file_fs._split_path("/mybucket/path/to/file") == (
        "",
        "/mybucket/path/to/file",
    )
    assert file_fs._split_path("/data-bucket/") == ("", "/data-bucket/")


def test_list(fs: FsspecStore):
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
    fs = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
        asynchronous=True,
    )

    out = await fs._ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [f"{TEST_BUCKET_NAME}/afile"]
    await fs._pipe_file(f"{TEST_BUCKET_NAME}/dir/bfile", b"data")
    out = await fs._ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert out == [f"{TEST_BUCKET_NAME}/afile", f"{TEST_BUCKET_NAME}/dir"]
    out = await fs._ls(f"{TEST_BUCKET_NAME}", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


def test_info(fs: FsspecStore):
    fs.pipe_file(f"{TEST_BUCKET_NAME}/dir/afile", b"data")

    # info for directory
    out = fs.info(f"{TEST_BUCKET_NAME}/dir")
    assert out == {
        "name": f"{TEST_BUCKET_NAME}/dir",
        "type": "directory",
        "size": 0,
    }

    # info for file not exist
    with pytest.raises(FileNotFoundError):
        fs.info(f"{TEST_BUCKET_NAME}/dir/bfile")

    # info for directory not exist
    with pytest.raises(FileNotFoundError):
        fs.info(f"{TEST_BUCKET_NAME}/dir_1/")

    # also test with isdir
    assert fs.isdir(f"{TEST_BUCKET_NAME}/dir")
    assert not fs.isdir(f"{TEST_BUCKET_NAME}/dir/afile")
    assert not fs.isdir(f"{TEST_BUCKET_NAME}/dir/bfile")
    assert not fs.isdir(f"{TEST_BUCKET_NAME}/dir_1/")


@pytest.mark.asyncio
async def test_info_async(fs: FsspecStore):
    await fs._pipe_file(f"{TEST_BUCKET_NAME}/dir/afile", b"data")

    # info for directory
    out = await fs._info(f"{TEST_BUCKET_NAME}/dir")
    assert out == {
        "name": f"{TEST_BUCKET_NAME}/dir",
        "type": "directory",
        "size": 0,
    }

    # info for file not exist
    with pytest.raises(FileNotFoundError):
        await fs._info(f"{TEST_BUCKET_NAME}/dir/bfile")

    # info for directory not exist
    with pytest.raises(FileNotFoundError):
        await fs._info(f"{TEST_BUCKET_NAME}/dir_1/")

    # also test with isdir
    assert await fs._isdir(f"{TEST_BUCKET_NAME}/dir")
    assert not await fs._isdir(f"{TEST_BUCKET_NAME}/dir/afile")
    assert not await fs._isdir(f"{TEST_BUCKET_NAME}/dir/bfile")
    assert not await fs._isdir(f"{TEST_BUCKET_NAME}/dir_1/")


def test_put_files(fs: FsspecStore, tmp_path: Path):
    """Test put new file to S3 synchronously."""
    test_data = "Hello, World!"
    local_file_path = tmp_path / "test_file.txt"
    local_file_path.write_text(test_data)

    assert local_file_path.read_text() == test_data
    remote_file_path = f"{TEST_BUCKET_NAME}/uploaded_test_file.txt"

    fs.put(str(local_file_path), remote_file_path)

    # Verify file upload
    assert remote_file_path in fs.ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert fs.cat(remote_file_path)[remote_file_path] == test_data.encode()  # type: ignore (fsspec)

    # Cleanup remote file
    fs.rm(remote_file_path)


@pytest.mark.asyncio
async def test_put_files_async(s3_store_config: S3Config, tmp_path: Path):
    """Test put new file to S3 asynchronously."""
    register("s3")
    fs = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
        asynchronous=True,
    )

    test_data = "Hello, World!"
    local_file_path = tmp_path / "test_file.txt"
    local_file_path.write_text(test_data)

    assert local_file_path.read_text() == test_data
    remote_file_path = f"{TEST_BUCKET_NAME}/uploaded_test_file.txt"

    await fs._put(str(local_file_path), remote_file_path)

    # Verify file upload
    assert remote_file_path in await fs._ls(f"{TEST_BUCKET_NAME}", detail=False)
    out = await fs._cat([remote_file_path])
    assert out[remote_file_path] == test_data.encode()

    # Cleanup remote file
    await fs._rm(remote_file_path)


@pytest.mark.network
def test_remote_parquet(s3_store_config: S3Config):
    register(["https", "s3"])
    fs = fsspec.filesystem("https")
    fs_s3 = fsspec.filesystem(
        "s3",
        config=s3_store_config,
        client_options={"allow_http": True},
    )

    url = "github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"  # noqa: E501
    pq.read_metadata(url, filesystem=fs)

    # also test with full url
    url = "https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
    pq.read_metadata(url, filesystem=fs)

    # Read the remote Parquet file into a PyArrow table
    table = pq.read_table(url, filesystem=fs)
    write_parquet_path = f"{TEST_BUCKET_NAME}/test.parquet"

    # Write the table to s3
    pq.write_table(table, write_parquet_path, filesystem=fs_s3)

    out = fs_s3.ls(f"{TEST_BUCKET_NAME}", detail=False)
    assert f"{TEST_BUCKET_NAME}/test.parquet" in out

    # Read Parquet file from s3 and verify its contents
    parquet_table = pq.read_table(write_parquet_path, filesystem=fs_s3)
    assert parquet_table.equals(
        table,
    ), "Parquet file contents from s3 do not match the original file"


def test_multi_file_ops(fs: FsspecStore):
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


def test_cat_ranges_one(fs: FsspecStore):
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


def test_cat_ranges_two(fs: FsspecStore):
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
def test_cat_ranges_mixed(fs: FsspecStore):
    data1 = os.urandom(10000)
    data2 = os.urandom(10000)
    fs.pipe({"data1": data1, "data2": data2})

    # single range in each file
    out = fs.cat_ranges(["data1", "data1", "data2"], [-10, None, 10], [None, -10, -10])
    assert out == [data1[-10:], data1[:-10], data2[10:-10]]


@pytest.mark.xfail(reason="atomic writes not working on moto")
def test_atomic_write(fs: FsspecStore):
    fs.pipe_file("data1", b"data1")
    fs.pipe_file("data1", b"data1", mode="overwrite")
    with pytest.raises(ValueError):  # noqa: PT011
        fs.pipe_file("data1", b"data1", mode="create")


def test_cat_ranges_error(fs: FsspecStore):
    with pytest.raises(ValueError):  # noqa: PT011
        fs.cat_ranges([f"{TEST_BUCKET_NAME}/path"], [], [])
