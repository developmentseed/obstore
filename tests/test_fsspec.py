import os

import pyarrow.parquet as pq
import pytest

import obstore as obs
from obstore.fsspec import AsyncFsspecStore
from obstore.store import S3Store


@pytest.fixture
def fs(s3_store: S3Store):
    return AsyncFsspecStore(s3_store)


def test_list(fs: AsyncFsspecStore):
    out = fs.ls("", detail=False)
    assert out == ["afile"]
    fs.pipe_file("dir/bfile", b"data")
    out = fs.ls("", detail=False)
    assert out == ["afile", "dir"]
    out = fs.ls("", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


@pytest.mark.asyncio
async def test_list_async(s3_store: S3Store):
    fs = AsyncFsspecStore(s3_store, asynchronous=True)
    out = await fs._ls("", detail=False)
    assert out == ["afile"]
    await fs._pipe_file("dir/bfile", b"data")
    out = await fs._ls("", detail=False)
    assert out == ["afile", "dir"]
    out = await fs._ls("", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


@pytest.mark.network
def test_remote_parquet():
    store = obs.store.HTTPStore.from_url("https://github.com")
    fs = AsyncFsspecStore(store)
    url = "opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
    pq.read_metadata(url, filesystem=fs)


def test_multi_file_ops(fs: AsyncFsspecStore):
    data = {"dir/test1": b"test data1", "dir/test2": b"test data2"}
    fs.pipe(data)
    out = fs.cat(list(data))
    assert out == data
    out = fs.cat("dir", recursive=True)
    assert out == data
    fs.cp("dir", "dir2", recursive=True)
    out = fs.find("", detail=False)
    assert out == ["afile", "dir/test1", "dir/test2", "dir2/test1", "dir2/test2"]
    fs.rm(["dir", "dir2"], recursive=True)
    out = fs.find("", detail=False)
    assert out == ["afile"]


def test_cat_ranges_one(fs: AsyncFsspecStore):
    data1 = os.urandom(10000)
    fs.pipe_file("data1", data1)

    # single range
    out = fs.cat_ranges(["data1"], [10], [20])
    assert out == [data1[10:20]]

    # range oob
    out = fs.cat_ranges(["data1"], [0], [11000])
    assert out == [data1]

    # two disjoint ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 40], [20, 60])
    assert out == [data1[10:20], data1[40:60]]

    # two adjoining ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 30], [20, 60])
    assert out == [data1[10:20], data1[30:60]]

    # two overlapping ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 15], [20, 60])
    assert out == [data1[10:20], data1[15:60]]

    # completely overlapping ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 0], [20, 60])
    assert out == [data1[10:20], data1[0:60]]


def test_cat_ranges_two(fs: AsyncFsspecStore):
    data1 = os.urandom(10000)
    data2 = os.urandom(10000)
    fs.pipe({"data1": data1, "data2": data2})

    # single range in each file
    out = fs.cat_ranges(["data1", "data2"], [10, 10], [20, 20])
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
        fs.cat_ranges(["path"], [], [])
