import pyarrow.parquet as pq
import pytest

import obstore as obs
from obstore.fsspec import AsyncFsspecStore


@pytest.fixture()
def fs(s3_store):
    return AsyncFsspecStore(s3_store)


def test_list(fs):
    out = fs.ls("", detail=False)
    assert out == ["afile"]
    fs.pipe_file("dir/bfile", b"data")
    out = fs.ls("", detail=False)
    assert out == ["afile", "dir"]
    out = fs.ls("", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


def test_remote_parquet():
    store = obs.store.HTTPStore.from_url("https://github.com")
    fs = AsyncFsspecStore(store)
    url = "opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
    pq.read_metadata(url, filesystem=fs)
