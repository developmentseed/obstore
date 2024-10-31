import os

import pytest
pytest.importorskip("moto")
import obstore as obs
import pyarrow.parquet as pq
from obstore.fsspec import AsyncFsspecStore


@pytest.fixture()
def fs(s3_store):
    return AsyncFsspecStore(s3_store)


def test_list(fs):
    out = fs.ls("", detail=False)
    assert out == ["afile"]


def test_remote_parquet():
    store = obs.store.HTTPStore.from_url("https://github.com")
    fs = AsyncFsspecStore(store)
    url = "opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
    pq.read_metadata(url, filesystem=fs)
