import pytest

import obstore as obs
from obstore.store import S3Store, new_store


@pytest.mark.asyncio
async def test_list_async(s3_store: S3Store):
    list_result = await obs.list(s3_store).collect_async()
    assert any("afile" in x["path"] for x in list_result)


@pytest.mark.asyncio
async def test_get_async(s3_store: S3Store):
    resp = await obs.get_async(s3_store, "afile")
    buf = await resp.bytes_async()
    assert buf == b"hello world"


def test_construct_store_boolean_config():
    # Should allow boolean parameter
    S3Store("bucket", skip_signature=True)


@pytest.mark.asyncio
async def test_new_store():
    store = new_store(
        "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1",
        region="us-west-2",
        skip_signature=True,
    )
    _meta = await obs.head_async(store, "2024-01-01_performance_fixed_tiles.parquet")
