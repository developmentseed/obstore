import pytest

import obstore as obs
from obstore.store import S3Store


@pytest.mark.asyncio
async def test_list_async(s3_store: S3Store):
    list_result = await obs.list(s3_store).collect_async()
    assert any("afile" in x["path"] for x in list_result)


@pytest.mark.asyncio
async def test_get_async(s3_store: S3Store):
    resp = await obs.get_async(s3_store, "afile")
    buf = await resp.bytes_async()
    assert buf == b"hello world"
