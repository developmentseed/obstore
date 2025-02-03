import pickle

# import cloudpickle
import pytest

import obstore as obs
from obstore.exceptions import BaseError
from obstore.store import S3Store, from_url


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


def test_error_overlapping_config_kwargs():
    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", config={"skip_signature": True}, skip_signature=True)

    # Also raises for variations of the same parameter
    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", config={"aws_skip_signature": True}, skip_signature=True)

    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", config={"AWS_SKIP_SIGNATURE": True}, skip_signature=True)


@pytest.mark.asyncio
async def test_from_url():
    store = from_url(
        "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1",
        region="us-west-2",
        skip_signature=True,
    )
    _meta = await obs.head_async(store, "2024-01-01_performance_fixed_tiles.parquet")


def test_pickle():
    store = S3Store(
        "ookla-open-data",
        region="us-west-2",
        skip_signature=True,
    )
    restored = pickle.loads(pickle.dumps(store))
    _objects = next(obs.list(restored))
