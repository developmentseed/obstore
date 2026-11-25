# ruff: noqa: PGH003

import pickle
from datetime import UTC, datetime

import pytest

from obstore.exceptions import BaseError, UnauthenticatedError
from obstore.store import S3Store, from_url


@pytest.mark.asyncio
async def test_list_async(minio_store: S3Store):
    await minio_store.put_async("afile", b"hello world")

    list_result = await minio_store.list().collect_async()
    assert any("afile" in x["path"] for x in list_result)


@pytest.mark.asyncio
async def test_get_async(minio_store: S3Store):
    await minio_store.put_async("afile", b"hello world")

    resp = await minio_store.get_async("afile")
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
        S3Store(
            "bucket",
            config={"aws_skip_signature": True},  # type: ignore
            skip_signature=True,
        )

    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store(
            "bucket",
            config={"AWS_SKIP_SIGNATURE": True},  # type: ignore
            skip_signature=True,
        )


def test_overlapping_config_keys():
    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store(
            "bucket",
            config={"aws_skip_signature": True, "skip_signature": True},  # type: ignore
        )

    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", aws_skip_signature=True, skip_signature=True)  # type: ignore

    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", AWS_SKIP_SIGNATURE=True, skip_signature=True)  # type: ignore


@pytest.mark.asyncio
async def test_from_url():
    store = from_url(
        "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1",
        region="us-west-2",
        skip_signature=True,
    )
    _meta = await store.head_async("2024-01-01_performance_fixed_tiles.parquet")


@pytest.mark.parametrize(
    "url",
    [
        "https://account.r2.cloudflarestorage.com/bucket",
        "https://account.eu.r2.cloudflarestorage.com/bucket",
    ],
)
def test_r2_from_url(url: str):
    store = S3Store.from_url(url)
    endpoint = store.config.get("endpoint")
    assert endpoint is not None
    assert (".eu." in endpoint) == (".eu." in url)


@pytest.mark.parametrize("path", ["", "/some/prefix"])
def test_dualstack_path_style_from_url(path: str):
    store = from_url(f"https://s3.dualstack.us-east-1.amazonaws.com/bucket{path}")
    assert isinstance(store, S3Store)
    assert store.config == {
        "bucket": "bucket",
        "region": "us-east-1",
        "endpoint": "https://s3.dualstack.us-east-1.amazonaws.com",
    }
    assert store.prefix == (path.lstrip("/") or None)
    assert pickle.loads(pickle.dumps(store)) == store


@pytest.mark.parametrize("path", ["", "/some/prefix"])
def test_dualstack_virtual_hosted_from_url(path: str):
    store = from_url(f"https://bucket.s3.dualstack.us-east-1.amazonaws.com{path}")
    assert isinstance(store, S3Store)
    assert store.config == {
        "bucket": "bucket",
        "region": "us-east-1",
        "endpoint": "https://bucket.s3.dualstack.us-east-1.amazonaws.com",
        "virtual_hosted_style_request": "true",
    }
    assert store.prefix == (path.lstrip("/") or None)
    assert pickle.loads(pickle.dumps(store)) == store


@pytest.mark.parametrize(
    "url",
    [
        "https://s3.dualstack.us-east-1.amazonaws.com/bucket",
        "https://bucket.s3.dualstack.us-east-1.amazonaws.com",
    ],
)
def test_dualstack_from_url_config_takes_precedence(url: str):
    store = S3Store.from_url(
        url,
        config={"endpoint": "http://localhost:9000", "region": "us-west-2"},
    )
    assert store.config.get("bucket") == "bucket"
    assert store.config.get("endpoint") == "http://localhost:9000"
    assert store.config.get("region") == "us-west-2"


def test_pickle():
    store = S3Store(
        "ookla-open-data",
        region="us-west-2",
        skip_signature=True,
    )
    restored = pickle.loads(pickle.dumps(store))
    _objects = next(restored.list())


def test_config_round_trip():
    store = S3Store.from_url(
        "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1",
        region="us-west-2",
        skip_signature=True,
    )
    new_store = S3Store(
        config=store.config,
        prefix=store.prefix,
        client_options=store.client_options,
        retry_config=store.retry_config,
    )
    assert store.config == new_store.config
    assert store.prefix == new_store.prefix
    assert store.client_options == new_store.client_options
    assert store.retry_config == new_store.retry_config


def test_endpoint_url_sts():
    store = S3Store("bucket", endpoint_url_sts="https://sts.example.com")
    assert store.config.get("endpoint_url_sts") == "https://sts.example.com"
    restored = pickle.loads(pickle.dumps(store))
    assert restored.config == store.config


def test_invalid_credential_provider():
    """Test that passing an invalid synchronous credential provider raises an error.

    instead of trying to await the value.
    """

    def credential_provider():
        return {"access_key_id": "str", "expires_at": datetime.now(UTC)}

    store = S3Store("bucket", credential_provider=credential_provider)  # type: ignore
    with pytest.raises(UnauthenticatedError):
        store.list().collect()


@pytest.mark.asyncio
async def test_invalid_credential_provider_async():
    async def credential_provider():
        return {"access_key_id": "str", "expires_at": datetime.now(UTC)}

    store = S3Store("bucket", credential_provider=credential_provider)  # type: ignore
    with pytest.raises(UnauthenticatedError):
        await store.list().collect_async()


def test_eq():
    store = S3Store("bucket", client_options={"timeout": "10s"})
    store2 = S3Store("bucket", client_options={"timeout": "10s"})
    store3 = S3Store("bucket")
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3
