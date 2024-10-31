import boto3
import pytest
from botocore import UNSIGNED
from botocore.client import Config
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

import obstore as obs
from obstore.store import S3Store

TEST_BUCKET_NAME = "test"


# See docs here: https://docs.getmoto.org/en/latest/docs/server_mode.html
@pytest.fixture(scope="module")
def moto_server_uri():
    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(ip_address="localhost", port=0)
    server.start()
    host, port = server.get_host_and_port()
    uri = f"http://{host}:{port}"
    yield uri
    server.stop()


@pytest.fixture()
def s3(moto_server_uri: str):
    client = boto3.client(
        "s3",
        config=Config(signature_version=UNSIGNED),
        region_name="us-east-1",
        endpoint_url=moto_server_uri,
    )
    client.create_bucket(Bucket=TEST_BUCKET_NAME, ACL="public-read")
    client.put_object(Bucket=TEST_BUCKET_NAME, Key="afile", Body=b"hello world")
    return moto_server_uri


# @pytest.fixture(autouse=True)
# def reset_s3_fixture(moto_server_uri):
#     import requests

#     # We reuse the MotoServer for all tests
#     # But we do want a clean state for every test
#     try:
#         requests.post(f"{moto_server_uri}/moto-api/reset")
#     except:
#         pass


@pytest.fixture()
def store(s3):
    return S3Store.from_url(
        f"s3://{TEST_BUCKET_NAME}/",
        config={
            "AWS_ENDPOINT_URL": s3,
            "AWS_REGION": "us-east-1",
            "AWS_SKIP_SIGNATURE": "True",
            "AWS_ALLOW_HTTP": "true",
        },
    )


@pytest.mark.asyncio
async def test_list_async(store: S3Store):
    list_result = await obs.list(store).collect_async()
    assert any("afile" in x["path"] for x in list_result)


@pytest.mark.asyncio
async def test_get_async(store: S3Store):
    resp = await obs.get_async(store, "afile")
    buf = await resp.bytes_async()
    assert buf == b"hello world"
