import os

import boto3
import pytest
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

import obstore as obs
from obstore.store import S3Store

ip = "localhost"
port = 5555
endpoint_uri = f"http://{ip}:{port}"
test_bucket_name = "test"


@pytest.fixture(scope="module")
def moto_server_uri():
    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    uri = f"http://{host}:{port}"
    print(uri)
    yield uri
    server.stop()


@pytest.fixture()
def s3(moto_server_uri: str):
    session = boto3.Session(region_name="us-east-1")
    client = session.client("s3", endpoint_url=moto_server_uri)
    client.create_bucket(Bucket=test_bucket_name, ACL="public-read")
    client.put_object(Bucket=test_bucket_name, Key="afile", Body=b"hello world")
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
        f"s3://{test_bucket_name}/",
        config={
            "AWS_SECRET_ACCESS_KEY": "foo",
            "AWS_ACCESS_KEY_ID": "foo",
            "AWS_ENDPOINT_URL": s3,
            "AWS_REGION": "us-east-1",
        },
        client_options={"allow_http": "True"},
    )


def test_list(store: S3Store):
    list_result = obs.list(store).collect()
    assert any("afile" in x["path"] for x in list_result)
