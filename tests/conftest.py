import boto3
import pytest
import urllib3
from botocore import UNSIGNED
from botocore.client import Config
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from obstore.store import S3Store

TEST_BUCKET_NAME = "test"


# See docs here: https://docs.getmoto.org/en/latest/docs/server_mode.html
@pytest.fixture()
def moto_server_uri():
    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(ip_address="localhost", port=0)
    server.start()
    if hasattr(server, "get_host_and_port"):
        host, port = server.get_host_and_port()
    else:
        host, port = server._server.server_address
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
    yield moto_server_uri
    urllib3.request(method="post", url=f"{moto_server_uri}/moto-api/reset")


@pytest.fixture()
def s3_store(s3):
    return S3Store.from_url(
        f"s3://{TEST_BUCKET_NAME}/",
        config={
            "AWS_ENDPOINT_URL": s3,
            "AWS_REGION": "us-east-1",
            "AWS_SKIP_SIGNATURE": "True",
            "AWS_ALLOW_HTTP": "true",
        },
    )