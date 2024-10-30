import os

import pytest
pytest.importorskip("moto")
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from object_store_rs.store import S3Store
import object_store_rs as obs
import pyarrow.parquet as pq
from object_store_rs.fsspec import AsyncFsspecStore


ip = "localhost"
port = 5555
endpoint_uri = f"http://{ip}:{port}"
test_bucket_name = "test"


@pytest.fixture(scope="module")
def s3_base():
    server = ThreadedMotoServer(ip_address=ip, port=port)
    server.start()
    os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    os.environ["AWS_ENDPOINT_URL"] = endpoint_uri

    print("server up")
    yield
    print("moto done")
    server.stop()


@pytest.fixture()
def s3(s3_base):
    from botocore.session import Session
    session = Session()
    client = session.create_client("s3", endpoint_url=endpoint_uri)
    client.create_bucket(Bucket=test_bucket_name, ACL="public-read")
    client.put_object(Bucket=test_bucket_name, Key="afile", Body=b"hello world")


@pytest.fixture(autouse=True)
def reset_s3_fixture():
    import requests
    # We reuse the MotoServer for all tests
    # But we do want a clean state for every test
    try:
        requests.post(f"{endpoint_uri}/moto-api/reset")
    except:
        pass


@pytest.fixture()
def fs(s3):
    return AsyncFsspecStore(S3Store.from_env(test_bucket_name))


def test_list(fs):
    out = fs.ls("", detail=False)
    breakpoint()
    1


def test_remote_parquet():
    store = obs.store.HTTPStore.from_url("https://github.com")
    fs = AsyncFsspecStore(store)
    url = "opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
    pq.read_metadata(url, filesystem=fs)
