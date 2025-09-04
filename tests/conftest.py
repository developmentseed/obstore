from __future__ import annotations

import socket
import time
import warnings
from typing import TYPE_CHECKING, Any

import boto3
import docker
import pytest
import requests
from botocore import UNSIGNED
from botocore.client import Config
from minio import Minio
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from requests.exceptions import RequestException

from obstore.store import S3Store

if TYPE_CHECKING:
    from collections.abc import Generator

    from obstore.store import ClientConfig, S3Config

TEST_BUCKET_NAME = "test"


# See docs here: https://docs.getmoto.org/en/latest/docs/server_mode.html
@pytest.fixture(scope="session")
def moto_server_uri():
    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(ip_address="localhost", port=0)
    server.start()
    if hasattr(server, "get_host_and_port"):
        host, port = server.get_host_and_port()
    else:
        s = server._server
        assert s is not None
        # An AF_INET6 socket address has 4 components.
        host, port = s.server_address[:2]
    uri = f"http://{host}:{port}"
    yield uri
    server.stop()


@pytest.fixture
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
    objects = client.list_objects_v2(Bucket=TEST_BUCKET_NAME)
    for name in objects.get("Contents", []):
        key = name.get("Key")
        assert key is not None
        client.delete_object(Bucket=TEST_BUCKET_NAME, Key=key)
    requests.post(f"{moto_server_uri}/moto-api/reset", timeout=30)


@pytest.fixture
def s3_store(s3: str):
    return S3Store.from_url(
        f"s3://{TEST_BUCKET_NAME}/",
        endpoint=s3,
        region="us-east-1",
        skip_signature=True,
        client_options={"allow_http": True},
    )


@pytest.fixture
def s3_store_config(s3: str) -> S3Config:
    return {
        "endpoint": s3,
        "region": "us-east-1",
        "skip_signature": True,
    }


def find_available_port() -> int:
    """Find a free port on localhost.

    Note that this is susceptible to race conditions.
    """
    # https://stackoverflow.com/a/36331860

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Bind to a free port provided by the host.
        s.bind(("", 0))

        # Return the port number assigned.
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def minio_bucket() -> Generator[tuple[S3Config, ClientConfig], Any, None]:
    warnings.warn(
        "Creating Docker client...",
        UserWarning,
        stacklevel=1,
    )
    docker_client = docker.from_env()
    warnings.warn(
        "Finished creating Docker client...",
        UserWarning,
        stacklevel=1,
    )

    port = find_available_port()
    username = "minioadmin"
    password = "minioadmin"  # noqa: S105
    bucket = "test-bucket"

    warnings.warn(
        "Starting MinIO container...",
        UserWarning,
        stacklevel=1,
    )
    minio_container = docker_client.containers.run(
        "quay.io/minio/minio",
        "server /data",
        detach=True,
        ports={f"{port}/tcp": port},
        environment={
            "MINIO_ACCESS_KEY": username,
            "MINIO_SECRET_KEY": password,
        },
    )
    warnings.warn(
        "Finished starting MinIO container...",
        UserWarning,
        stacklevel=1,
    )

    # Wait for MinIO to be ready
    endpoint = f"http://localhost:{port}"
    wait_for_minio(endpoint, timeout=30)

    minio_client = Minio(
        f"localhost:{port}",
        access_key=username,
        secret_key=password,
        secure=False,
    )
    minio_client.make_bucket(bucket)

    s3_config: S3Config = {
        "bucket": bucket,
        "endpoint": endpoint,
        "access_key_id": username,
        "secret_access_key": password,
        "virtual_hosted_style_request": False,
    }
    client_options: ClientConfig = {"allow_http": True}

    yield (s3_config, client_options)

    minio_container.stop()
    minio_container.remove()


@pytest.fixture
def minio_store(minio_bucket: tuple[S3Config, ClientConfig]) -> S3Store:
    """Create an S3Store configured for MinIO integration testing."""
    return S3Store(config=minio_bucket[0], client_options=minio_bucket[1])


def wait_for_minio(endpoint: str, timeout: int):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # MinIO health check endpoint
            response = requests.get(f"{endpoint}/minio/health/live", timeout=2)
            if response.status_code == 200:
                return
        except RequestException:
            pass
        time.sleep(0.5)

    exc_str = f"MinIO failed to start within {timeout} seconds"
    raise TimeoutError(exc_str)
