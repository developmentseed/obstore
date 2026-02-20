from __future__ import annotations

import socket
import sys
import time
import warnings
from typing import TYPE_CHECKING, Any

import docker
import pytest
import requests
from minio import Minio
from requests.exceptions import RequestException

from obstore.store import S3Store


def pytest_configure(config: pytest.Config) -> None:
    """Disable pytest-freethreaded's multi-thread defaults.

    Tests must opt-in via @pytest.mark.freethreaded(threads=N, iterations=M).
    """
    if sys.version_info >= (3, 13) and hasattr(config.option, "threads"):
        config.option.threads = 2
        config.option.iterations = 1


if TYPE_CHECKING:
    from collections.abc import Generator

    from obstore.store import ClientConfig, S3Config

TEST_BUCKET_NAME = "test-bucket"


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
def minio_config() -> Generator[tuple[S3Config, ClientConfig], Any, None]:
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

    username = "minioadmin"
    password = "minioadmin"  # noqa: S105
    port = find_available_port()
    console_port = find_available_port()

    print(f"Using ports: {port=}, {console_port=}")  # noqa: T201
    print(  # noqa: T201
        f"Log on to MinIO console at http://localhost:{console_port} with "
        f"{username=} and {password=}",
    )

    warnings.warn(
        "Starting MinIO container...",
        UserWarning,
        stacklevel=1,
    )
    minio_container = docker_client.containers.run(
        "quay.io/minio/minio",
        "server /data --console-address :9001",
        detach=True,
        ports={
            "9000/tcp": port,
            "9001/tcp": console_port,
        },
        environment={
            "MINIO_ROOT_USER": username,
            "MINIO_ROOT_PASSWORD": password,
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
    minio_client.make_bucket(TEST_BUCKET_NAME)

    s3_config: S3Config = {
        "bucket": TEST_BUCKET_NAME,
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
def minio_bucket(
    minio_config: tuple[S3Config, ClientConfig],
) -> Generator[tuple[S3Config, ClientConfig], Any, None]:
    yield minio_config

    # Remove all files from bucket
    store = S3Store(config=minio_config[0], client_options=minio_config[1])
    objects = store.list().collect()
    paths = [obj["path"] for obj in objects]
    store.delete(paths)


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
