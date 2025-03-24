from pathlib import Path

import pytest

from obstore.exceptions import BaseError, UnknownConfigurationKeyError
from obstore.store import from_url


def test_local():
    cwd = Path().absolute()
    url = f"file://{cwd}"
    _store = from_url(url)


def test_memory():
    url = "memory:///"
    _store = from_url(url)

    with pytest.raises(BaseError):
        from_url(url, access_key_id="test")


def test_s3_params():
    from_url(
        "s3://bucket/path",
        access_key_id="access_key_id",
        secret_access_key="secret_access_key",  # noqa: S106
    )

    with pytest.raises(UnknownConfigurationKeyError):
        from_url("s3://bucket/path", tenant_id="")


def test_gcs_params():
    # Just to test the params. In practice, the bucket shouldn't be passed
    # Note: we can't pass the bucket name here as a kwarg because it would conflict with
    # the bucket name in the URL.
    from_url("gs://test.example.com/path")

    with pytest.raises(UnknownConfigurationKeyError):
        from_url("gs://test.example.com/path", tenant_id="")


def test_azure_params():
    url = "abfs://container@account.dfs.core.windows.net/path"
    from_url(url, skip_signature=True)

    with pytest.raises(UnknownConfigurationKeyError):
        from_url(url, bucket="test")


def test_http():
    url = "https://mydomain/path"
    from_url(url)

    with pytest.raises(BaseError):
        from_url(url, bucket="test")
