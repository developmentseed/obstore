from pathlib import Path

import pytest

from obstore.exceptions import ObstoreError, UnknownConfigurationKeyError
from obstore.store import from_url


def test_local():
    cwd = Path(".").absolute()
    url = f"file://{cwd}"
    _store = from_url(url)


def test_memory():
    url = "memory:///"
    _store = from_url(url)

    with pytest.raises(ObstoreError):
        from_url(url, aws_access_key_id="test")


def test_s3_params():
    from_url(
        "s3://bucket/path",
        access_key_id="access_key_id",
        secret_access_key="secret_access_key",
    )

    with pytest.raises(UnknownConfigurationKeyError):
        from_url("s3://bucket/path", azure_authority_id="")


def test_gcs_params():
    # Just to test the params. In practice, the bucket shouldn't be passed
    from_url("gs://test.example.com/path", google_bucket="test_bucket")

    with pytest.raises(UnknownConfigurationKeyError):
        from_url("gs://test.example.com/path", azure_authority_id="")


def test_azure_params():
    url = "abfs://container@account.dfs.core.windows.net/path"
    from_url(url, azure_skip_signature=True)

    with pytest.raises(UnknownConfigurationKeyError):
        from_url(url, aws_bucket="test")


def test_http():
    url = "https://mydomain/path"
    from_url(url)

    with pytest.raises(ObstoreError):
        from_url(url, aws_bucket="test")
