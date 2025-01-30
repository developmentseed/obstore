from pathlib import Path

import pytest

import obstore as obs
from obstore.exceptions import ObstoreError, UnknownConfigurationKeyError
from obstore.store import new_store


def test_local():
    cwd = Path(".").absolute()
    url = f"file://{cwd}"
    store = new_store(url)
    cwd_files = obs.list(store).collect()
    assert any(file["path"] == "test_from_url.py" for file in cwd_files)


def test_memory():
    url = "memory:///"
    _store = new_store(url)

    with pytest.raises(ObstoreError):
        new_store(url, aws_access_key_id="test")


def test_s3_params():
    new_store(
        "s3://bucket/path",
        access_key_id="access_key_id",
        secret_access_key="secret_access_key",
    )

    with pytest.raises(UnknownConfigurationKeyError):
        new_store("s3://bucket/path", azure_authority_id="")


def test_gcs_params():
    # Just to test the params. In practice, the bucket shouldn't be passed
    new_store("gs://test.example.com/path", google_bucket="test_bucket")

    with pytest.raises(UnknownConfigurationKeyError):
        new_store("gs://test.example.com/path", azure_authority_id="")


def test_azure_params():
    url = "abfs://container@account.dfs.core.windows.net/path"
    new_store(url, azure_skip_signature=True)

    with pytest.raises(UnknownConfigurationKeyError):
        new_store(url, aws_bucket="test")


def test_http():
    url = "https://mydomain/path"
    new_store(url)

    with pytest.raises(ObstoreError):
        new_store(url, aws_bucket="test")
