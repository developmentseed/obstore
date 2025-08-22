import pytest

from obstore.exceptions import BaseError, GenericError
from obstore.store import GCSStore


def test_overlapping_config_keys():
    with pytest.raises(BaseError, match="Duplicate key"):
        GCSStore(google_bucket="bucket", GOOGLE_BUCKET="bucket")  # type: ignore intentional test

    with pytest.raises(BaseError, match="Duplicate key"):
        GCSStore(config={"google_bucket": "test", "GOOGLE_BUCKET": "test"})  # type: ignore intentional test


def test_eq():
    store = GCSStore("bucket", client_options={"timeout": "10s"})
    store2 = GCSStore("bucket", client_options={"timeout": "10s"})
    store3 = GCSStore("bucket")
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3


def test_application_credentials():
    # The application_credentials parameter should be correctly passed down
    # Finalizing the GCSBuilder should try to load and parse those credentials, which
    # should error here.
    with pytest.raises(GenericError, match="source: OpenCredentials"):
        GCSStore("bucket", application_credentials="path/to/creds.json")
