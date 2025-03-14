import pytest

from obstore.exceptions import GenericError
from obstore.store import GCSStore


def test_overlapping_config_keys():
    with pytest.raises(GenericError, match="Duplicate key"):
        GCSStore(google_bucket="bucket", GOOGLE_BUCKET="bucket")

    with pytest.raises(GenericError, match="Duplicate key"):
        GCSStore(config={"google_bucket": "test", "GOOGLE_BUCKET": "test"})
