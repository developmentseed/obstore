import pytest

from obstore.exceptions import BaseError
from obstore.store import GCSStore


def test_overlapping_config_keys():
    with pytest.raises(BaseError, match="Duplicate key"):
        GCSStore(google_bucket="bucket", GOOGLE_BUCKET="bucket")
