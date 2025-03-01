import pytest

from obstore.exceptions import BaseError
from obstore.store import AzureStore


def test_overlapping_config_keys():
    with pytest.raises(BaseError, match="Duplicate key"):
        AzureStore(azure_container_name="test", AZURE_CONTAINER_NAME="test")

    with pytest.raises(BaseError, match="Duplicate key"):
        AzureStore(
            config={"azure_container_name": "test", "AZURE_CONTAINER_NAME": "test"},
        )
