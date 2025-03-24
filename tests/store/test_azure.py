import pytest

from obstore.exceptions import BaseError
from obstore.store import AzureStore


def test_overlapping_config_keys():
    with pytest.raises(BaseError, match="Duplicate key"):
        AzureStore(container_name="test", AZURE_CONTAINER_NAME="test")  # type: ignore intentional test

    with pytest.raises(BaseError, match="Duplicate key"):
        AzureStore(
            config={"azure_container_name": "test", "AZURE_CONTAINER_NAME": "test"},  # type: ignore (intentional test)
        )


def test_eq():
    store = AzureStore(
        "container",
        account_name="account_name",
        client_options={"timeout": "10s"},
    )
    store2 = AzureStore(
        "container",
        account_name="account_name",
        client_options={"timeout": "10s"},
    )
    store3 = AzureStore(
        "container",
        account_name="account_name",
    )
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3
