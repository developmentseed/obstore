from pathlib import Path

import pytest

import obstore as obs
from obstore.exceptions import GenericError
from obstore.store import LocalStore

HERE = Path(".")


def test_local_store():
    store = LocalStore(HERE)
    list_result = obs.list(store).collect()
    assert any("test_local.py" in x["path"] for x in list_result)


def test_repr():
    store = LocalStore(HERE)
    assert repr(store).startswith("LocalStore")


def test_local_from_url():
    with pytest.raises(ValueError):
        LocalStore.from_url("")

    LocalStore.from_url("file://")
    LocalStore.from_url("file:///")

    url = f"file://{HERE.absolute()}"
    store = LocalStore.from_url(url)
    list_result = obs.list(store).collect()
    assert any("test_local.py" in x["path"] for x in list_result)

    # Test with trailing slash
    url = f"file://{HERE.absolute()}/"
    store = LocalStore.from_url(url)
    list_result = obs.list(store).collect()
    assert any("test_local.py" in x["path"] for x in list_result)

    # Test with two trailing slashes
    url = f"file://{HERE.absolute()}//"
    with pytest.raises(GenericError):
        store = LocalStore.from_url(url)
