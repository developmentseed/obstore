import pickle
import tempfile
from pathlib import Path

import pytest

import obstore as obs
from obstore.exceptions import GenericError
from obstore.store import LocalStore

HERE = Path()


def test_local_store():
    store = LocalStore(HERE)
    list_result = obs.list(store).collect()
    assert any("test_local.py" in x["path"] for x in list_result)


def test_repr():
    store = LocalStore(HERE)
    assert repr(store).startswith("LocalStore")


def test_local_from_url():
    with pytest.raises(ValueError, match="relative URL without a base"):
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


def test_create_prefix():
    tmpdir = Path(tempfile.gettempdir()) / "abc"
    assert not tmpdir.exists()
    LocalStore(tmpdir, mkdir=True)
    assert tmpdir.exists()

    # Assert that mkdir=True works even when the dir already exists
    LocalStore(tmpdir, mkdir=True)
    assert tmpdir.exists()


def test_prefix_property():
    tmpdir = Path(tempfile.gettempdir())
    store = LocalStore(tmpdir)
    assert store.prefix == tmpdir
    assert isinstance(store.prefix, Path)
    # Can pass it back to the store init
    LocalStore(store.prefix)


def test_pickle():
    tmpdir = Path(tempfile.gettempdir())
    store = LocalStore(tmpdir)
    obs.put(store, "path.txt", b"foo")
    new_store: LocalStore = pickle.loads(pickle.dumps(store))
    assert obs.get(new_store, "path.txt").bytes() == b"foo"
