import pickle
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


def test_create_prefix(tmp_path: Path):
    tmpdir = tmp_path / "abc"
    assert not tmpdir.exists()
    LocalStore(tmpdir, mkdir=True)
    assert tmpdir.exists()

    # Assert that mkdir=True works even when the dir already exists
    LocalStore(tmpdir, mkdir=True)
    assert tmpdir.exists()


def test_prefix_property(tmp_path: Path):
    store = LocalStore(tmp_path)
    assert store.prefix == tmp_path
    assert isinstance(store.prefix, Path)
    # Can pass it back to the store init
    LocalStore(store.prefix)


def test_pickle(tmp_path: Path):
    store = LocalStore(tmp_path)
    obs.put(store, "path.txt", b"foo")
    new_store: LocalStore = pickle.loads(pickle.dumps(store))
    assert obs.get(new_store, "path.txt").bytes() == b"foo"


def test_eq():
    store = LocalStore(HERE, automatic_cleanup=True)
    store2 = LocalStore(HERE, automatic_cleanup=True)
    store3 = LocalStore(HERE)
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3


def test_local_store_percent_encoded(tmp_path: Path):
    fname1 = "hello%20world.txt"
    content1 = b"Hello, World!"
    with (tmp_path / fname1).open("wb") as f:
        f.write(content1)

    store = LocalStore(tmp_path)
    assert store.get(fname1).bytes() == content1

    fname2 = "hello world.txt"
    content2 = b"Hello, World! (with spaces)"
    with (tmp_path / fname2).open("wb") as f:
        f.write(content2)

    assert store.get(fname2).bytes() == content2


def test_head(tmp_path: Path):
    store = LocalStore(tmp_path)
    store.put("freshly_created/some.txt", b"foo")

    # ✅ works fine
    assert store.head("freshly_created/some.txt")["path"] == "freshly_created/some.txt"

    # 🛑 fails on windows
    assert store.head("freshly_created")["path"] == "freshly_created"
