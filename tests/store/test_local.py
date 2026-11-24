import pickle
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from obstore.exceptions import GenericError
from obstore.store import LocalStore

HERE = Path()


def test_local_store():
    store = LocalStore(HERE)
    list_result = store.list().collect()
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
    list_result = store.list().collect()
    assert any("test_local.py" in x["path"] for x in list_result)

    # Test with trailing slash
    url = f"file://{HERE.absolute()}/"
    store = LocalStore.from_url(url)
    list_result = store.list().collect()
    assert any("test_local.py" in x["path"] for x in list_result)

    # Test with two trailing slashes
    url = f"file://{HERE.absolute()}//"
    with pytest.raises(GenericError):
        store = LocalStore.from_url(url)


def test_create_prefix():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        tmpdir = tmp_path / "abc"
        assert not tmpdir.exists()
        LocalStore(tmpdir, mkdir=True)
        assert tmpdir.exists()

        # Assert that mkdir=True works even when the dir already exists
        LocalStore(tmpdir, mkdir=True)
        assert tmpdir.exists()


def test_prefix_property():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = LocalStore(tmp_path)
        assert store.prefix == tmp_path
        assert isinstance(store.prefix, Path)
        # Can pass it back to the store init
        LocalStore(store.prefix)


def test_pickle():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = LocalStore(tmp_path)
        store.put("path.txt", b"foo")
        new_store: LocalStore = pickle.loads(pickle.dumps(store))
        assert new_store.get("path.txt").bytes() == b"foo"


def test_eq():
    store = LocalStore(HERE, automatic_cleanup=True)
    store2 = LocalStore(HERE, automatic_cleanup=True)
    store3 = LocalStore(HERE)
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3

    assert LocalStore(HERE, fsync=True) == LocalStore(HERE, fsync=True)
    assert LocalStore(HERE, fsync=True) != LocalStore(HERE)


def test_fsync():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = LocalStore(tmp_path, fsync=True)

        store.put("a.txt", b"foo")
        assert store.get("a.txt").bytes() == b"foo"

        data = b"0123456789" * 1000
        store.put("nested/multipart.bin", data, use_multipart=True, chunk_size=1024)
        assert store.get("nested/multipart.bin").bytes() == data

        store.copy("a.txt", "copy/overwrite.txt")
        store.copy("a.txt", "copy/create.txt", overwrite=False)
        store.rename("copy/overwrite.txt", "rename/overwrite.txt")
        store.rename("copy/create.txt", "rename/create.txt", overwrite=False)

        assert store.get("rename/overwrite.txt").bytes() == b"foo"
        assert store.get("rename/create.txt").bytes() == b"foo"
        assert not (tmp_path / "copy" / "overwrite.txt").exists()
        assert not (tmp_path / "copy" / "create.txt").exists()


def test_fsync_pickle():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = LocalStore(tmp_path, fsync=True)
        _args, kwargs = store.__getnewargs_ex__()
        assert kwargs["fsync"] is True

        new_store: LocalStore = pickle.loads(pickle.dumps(store))
        assert new_store == store
        assert new_store != LocalStore(tmp_path)


def test_fsync_from_url():
    url = f"file://{HERE.absolute()}"
    store = LocalStore.from_url(url, fsync=True)
    _args, kwargs = store.__getnewargs_ex__()
    assert kwargs["fsync"] is True
    assert store == LocalStore.from_url(url, fsync=True)
    assert store != LocalStore.from_url(url)


def test_local_store_percent_encoded():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
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
