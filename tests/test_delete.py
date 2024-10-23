from tempfile import TemporaryDirectory

import object_store_rs as obs
import pytest
from object_store_rs.store import LocalStore, MemoryStore


def test_delete_one():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo")
    obs.put(store, "file2.txt", b"bar")
    obs.put(store, "file3.txt", b"baz")

    assert len(obs.list(store).collect()) == 3
    obs.delete(store, "file1.txt")
    obs.delete(store, "file2.txt")
    obs.delete(store, "file3.txt")
    assert len(obs.list(store).collect()) == 0


def test_delete_many():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo")
    obs.put(store, "file2.txt", b"bar")
    obs.put(store, "file3.txt", b"baz")

    assert len(obs.list(store).collect()) == 3
    obs.delete(
        store,
        ["file1.txt", "file2.txt", "file3.txt"],
    )
    assert len(obs.list(store).collect()) == 0


# Local filesystem errors if the file does not exist.
def test_delete_one_local_fs():
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)

        obs.put(store, "file1.txt", b"foo")
        obs.put(store, "file2.txt", b"bar")
        obs.put(store, "file3.txt", b"baz")

        assert len(obs.list(store).collect()) == 3
        obs.delete(store, "file1.txt")
        obs.delete(store, "file2.txt")
        obs.delete(store, "file3.txt")
        assert len(obs.list(store).collect()) == 0

        with pytest.raises(Exception, match="No such file"):
            obs.delete(store, "file1.txt")


def test_delete_many_local_fs():
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)

        obs.put(store, "file1.txt", b"foo")
        obs.put(store, "file2.txt", b"bar")
        obs.put(store, "file3.txt", b"baz")

        assert len(obs.list(store).collect()) == 3
        obs.delete(
            store,
            ["file1.txt", "file2.txt", "file3.txt"],
        )

        with pytest.raises(Exception, match="No such file"):
            obs.delete(
                store,
                ["file1.txt", "file2.txt", "file3.txt"],
            )
