import pytest

import obstore as obs
from obstore.exceptions import AlreadyExistsError
from obstore.store import MemoryStore


def test_put_non_multipart():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo", use_multipart=False)
    assert obs.get(store, "file1.txt").bytes() == b"foo"


def test_put_multipart_one_chunk():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo", use_multipart=True)
    assert obs.get(store, "file1.txt").bytes() == b"foo"


def test_put_multipart_large():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    obs.put(store, path, data, use_multipart=True)
    assert obs.get(store, path).bytes() == data


def test_put_mode():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo")
    obs.put(store, "file1.txt", b"bar", mode="overwrite")

    with pytest.raises(AlreadyExistsError):
        obs.put(store, "file1.txt", b"foo", mode="create")

    assert obs.get(store, "file1.txt").bytes() == b"bar"
