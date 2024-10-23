import object_store_rs as obs
from object_store_rs.store import MemoryStore


def test_list():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo")
    obs.put(store, "file2.txt", b"bar")
    obs.put(store, "file3.txt", b"baz")

    stream = obs.list(store)
    result = stream.collect()
    assert len(result) == 3
