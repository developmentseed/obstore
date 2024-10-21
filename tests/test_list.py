import object_store_rs as obs
from object_store_rs.store import MemoryStore


def test_list_max_items():
    store = MemoryStore()

    obs.put_file(store, "file1.txt", b"foo")
    obs.put_file(store, "file2.txt", b"bar")
    obs.put_file(store, "file3.txt", b"baz")

    assert len(obs.list(store)) == 3
    assert len(obs.list(store, max_items=2)) == 2
    assert len(obs.list(store, max_items=1)) == 1
    assert len(obs.list(store, max_items=0)) == 1
