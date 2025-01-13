import obstore as obs
from obstore.store import MemoryStore, PrefixStore


def test_prefix_store():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog"
    path = "a/b/c/data.txt"

    obs.put(store, path, data)

    prefix_store = PrefixStore(store, "a/")
    assert obs.get(prefix_store, "b/c/data.txt").bytes() == data

    # The / after the passed-in prefix is inferred
    prefix_store2 = PrefixStore(store, "a")
    assert obs.get(prefix_store2, "b/c/data.txt").bytes() == data

    # The prefix is removed from list results
    assert obs.list(prefix_store).collect()[0]["path"] == "b/c/data.txt"

    # More deeply nested prefix
    prefix_store3 = PrefixStore(store, "a/b/c")
    assert obs.get(prefix_store3, "data.txt").bytes() == data
