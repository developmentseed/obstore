from obstore.store import ObjectStore

class PrefixStore:
    """Store wrapper that applies a constant prefix to all paths handled by the store.

    **Example**:

    ```py
    import obstore as obs
    from obstore.store import MemoryStore, PrefixStore

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
    ```
    """
    def __init__(self, store: ObjectStore, prefix: str) -> None:
        """Create a new PrefixStore with the provided prefix.

        Args:
            store: The underlying store to wrap.
            prefix: If the prefix does not end with `/`, one will be added.
        """

    def __repr__(self) -> str: ...
