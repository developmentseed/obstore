from obstore.store import MemoryStore


def test_eq():
    store = MemoryStore()
    store2 = MemoryStore()
    assert store == store  # noqa: PLR0124
    assert store != store2
