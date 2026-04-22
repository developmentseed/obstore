from obstore.store import MemoryStore


def test_head_non_ascii():
    store = MemoryStore()

    name1 = "café.txt"
    name2 = "ümlaut.txt"
    name3 = "こんにちは世界.txt"
    name4 = "你好世界.txt"

    store.put(name1, b"foo")
    store.put(name2, b"bar")
    store.put(name3, b"baz")
    store.put(name4, b"qux")

    assert store.head(name1)["path"] == name1
    assert store.head(name2)["path"] == name2
    assert store.head(name3)["path"] == name3
    assert store.head(name4)["path"] == name4
