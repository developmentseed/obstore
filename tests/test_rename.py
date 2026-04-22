from obstore.store import MemoryStore


def test_rename_non_ascii():
    store = MemoryStore()

    name1 = "café.txt"
    name2 = "ümlaut.txt"
    name3 = "こんにちは世界.txt"
    name4 = "你好世界.txt"

    store.put(name1, b"foo")
    store.put(name3, b"bar")

    store.rename(name1, name2)
    store.rename(name3, name4)

    result = store.list().collect()
    assert len(result) == 2
    assert result[0]["path"] == name2
    assert result[1]["path"] == name4
