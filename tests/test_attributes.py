from obstore.store import MemoryStore


def test_content_type():
    store = MemoryStore()
    store.put("test.txt", b"Hello, World!", attributes={"Content-Type": "text/plain"})
    result = store.get("test.txt")
    assert result.attributes.get("Content-Type") == "text/plain"


def test_custom_attribute():
    store = MemoryStore()
    store.put(
        "test.txt",
        b"Hello, World!",
        attributes={"My-Custom-Attribute": "CustomValue"},
    )
    result = store.get("test.txt")
    assert result.attributes.get("My-Custom-Attribute") == "CustomValue"
