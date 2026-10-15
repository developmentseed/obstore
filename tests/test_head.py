from __future__ import annotations

from typing import TYPE_CHECKING

from obstore.store import MemoryStore, S3Store

if TYPE_CHECKING:
    from obstore.store import ClientConfig, S3Config


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


def test_head_with_prefix(minio_bucket: tuple[S3Config, ClientConfig]):
    # https://github.com/developmentseed/obstore/issues/744
    # The returned path should be relative to the store prefix, matching `list`.
    store = S3Store(
        config=minio_bucket[0],
        client_options=minio_bucket[1],
        prefix="test-prefix",
    )
    store.put("file1.txt", b"foo")

    assert store.head("file1.txt")["path"] == "file1.txt"
