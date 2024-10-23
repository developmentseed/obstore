import object_store_py as obs
import pytest
from arro3.core import RecordBatch
from object_store_py.store import MemoryStore


def test_list():
    store = MemoryStore()

    obs.put(store, "file1.txt", b"foo")
    obs.put(store, "file2.txt", b"bar")
    obs.put(store, "file3.txt", b"baz")

    stream = obs.list(store)
    result = stream.collect()
    assert len(result) == 3


def test_list_as_arrow():
    store = MemoryStore()

    for i in range(100):
        obs.put(store, f"file{i}.txt", b"foo")

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    yielded_batches = 0
    for batch in stream:
        assert isinstance(batch, RecordBatch)
        yielded_batches += 1
        assert batch.num_rows == 10

    assert yielded_batches == 10

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    batch = stream.collect()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 100


@pytest.mark.asyncio
async def test_list_stream_async():
    store = MemoryStore()

    for i in range(100):
        await obs.put_async(store, f"file{i}.txt", b"foo")

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    yielded_batches = 0
    async for batch in stream:
        assert isinstance(batch, RecordBatch)
        yielded_batches += 1
        assert batch.num_rows == 10

    assert yielded_batches == 10

    stream = obs.list(store, return_arrow=True, chunk_size=10)
    batch = await stream.collect_async()
    assert isinstance(batch, RecordBatch)
    assert batch.num_rows == 100
