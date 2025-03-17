# ruff: noqa: ERA001

# from arro3.core import RecordBatch, Table

# # TODO: fix imports
# from obspec._get import Get, GetAsync
# from obspec._list import List, ListWithDelimiter, ListWithDelimiterAsync
# from obspec._put import Put, PutAsync

# from obstore.store import MemoryStore


# def store():
#     return MemoryStore()


# def accepts_get(store: Get):
#     store.get("path")


# async def accepts_get_async(store: GetAsync):
#     await store.get_async("path")


# def accepts_list(store: List):
#     objects = store.list().collect()
#     assert isinstance(objects, list)

#     objects = next(store.list(return_arrow=True))
#     _rb = RecordBatch(objects)


# def accepts_list_with_delimiter(store: ListWithDelimiter):
#     objects = store.list_with_delimiter()
#     assert isinstance(objects["objects"], list)

#     objects = store.list_with_delimiter(return_arrow=True)
#     _rb = Table(objects["objects"])


# async def accepts_list_with_delimiter_async(store: ListWithDelimiterAsync):
#     objects = await store.list_with_delimiter_async()
#     assert isinstance(objects["objects"], list)

#     objects = await store.list_with_delimiter_async(return_arrow=True)
#     _rb = Table(objects["objects"])


# def accepts_put(store: Put):
#     store.put("path", b"content")


# async def accepts_put_async(store: PutAsync):
#     await store.put_async("path", b"content")


# async def test_typing():
#     store = MemoryStore()

#     # Make sure to put "put" first
#     accepts_put(store)
#     await accepts_put_async(store)

#     accepts_get(store)
#     await accepts_get_async(store)
#     accepts_list(store)
#     accepts_list_with_delimiter(store)
#     await accepts_list_with_delimiter_async(store)
