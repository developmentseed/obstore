# object-store-rs

A Python interface and pyo3 integration to the Rust [`object_store`](https://docs.rs/object_store/latest/object_store/) crate. This crate provides a uniform API for interacting with object storage services and local files. Using this library, the same code can run in multiple clouds and local test environments, via a simple runtime configuration change.

<!-- For Rust developers looking to add object_store support to their Python packages, refer to pyo3-object_store. -->

- Easy to install with no Python dependencies.
- Full static type hinting
- Full sync and async API
- Helpers for constructing from environment variables and `boto3.Session` objects

Among the included backend are:

- Amazon S3 and S3-compliant APIs like Cloudflare R2
- Google Cloud Storage
- Azure Blob Gen1 and Gen2 accounts (including ADLS Gen2)
- Local filesystem
- In-memory storage



## Installation

```sh
pip install object-store-rs
```

## Comparison to object-store-python

- More maintainable API than object-store-python.
- Fewer classes. Use native Python (typed) dicts and objects where possible.

## Usage

### Constructing a store

For ease of use and accurate validation, there are separate classes for each backend.

TODO: finish doc here

#### Configuration

- Each store concept has their own configuration. This is covered in the docs, and string literals are in the type hints.

### Interacting with a store

All methods for interacting with a store are exported as top-level functions,
such as `get`, `put`, `list`, and `delete`.

```py
import object_store_rs as obs

store = obs.store.MemoryStore()

obs.put(store, "file.txt", b"hello world!")
response = obs.get(store, "file.txt")
response.meta
# {'size': 12,
#  'last_modified': datetime.datetime(2024, 10, 18, 4, 8, 12, 57046, tzinfo=datetime.timezone.utc),
#  'version': None,
#  'e_tag': '0',
#  'location': 'file.txt'}

assert response.bytes() == b"hello world!"

byte_range = obs.get_range(store, "file.txt", offset=0, length=5)
assert byte_range == b"hello"

obs.copy(store, "file.txt", "other.txt")
assert obs.get(store, "other.txt").bytes() == b"hello world!"
```

All of these methods also have `async` counterparts, suffixed with `_async`.

```py
import object_store_rs as obs

store = obs.store.MemoryStore()

await obs.put_async(store, "file.txt", b"hello world!")
response = await obs.get_async(store, "file.txt")
response.meta
# {
#     "last_modified": datetime.datetime(
#         2024, 10, 18, 4, 14, 39, 630310, tzinfo=datetime.timezone.utc
#     ),
#     "size": 12,
#     "location": "file.txt",
#     "version": None,
#     "e_tag": "0",
# }
assert await response.bytes_async() == b"hello world!"

byte_range = await obs.get_range_async(store, "file.txt", offset=0, length=5)
assert byte_range == b"hello"

await obs.copy_async(store, "file.txt", "other.txt")
resp = await obs.get_async(store, "other.txt")
assert await resp.bytes_async() == b"hello world!"
```
