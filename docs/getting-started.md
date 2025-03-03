# Getting Started

There are two parts to `obstore`:

1. Constructing a `Store`, a representation of a remote object store with configuration and credentials.
2. Interacting with the `Store` to download, upload, move, and delete objects.

## Constructing a store

You can use the top-level [`from_url`][obstore.store.from_url] function to construct a store from a top-level URL. Among others this supports `s3://bucket/path`, `gs://bucket/path`, `az://account/container/path`, and `https://mydomain/path`.

Alternatively, you can construct a store directly:

- [`S3Store`][obstore.store.S3Store]: Configure a connection to Amazon S3.
- [`GCSStore`][obstore.store.GCSStore]: Configure a connection to Google Cloud Storage.
- [`AzureStore`][obstore.store.AzureStore]: Configure a connection to Microsoft Azure Blob Storage.
- [`HTTPStore`][obstore.store.HTTPStore]: Configure a connection to a generic HTTP server
- [`LocalStore`][obstore.store.LocalStore]: Local filesystem storage providing the same object store interface.
- [`MemoryStore`][obstore.store.MemoryStore]: A fully in-memory implementation of ObjectStore.

Each store concept has a variety of constructors, and a host of configuration options.

Note that each store is scoped to **one bucket**, so you'll have to create a separate store instance per bucket, even if they're in the same region.

**Example:**

For example, multiple ways to create an anonymous `S3Store` client (without any credentials, for use with fully public buckets):

```py
from obstore.store import S3Store, from_url

from_url("s3://bucket-name", region="us-east-1", skip_signature=True)
from_url("https://bucket-name.s3.us-east-1.amazonaws.com", skip_signature=True)
store = S3Store("bucket-name", region="us-east-1", skip_signature=True)
```

The process is similar for `GCSStore` and `AzureStore`.

### Configuration

Each store class above has its own store-specific configuration. Elements of the store configuration can be passed as keyword arguments or as a dictionary through the `config` named parameter.

- [`S3Config`][obstore.store.S3Config]: Configuration parameters for Amazon S3.
- [`GCSConfig`][obstore.store.GCSConfig]: Configuration parameters for Google Cloud Storage.
- [`AzureConfig`][obstore.store.AzureConfig]: Configuration parameters for Microsoft Azure Blob Storage.

Additionally, each store accepts parameters for the underlying HTTP client ([`ClientConfig`][obstore.store.ClientConfig]) and parameters for retrying requests that error ([`RetryConfig`][obstore.store.RetryConfig]).

## Interacting with a store

All operations for interacting with a store are exported as **top-level functions** (not methods on the `store` object):

- [`copy`][obstore.copy]: Copy an object from one path to another in the same object store.
- [`delete`][obstore.delete]: Delete the object at the specified location.
- [`get`][obstore.get]: Return the bytes that are stored at the specified location.
- [`head`][obstore.head]: Return the metadata for the specified location
- [`list`][obstore.list]: List all the objects with the given prefix.
- [`put`][obstore.put]: Save the provided buffer to the specified location.
- [`rename`][obstore.rename]: Move an object from one path to another in the same object store.

There are a few additional APIs useful for specific use cases:

- [`get_range`][obstore.get_range]: Get a specific byte range from a file.
- [`get_ranges`][obstore.get_ranges]: Get multiple byte ranges from a single file.
- [`list_with_delimiter`][obstore.list_with_delimiter]: List objects within a specific directory.
- [`sign`][obstore.sign]: Create a signed URL.

File-like object support is also provided:

- [`open_reader`][obstore.open_reader]: Open a remote object as a readable file-like object, similar to a Python [`BufferedReader`](https://docs.python.org/3/library/io.html#io.BufferedReader).
- [`open_writer`][obstore.open_writer]: Open a remote object as a writable file-like object, similar to a Python [`BufferedWriter`](https://docs.python.org/3/library/io.html#io.BufferedWriter)
- [`FsspecStore`][obstore.fsspec.FsspecStore] adapter for use with [`fsspec`](https://github.com/fsspec/filesystem_spec).

**All operations have a comparable async method** with the same name plus an `_async` suffix.

### Example

```py
import obstore as obs

store = obs.store.MemoryStore()

obs.put(store, "file.txt", b"hello world!")
response = obs.get(store, "file.txt")
response.meta
# {'path': 'file.txt',
#  'last_modified': datetime.datetime(2024, 10, 21, 16, 19, 45, 102620, tzinfo=datetime.timezone.utc),
#  'size': 12,
#  'e_tag': '0',
#  'version': None}
assert response.bytes() == b"hello world!"

byte_range = obs.get_range(store, "file.txt", start=0, end=5)
assert byte_range == b"hello"

obs.copy(store, "file.txt", "other.txt")
assert obs.get(store, "other.txt").bytes() == b"hello world!"
```

All of these methods also have `async` counterparts, suffixed with `_async`.

```py
import obstore as obs

store = obs.store.MemoryStore()

await obs.put_async(store, "file.txt", b"hello world!")
response = await obs.get_async(store, "file.txt")
response.meta
# {'path': 'file.txt',
#  'last_modified': datetime.datetime(2024, 10, 21, 16, 20, 36, 477418, tzinfo=datetime.timezone.utc),
#  'size': 12,
#  'e_tag': '0',
#  'version': None}
assert await response.bytes_async() == b"hello world!"

byte_range = await obs.get_range_async(store, "file.txt", start=0, end=5)
assert byte_range == b"hello"

await obs.copy_async(store, "file.txt", "other.txt")
resp = await obs.get_async(store, "other.txt")
assert await resp.bytes_async() == b"hello world!"
```
