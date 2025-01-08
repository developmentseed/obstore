# obstore

[![PyPI][pypi_badge]][pypi_link]
[![Conda Version][conda_version_badge]][conda_version]

[pypi_badge]: https://badge.fury.io/py/obstore.svg
[pypi_link]: https://pypi.org/project/obstore/
[conda_version_badge]: https://img.shields.io/conda/vn/conda-forge/obstore.svg
[conda_version]: https://prefix.dev/channels/conda-forge/packages/obstore

Simple, fast integration with object storage services like Amazon S3, Google Cloud Storage, Azure Blob Storage, and S3-compliant APIs like Cloudflare R2.

- Sync and async API.
- Streaming downloads with configurable chunking.
- Streaming `list`, with no need to paginate.
- File-like object API and [fsspec](https://github.com/fsspec/filesystem_spec) integration.
- Support for conditional put ("put if not exists"), as well as custom tags and attributes.
- Automatically uses [multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) under the hood for large file objects.
- Optionally return list results as [Arrow](https://arrow.apache.org/), which is faster than materializing Python `dict`/`list` objects.
- Easy to install with no required Python dependencies.
- The [underlying Rust library](https://docs.rs/object_store) is production quality and used in large scale production systems, such as the Rust package registry [crates.io](https://crates.io/).
- Support for zero-copy data exchange from Rust into Python in `get_range` and `get_ranges`.
- Simple API with static type checking.
- Helpers for constructing from environment variables and `boto3.Session` objects

<!-- For Rust developers looking to add object_store support to their Python packages, refer to pyo3-object_store. -->

## Installation

To install obstore using pip:

```sh
pip install obstore
```

Obstore is on [conda-forge](https://prefix.dev/channels/conda-forge/packages/obstore) and can be installed using [conda](https://docs.conda.io), [mamba](https://mamba.readthedocs.io/), or [pixi](https://pixi.sh/). To install obstore using conda:

```
conda install -c conda-forge obstore
```

## Documentation

[Full documentation is available on the website](https://developmentseed.org/obstore).

## Usage

### Constructing a store

Classes to construct a store are exported from the `obstore.store` submodule:

- [`S3Store`](https://developmentseed.org/obstore/latest/api/store/aws/): Configure a connection to Amazon S3.
- [`GCSStore`](https://developmentseed.org/obstore/latest/api/store/gcs/): Configure a connection to Google Cloud Storage.
- [`AzureStore`](https://developmentseed.org/obstore/latest/api/store/azure/): Configure a connection to Microsoft Azure Blob Storage.
- [`HTTPStore`](https://developmentseed.org/obstore/latest/api/store/http/): Configure a connection to a generic HTTP server
- [`LocalStore`](https://developmentseed.org/obstore/latest/api/store/local/): Local filesystem storage providing the same object store interface.
- [`MemoryStore`](https://developmentseed.org/obstore/latest/api/store/memory/): A fully in-memory implementation of ObjectStore.

#### Example

```py
import boto3
from obstore.store import S3Store

session = boto3.Session()
store = S3Store.from_session(session, "bucket-name", config={"AWS_REGION": "us-east-1"})
```

#### Configuration

Each store class above has its own configuration, accessible through the `config` named parameter. This is covered in the docs, and string literals are in the type hints.

Additional [HTTP client configuration](https://developmentseed.org/obstore/latest/api/store/config/) is available via the `client_options` named parameter.

### Interacting with a store

All methods for interacting with a store are exported as **top-level functions** (not methods on the `store` object):

- [`copy`](https://developmentseed.org/obstore/latest/api/copy/): Copy an object from one path to another in the same object store.
- [`delete`](https://developmentseed.org/obstore/latest/api/delete/): Delete the object at the specified location.
- [`get`](https://developmentseed.org/obstore/latest/api/get/): Return the bytes that are stored at the specified location.
- [`head`](https://developmentseed.org/obstore/latest/api/head/): Return the metadata for the specified location
- [`list`](https://developmentseed.org/obstore/latest/api/list/): List all the objects with the given prefix.
- [`put`](https://developmentseed.org/obstore/latest/api/put/): Save the provided bytes to the specified location
- [`rename`](https://developmentseed.org/obstore/latest/api/rename/): Move an object from one path to another in the same object store.

There are a few additional APIs useful for specific use cases:

- [`get_range`](https://developmentseed.org/obstore/latest/api/get/#obstore.get_range): Get a specific byte range from a file.
- [`get_ranges`](https://developmentseed.org/obstore/latest/api/get/#obstore.get_ranges): Get multiple byte ranges from a single file.
- [`list_with_delimiter`](https://developmentseed.org/obstore/latest/api/list/#obstore.list_with_delimiter): List objects within a specific directory.
- [`sign`](https://developmentseed.org/obstore/latest/api/sign/): Create a signed URL.

File-like object support is also provided:

- [`open`](https://developmentseed.org/obstore/latest/api/file/#obstore.open): Open a remote object as a Python file-like object.
- [`AsyncFsspecStore`](https://developmentseed.org/obstore/latest/api/fsspec/#obstore.fsspec.AsyncFsspecStore) adapter for use with [`fsspec`](https://github.com/fsspec/filesystem_spec).

All methods have a comparable async method with the same name plus an `_async` suffix.

#### Example

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

byte_range = obs.get_range(store, "file.txt", offset=0, length=5)
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

byte_range = await obs.get_range_async(store, "file.txt", offset=0, length=5)
assert byte_range == b"hello"

await obs.copy_async(store, "file.txt", "other.txt")
resp = await obs.get_async(store, "other.txt")
assert await resp.bytes_async() == b"hello world!"
```

## Comparison to object-store-python

[Read a detailed comparison](https://github.com/roeap/object-store-python/issues/24#issuecomment-2422689636) to [`object-store-python`](https://github.com/roeap/object-store-python), a previous Python library that also wraps the same Rust `object_store` crate.
