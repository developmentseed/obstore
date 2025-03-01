---
draft: false
date: 2025-02-10
categories:
  - Release
authors:
  - kylebarron
links:
  - CHANGELOG.md
---

# Releasing obstore 0.4!

Obstore is the simplest, highest-throughput Python interface to Amazon S3, Google Cloud Storage, and Azure Storage, powered by Rust.

This post gives an overview of what's new in obstore version 0.4.

<!-- more -->

Refer to the [changelog](../../CHANGELOG.md#040-2025-02-10) for all updates.

## Easier store creation with `from_url`

There's a new top-level [`obstore.store.from_url`][] function, which makes it dead-simple to create a store from a URL.

Here's an example of using it to inspect data from the [Sentinel-2 open data bucket](https://registry.opendata.aws/sentinel-2-l2a-cogs/). `from_url` automatically infers that this is an S3 path and constructs an [`S3Store`][obstore.store.S3Store], which we can pass to [`obstore.list_with_delimiter`][] and [`obstore.get`][].

```py
import obstore as obs
from obstore.store import from_url

# The base path within the bucket to "mount" to
url = "s3://sentinel-cogs/sentinel-s2-l2a-cogs/12/S/UF/2022/6/S2A_12SUF_20220601_0_L2A"

# Pass in store-specific parameters as keyword arguments
# Here, we pass `skip_signature=True` because it's a public bucket
store = from_url(url, region="us-west-2", skip_signature=True)

# Print filenames in this directory
print([meta["path"] for meta in obs.list_with_delimiter(store)["objects"]])
# ['AOT.tif', 'B01.tif', 'B02.tif', 'B03.tif', 'B04.tif', 'B05.tif', 'B06.tif', 'B07.tif', 'B08.tif', 'B09.tif', 'B11.tif', 'B12.tif', 'B8A.tif', 'L2A_PVI.tif', 'S2A_12SUF_20220601_0_L2A.json', 'SCL.tif', 'TCI.tif', 'WVP.tif', 'granule_metadata.xml', 'thumbnail.jpg', 'tileinfo_metadata.json']

# Download thumbnail
with open("thumbnail.jpg", "wb") as f:
    f.write(obs.get(store, "thumbnail.jpg").bytes())
```

And voilà, we have a thumbnail of the Grand Canyon from space:

![](../../assets/sentinel2-grca-thumbnail-obstore-04.jpg)

`from_url` also supports typing overloads. So your type checker will raise an error if you try to mix AWS-specific and Azure-specific configuration.

Nevertheless, for best typing support, we still suggest using one of the store-specific `from_url` constructors (such as [`S3Store.from_url`][obstore.store.from_url]) if you know the protocol. Then your type checker can infer the type of the returned store.


## Pickle support

One of obstore's initial integration targets is [zarr-python](https://github.com/zarr-developers/zarr-python), which needs to load large chunked N-dimensional arrays from object storage. In our [early benchmarking](https://github.com/maxrjones/zarr-obstore-performance), we've found that the [obstore-based backend](https://github.com/zarr-developers/zarr-python/pull/1661) can cut data loading times in half as compared to the standard fsspec-based backend.

However, Zarr is commonly used in distributed execution environments like [Dask](https://www.dask.org/), which needs to be able to move store instances between workers. We've implemented [pickle](https://docs.python.org/3/library/pickle.html) support for store classes to unblock this use case. Read [our pickle documentation](../../advanced/pickle.md) for more info.

## Enhanced loading of AWS credentials (provisional)

By default, each store class expects to find credential information either in environment variables or in passed-in arguments. In the case of AWS, that means the default constructors will not look in file-based credentials sources.

The provisional [`S3Store._from_native`](https://developmentseed.org/obstore/v0.4.0/api/store/aws/#obstore.store.S3Store._from_native) constructor uses the [official AWS Rust configuration crate](https://docs.rs/aws-config/latest/aws_config/) to find credentials on the file system. This integration is expected to also automatically refresh temporary credentials before expiration.

This API is provisional and may change in the future. If you have any feedback, please [open an issue](https://github.com/developmentseed/obstore/issues/new/choose).

Obstore version 0.5 is expected to improve on extensible credentials by enabling users to pass in arbitrary credentials in a sync or async function callback.

## Return Arrow data from `list_with_delimiter`

By default, the [`obstore.list`][] and [`obstore.list_with_delimiter`][] APIs [return standard Python `dict`s][obstore.ObjectMeta]. However, if you're listing a large bucket, the overhead of materializing all those Python objects can become significant.

[`obstore.list`][] and [`obstore.list_with_delimiter`][] now both support a `return_arrow` keyword parameter. If set to `True`, an Arrow [`RecordBatch`][arro3.core.RecordBatch] or [`Table`][arro3.core.Table] will be returned, which is both faster and more memory efficient.

## Access configuration values back from a store

There are new attributes, such as [`config`][obstore.store.S3Store.config], [`client_options`][obstore.store.S3Store.client_options], and [`retry_config`][obstore.store.S3Store.retry_config] for accessing configuration parameters _back_ from a store instance.

This example uses an [`S3Store`][obstore.store.S3Store] but the same behavior applies to [`GCSStore`][obstore.store.GCSStore] and [`AzureStore`][obstore.store.AzureStore] as well.

```py
from obstore.store import S3Store

store = S3Store.from_url(
    "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1",
    region="us-west-2",
    skip_signature=True,
)
new_store = S3Store(
    config=store.config,
    prefix=store.prefix,
    client_options=store.client_options,
    retry_config=store.retry_config,
)
assert store.config == new_store.config
assert store.prefix == new_store.prefix
assert store.client_options == new_store.client_options
assert store.retry_config == new_store.retry_config
```

## Open remote objects as file-like readers or writers

This version adds support for opening remote objects as a [file-like](../../api/file.md) reader or writer.

```py
import os

import obstore as obs
from obstore.store import MemoryStore

# Create an in-memory store
store = MemoryStore()

# Iteratively write to the file
with obs.open_writer(store, "new_file.csv") as writer:
    writer.write(b"col1,col2,col3\n")
    writer.write(b"a,1,True\n")
    writer.write(b"b,2,False\n")
    writer.write(b"c,3,True\n")


# Open a reader from the file
reader = obs.open_reader(store, "new_file.csv")
file_length = reader.seek(0, os.SEEK_END)
print(file_length) # 43
reader.seek(0)
buf = reader.read()
print(buf)
# Bytes(b"col1,col2,col3\na,1,True\nb,2,False\nc,3,True\n")
```

See [`obstore.open_reader`][] and [`obstore.open_writer`][] for more details. An async file-like reader and writer is also provided, see [`obstore.open_reader_async`][] and [`obstore.open_writer_async`][].

## Benchmarking

[Benchmarking is still ongoing](https://github.com/geospatial-jeff/pyasyncio-benchmark), but early results have been very promising and we've [added documentation about our progress so far](../../performance.md).

## New examples

We've worked to update the documentation with more examples! We now have examples for how to use obstore with [FastAPI](../../examples/fastapi.md), [MinIO](../../examples/minio.md), and [tqdm](../../examples/tqdm.md).

We've also worked to consolidate introductory documentation into the ["user guide"](../../getting-started.md).

## All updates

Refer to the [changelog](../../CHANGELOG.md#040-2025-02-10) for all updates.
