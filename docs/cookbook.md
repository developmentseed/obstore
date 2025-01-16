# Cookbook

## List objects

Use the [`obstore.list`][] method.

```py
import obstore as obs

# Create a Store
store = get_object_store()

# Recursively list all files below the 'data' path.
# 1. On AWS S3 this would be the 'data/' prefix
# 2. On a local filesystem, this would be the 'data' directory
prefix = "data"

# Get a stream of metadata objects:
list_stream = obs.list(store, prefix)

# Print info
for batch in list_stream:
    for meta in batch:
        print(f"Name: {meta.path}, size: {meta.size}")
```

## List objects as Arrow

The default `list` behavior creates many small Python `dict`s. When listing a large bucket, generating these Python objects can add up to a lot of overhead.

Instead, you may consider passing `return_arrow=True` to [`obstore.list`][] to return each chunk of list results as an [Arrow](https://arrow.apache.org/) [`RecordBatch`][arro3.core.RecordBatch]. This can be much faster than materializing Python objects for each row because Arrow can be shared zero-copy between Rust and Python.

This Arrow integration requires the [`arro3-core` dependency](https://kylebarron.dev/arro3/latest/), a lightweight Arrow implementation. You can pass the emitted `RecordBatch` to [`pyarrow`](https://arrow.apache.org/docs/python/index.html) (zero-copy) by passing it to [`pyarrow.record_batch`][] or to [`polars`](https://pola.rs/) (also zero-copy) by passing it to `polars.DataFrame`.

```py
import obstore as obs

# Create a Store
store = get_object_store()

# Get a stream of Arrow RecordBatches of metadata
list_stream = obs.list(store, prefix="data", return_arrow=True)
for record_batch in list_stream:
    print(record_batch.num_rows)
```

Here's a working example with the [`sentinel-cogs` bucket](https://registry.opendata.aws/sentinel-2-l2a-cogs/) in AWS Open Data:

```py
import obstore as obs
import pandas as pd
import pyarrow as pa
from obstore.store import S3Store

store = S3Store("sentinel-cogs", region="us-west-2", skip_signature=True)
stream = obs.list(store, chunk_size=20, return_arrow=True)

for record_batch in stream:
    # Convert to pyarrow (zero-copy), then to pandas for easy export to a
    # Markdown table
    df = pa.record_batch(record_batch).to_pandas()
    print(df.iloc[:5].to_markdown(index=False))
    break
```

The Arrow record batch looks like the following:

| path                                                                | last_modified             |     size | e_tag                                | version   |
|:--------------------------------------------------------------------|:--------------------------|---------:|:-------------------------------------|:----------|
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/AOT.tif | 2020-09-30 20:25:56+00:00 |    50510 | "2e24c2ee324ea478f2f272dbd3f5ce69"   |           |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B01.tif | 2020-09-30 20:22:48+00:00 |  1455332 | "a31b78e96748ccc2b21b827bef9850c1"   |           |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B02.tif | 2020-09-30 20:23:19+00:00 | 38149405 | "d7a92f88ad19761081323165649ce799-5" |           |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B03.tif | 2020-09-30 20:23:52+00:00 | 38123224 | "4b938b6969f1c16e5dd685e6599f115f-5" |           |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B04.tif | 2020-09-30 20:24:21+00:00 | 39033591 | "4781b581cd32b2169d0b3d22bf40a8ef-5" |           |

## Fetch objects

Use the [`obstore.get`][] function to fetch data bytes from remote storage or files in the local filesystem.

```py
import obstore as obs

# Create a Store
store = get_object_store()

# Retrieve a specific file
path = "data/file01.parquet"

# Fetch just the file metadata
meta = obs.head(store, path)
print(meta)

# Fetch the object including metadata
result = obs.get(store, path)
assert result.meta == meta

# Buffer the entire object in memory
buffer = result.bytes()
assert len(buffer) == meta.size

# Alternatively stream the bytes from object storage
stream = obs.get(store, path).stream()

# We can now iterate over the stream
total_buffer_len = 0
for chunk in stream:
    total_buffer_len += len(chunk)

assert total_buffer_len == meta.size
```

## Put object

Use the [`obstore.put`][] function to atomically write data. `obstore.put` will automatically use [multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) for large input data.

```py
import obstore as obs

store = get_object_store()
path = "data/file1"
content = b"hello"
obs.put(store, path, content)
```

You can also upload local files:

```py
from pathlib import Path
import obstore as obs

store = get_object_store()
path = "data/file1"
content = Path("path/to/local/file")
obs.put(store, path, content)
```

Or file-like objects:

```py
import obstore as obs

store = get_object_store()
path = "data/file1"
with open("path/to/local/file", "rb") as content:
    obs.put(store, path, content)
```

Or iterables:

```py
import obstore as obs

def bytes_iter():
    for i in range(5):
        yield b"foo"

store = get_object_store()
path = "data/file1"
content = bytes_iter()
obs.put(store, path, content)
```


Or async iterables:

```py
import obstore as obs

async def bytes_stream():
    for i in range(5):
        yield b"foo"

store = get_object_store()
path = "data/file1"
content = bytes_stream()
obs.put(store, path, content)
```

## Copy objects from one store to another

Perhaps you have data in AWS S3 that you need to copy to Google Cloud Storage. It's easy to **stream** a `get` from one store directly to the `put` of another.

!!! note
    Using the async API is required for this.

```py
import obstore as obs

store1 = get_object_store()
store2 = get_object_store()

path1 = "data/file1"
path2 = "data/file1"

# This only constructs the stream, it doesn't materialize the data in memory
resp = await obs.get_async(store1, path1, timeout="2min")

# A streaming upload is created to copy the file to path2
await obs.put_async(store2, path2)
```

!!! note
    You may need to increase the download timeout for large source files. The timeout defaults to 30 seconds, which may not be long enough to upload the file to the destination.

    You may set the [`timeout` parameter][obstore.store.ClientConfig] in the `client_options` passed to the initial `get_async` call.
