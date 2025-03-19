# Cookbook

## List objects

Use the [`obstore.list`][] method.

```py
import obstore as obs

store = ... # store of your choice

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

store = ... # store of your choice

# Get a stream of Arrow RecordBatches of metadata
list_stream = obs.list(store, prefix="data", return_arrow=True)
for record_batch in list_stream:
    # Perform zero-copy conversion to your arrow-backed library of choice
    #
    # To pyarrow:
    # pyarrow.record_batch(record_batch)
    #
    # To polars:
    # polars.DataFrame(record_batch)
    #
    # To pandas (with Arrow-backed data-types):
    # pyarrow.record_batch(record_batch).to_pandas(types_mapper=pd.ArrowDtype)
    #
    # To arro3:
    # arro3.core.RecordBatch(record_batch)
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

| path                                                                | last_modified             |     size | e_tag                                | version |
| :------------------------------------------------------------------ | :------------------------ | -------: | :----------------------------------- | :------ |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/AOT.tif | 2020-09-30 20:25:56+00:00 |    50510 | "2e24c2ee324ea478f2f272dbd3f5ce69"   |         |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B01.tif | 2020-09-30 20:22:48+00:00 |  1455332 | "a31b78e96748ccc2b21b827bef9850c1"   |         |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B02.tif | 2020-09-30 20:23:19+00:00 | 38149405 | "d7a92f88ad19761081323165649ce799-5" |         |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B03.tif | 2020-09-30 20:23:52+00:00 | 38123224 | "4b938b6969f1c16e5dd685e6599f115f-5" |         |
| sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/B04.tif | 2020-09-30 20:24:21+00:00 | 39033591 | "4781b581cd32b2169d0b3d22bf40a8ef-5" |         |

## Fetch objects

Use the [`obstore.get`][] function to fetch data bytes from remote storage or files in the local filesystem.

```py
import obstore as obs

store = ... # store of your choice

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

### Download to disk

Using the response as an iterator ensures that we don't buffer the entire file
into memory.

```py
import obstore as obs

resp = obs.get(store, path)

with open("output/file", "wb") as f:
    for chunk in resp:
        f.write(chunk)
```

## Put object

Use the [`obstore.put`][] function to atomically write data. `obstore.put` will automatically use [multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) for large input data.

```py
import obstore as obs

store = ... # store of your choice
path = "data/file1"
content = b"hello"
obs.put(store, path, content)
```

You can also upload local files:

```py
from pathlib import Path
import obstore as obs

store = ... # store of your choice
path = "data/file1"
content = Path("path/to/local/file")
obs.put(store, path, content)
```

Or file-like objects:

```py
import obstore as obs

store = ... # store of your choice
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

store = ... # store of your choice
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

store = ... # store of your choice
path = "data/file1"
content = bytes_stream()
obs.put(store, path, content)
```

## Copy objects from one store to another

Perhaps you have data in one store, say AWS S3, that you need to copy to another, say Google Cloud Storage.

### In memory

Download the file, collect its bytes in memory, then upload it. Note that this will materialize the entire file in memory.

```py
import obstore as obs

store1 = ... # store of your choice
store2 = ... # store of your choice

path1 = "data/file1"
path2 = "data/file2"

buffer = obs.get(store1, path1).bytes()
obs.put(store2, path2, buffer)
```

### Local file

First download the file to disk, then upload it.

```py
from pathlib import Path
import obstore as obs

store1 = ... # store of your choice
store2 = ... # store of your choice

path1 = "data/file1"
path2 = "data/file2"

resp = obs.get(store1, path1)

with open("temporary_file", "wb") as f:
    for chunk in resp:
        f.write(chunk)

# Upload the path
obs.put(store2, path2, Path("temporary_file"))
```

### Streaming

It's easy to **stream** a download from one store directly as the upload to another. Only the given

!!! note

    Using the async API is currently required to use streaming copies.

```py
import obstore as obs

store1 = ... # store of your choice
store2 = ... # store of your choice

path1 = "data/file1"
path2 = "data/file2"

# This only constructs the stream, it doesn't materialize the data in memory
resp = await obs.get_async(store1, path1)
# A streaming upload is created to copy the file to path2
await obs.put_async(store2, path2, resp)
```

Or, by customizing the chunk size and the upload concurrency you can control memory overhead.

```py
resp = await obs.get_async(store1, path1)
chunk_size = 5 * 1024 * 1024 # 5MB
stream = resp.stream(min_chunk_size=chunk_size)

# A streaming upload is created to copy the file to path2
await obs.put_async(
    store2,
    path2,
    stream,
    chunk_size=chunk_size,
    max_concurrency=12
)
```

This will start up to 12 concurrent uploads, each with around 5MB chunks, giving a total memory usage of up to _roughly_ 60MB for this copy.

!!! note

    You may need to increase the download timeout for large source files. The timeout defaults to 30 seconds, which may not be long enough to upload the file to the destination.

    You may set the [`timeout` parameter][obstore.store.ClientConfig] in the `client_options` passed when creating the store.
