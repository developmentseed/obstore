# Cookbook

## List objects

Use the [`obstore.list`][] method

## List object as Arrow

This can be faster than materializing Python objects for each row.

Use the [`obstore.list`][] method

## Fetch objects

Use the [`obstore.get`][] function to fetch data bytes from remote storage or files in the local filesystem.

```py
import obstore as obs

# Create a Store
store = get_object_store()

# Retrieve a specific file
path = "data/file01.parquet";

# Fetch just the file metadata
meta = obs.head(store, path)
print(meta)

# Fetch the object including metadata
result = obs.get(store, path)
assert result.meta == meta

# Buffer the entire object in memory
buffer = result.bytes();
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

Perhaps you have data in AWS S3 that you need to copy to Google Cloud Storage. It's easy to **stream** a `get` from one store directly to the `put` of another. The first file is not

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
