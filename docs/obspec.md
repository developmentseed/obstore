# Generic storage abstractions with Obspec

Obstore supports Amazon S3, Google Cloud Storage, and Azure Storage, but some libraries may want to also support other backends, such as HTTP clients or more obscure things like SFTP or HDFS filesystems.

Additionally, there's a bunch of useful behavior that could exist on top of Obstore stores: caching, metrics, globbing, bulk operations. While all of those operations are useful, we want to keep the core Obstore library as small as possible, tightly coupled with the underlying Rust `object_store` library.

[Obspec](https://developmentseed.org/obspec/) exists to provide the abstractions for generic programming against object store backends. Obspec is essentially a formalization and generalization of the Obstore API, so if you're already using Obstore, very few changes are needed to use Obspec instead.

Downstream libraries can program against the Obspec API to be fully generic around what underlying backend is used at runtime.

## Example

To download a file generically, depend on the [Get][obspec.Get] protocol, which defines a `get` method.

```py
from obspec import Get

def download_file(client: Get):
    response = client.get("my-file.txt")
    for buffer in response:
        # Process each buffer chunk as needed
        print(f"Received buffer of size: {len(memoryview(buffer))} bytes")
```

All Obstore stores implement the Obspec protocol. So you can now pass:

```py
from obstore.store import S3Store

store = S3Store("bucket", ...)
download_file(store)
```

## More Information

For further information, refer to the [Obspec documentation](https://developmentseed.org/obspec/latest/) and the [Obspec announcement blog post](https://developmentseed.org/obspec/latest/blog/2025/06/25/introducing-obspec-a-python-protocol-for-interfacing-with-object-storage/).
