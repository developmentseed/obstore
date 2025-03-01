# Alternatives to Obstore

## Obstore vs fsspec

[Fsspec](https://github.com/fsspec/filesystem_spec) is a generic specification for pythonic filesystems. It includes implementations for several cloud storage providers, including [s3fs](https://github.com/fsspec/s3fs) for Amazon S3, [gcsfs](https://github.com/fsspec/gcsfs) for Google Cloud Storage, and [adlfs](https://github.com/fsspec/adlfs) for Azure Storage.

### API Differences

Like Obstore, fsspec presents an abstraction layer that allows you to write code once to interface to multiple cloud providers. However, the abstracted API each presents is different. Obstore tries to mirror **native object store** APIs while fsspec tries to mirror a **file-like** API.

The upstream Rust library powering obstore, [`object_store`](https://docs.rs/object_store), documents why [it intentionally avoids](https://docs.rs/object_store/latest/object_store/index.html#why-not-a-filesystem-interface) a primary file-like API:

> The `ObjectStore` interface is designed to mirror the APIs of object stores and not filesystems, and thus has stateless APIs instead of cursor based interfaces such as `Read` or `Seek` available in filesystems.
>
> This design provides the following advantages:
>
> - All operations are atomic, and readers cannot observe partial and/or failed writes
> - Methods map directly to object store APIs, providing both efficiency and predictability
> - Abstracts away filesystem and operating system specific quirks, ensuring portability
> - Allows for functionality not native to filesystems, such as operation preconditions and atomic multipart uploads

Obstore's primary APIs, like [`get`][obstore.get], [`put`][obstore.put], and [`list`][obstore.list], mirror such object store APIs. However, if you still need to use a file-like API, Obstore provides such APIs with [`open_reader`][obstore.open_reader] and [`open_writer`][obstore.open_writer].

Obstore also includes a best-effort [fsspec compatibility layer][obstore.fsspec], which allows you to use obstore in applications that expect an fsspec-compatible API.

### Performance

Beyond API design, performance can also be a consideration. [Initial benchmarks](./performance.md) show that obstore's async API can provide 9x higher throughput than fsspec's async API.

## Obstore vs boto3

[boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) is the official Python client for working with AWS services, including S3.

boto3 supports all features of S3, including some features that obstore doesn't provide, like creating or deleting buckets.

However, boto3 is synchronous and specific to AWS. To support multiple clouds you'd need to use boto3 and another library and abstract away those differences yourself. With obstore you can interface with data in multiple clouds, changing only configuration settings.

## Obstore vs aioboto3

[aioboto3](https://github.com/terricain/aioboto3) is an async Python client for S3, wrapping boto3 and [aiobotocore](https://github.com/aio-libs/aiobotocore).

aioboto3 presents largely the same API as boto3, but async. As above, this means that it may support more S3-specific features than what obstore supports.

But it's still specific to AWS, and in early [benchmarks](./performance.md) we've measured obstore to provide significantly higher throughput than aioboto3.

## Obstore vs Google Cloud Storage Python Client

The official [Google Cloud Storage Python client](https://cloud.google.com/python/docs/reference/storage/latest) [uses requests](https://github.com/googleapis/python-storage/blob/f2cc9c5a2b1cc9724ca1269b8d452304da96bf03/setup.py#L42) as its HTTP client. This means that the GCS Python client supports only synchronous requests.

It also presents a Google-specific API, so you'd need to re-implement your code if you want to use multiple cloud providers.
