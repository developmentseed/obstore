---
draft: false
date: 2025-06-25
categories:
  - Release
authors:
  - kylebarron
links:
  - CHANGELOG.md
---

# Releasing obstore 0.7!

Obstore is the simplest, highest-throughput Python interface to Amazon S3, Google Cloud Storage, and Azure Storage, powered by Rust.

This post gives an overview of what's new in obstore version 0.7.

<!-- more -->

Refer to the [changelog](../../CHANGELOG.md) for all updates.

## Anonymous connections to Google Cloud Storage

Obstore now supports anonymous connections to GCS. Pass [`skip_signature=True`][obstore.store.GCSConfig.skip_signature] to configure an anonymous connection.

```py
from obstore.store import GCSStore

store = GCSStore(
    "weatherbench2",
    prefix="datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
    # Anonymous connection
    skip_signature=True,
)
store.list_with_delimiter()["objects"]
```

Now prints:

```py
[{'path': '.zattrs',
  'last_modified': datetime.datetime(2023, 11, 22, 9, 4, 54, 481000, tzinfo=datetime.timezone.utc),
  'size': 2,
  'e_tag': '"99914b932bd37a50b983c5e7c90ae93b"',
  'version': None},
 {'path': '.zgroup',
  'last_modified': datetime.datetime(2023, 11, 22, 9, 4, 53, 465000, tzinfo=datetime.timezone.utc),
  'size': 24,
  'e_tag': '"e20297935e73dd0154104d4ea53040ab"',
  'version': None},
 {'path': '.zmetadata',
  'last_modified': datetime.datetime(2023, 11, 22, 9, 4, 54, 947000, tzinfo=datetime.timezone.utc),
  'size': 46842,
  'e_tag': '"9d287796ca614bfec4f1bb20a4ac1ba3"',
  'version': None}]
```

## Obspec v0.1 compatibility

Obstore provides an implementation for accessing Amazon S3, Google Cloud Storage, and Azure Storage, but some libraries may want to also support other backends, such as HTTP clients or more obscure things like SFTP or HDFS filesystems.

Additionally, there's a bunch of useful behavior that could exist on top of Obstore: caching, metrics, globbing, bulk operations. While all of those operations are useful, we want to keep the core Obstore library as small as possible, tightly coupled with the underlying Rust `object_store` library.

[Obspec](https://developmentseed.org/obspec/) exists to provide the abstractions for generic programming against object store backends. Obspec is essentially a formalization and generalization of the Obstore API, so if you're already using Obstore, very few changes are needed to use Obspec instead.

Downstream libraries can program against the Obspec API to be fully generic around what underlying backend is used at runtime.

For further information, refer to the [Obspec documentation](https://developmentseed.org/obspec/latest/) and the [Obspec announcement blog post](https://developmentseed.org/obspec/latest/blog/2025/06/25/introducing-obspec-a-python-protocol-for-interfacing-with-object-storage/).

## Customize headers sent in requests

`ClientConfig` now accepts a [`default_headers` key][obstore.store.ClientConfig.default_headers]. This allows you to add additional headers that will be sent by the HTTP client on every request.

## Improvements to NASA Earthdata credential provider

The NASA Earthdata credential provider now allows user to customize the host that handles credentialization.

It also allows for more possibilities of passing credentials. Authentication information can be a NASA Earthdata token, NASA Earthdata username/password (tuple), or `None`, in which case, environment variables or a `~/.netrc` file are used, if set.

See updated documentation on the [NASA Earthdata page](../../../../../api/auth/earthdata/).

## Fixed creation of `AzureStore` from HTTPS URL

Previously, this would create an incorrect AzureStore configuration:

```py
url = "https://overturemapswestus2.blob.core.windows.net/release"
store = AzureStore.from_url(url, skip_signature=True)
```

because it would interpret `release` as part of the within-bucket _prefix_, when it should really be interpreted as the _container name_.

This is now fixed and this test passes:

```py
url = "https://overturemapswestus2.blob.core.windows.net/release"
store = AzureStore.from_url(url, skip_signature=True)

assert store.config.get("container_name") == "release"
assert store.config.get("account_name") == "overturemapswestus2"
assert store.prefix is None
```

## Improved documentation

- [New `Zarr` example](../../examples/zarr.md)
- [New `stream-zip` example](../../examples/stream-zip.md)

## All updates

Refer to the [changelog](../../CHANGELOG.md) for all updates.
