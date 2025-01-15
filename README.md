# obstore

[![PyPI][pypi_badge]][pypi_link]
[![Conda Version][conda_version_badge]][conda_version]
[![PyPI - Downloads][pypi-img]][pypi-link]

[pypi_badge]: https://badge.fury.io/py/obstore.svg
[pypi_link]: https://pypi.org/project/obstore/
[conda_version_badge]: https://img.shields.io/conda/vn/conda-forge/obstore.svg
[conda_version]: https://prefix.dev/channels/conda-forge/packages/obstore
[pypi-img]: https://img.shields.io/pypi/dm/obstore
[pypi-link]: https://pypi.org/project/obstore/

Simple, fast integration with object storage services like Amazon S3, Google Cloud Storage, Azure Blob Storage, and S3-compliant APIs like Cloudflare R2.

- Sync and async API.
- Streaming downloads with configurable chunking.
- Streaming uploads from async or sync iterators.
- Streaming `list`, with no need to paginate.
- File-like object API and [fsspec](https://github.com/fsspec/filesystem_spec) integration.
- Support for conditional put ("put if not exists"), as well as custom tags and attributes.
- Automatically uses [multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) under the hood for large file objects.
- Optionally return list results as [Arrow](https://arrow.apache.org/), which is faster than materializing Python `dict`/`list` objects.
- Easy to install with no required Python dependencies.
- The [underlying Rust library](https://docs.rs/object_store) is production quality and used in large scale production systems, such as the Rust package registry [crates.io](https://crates.io/).
- Zero-copy data exchange between Rust and Python in `get_range`, `get_ranges`, `GetResult.bytes`, and `put` via the Python [buffer protocol](https://jakevdp.github.io/blog/2014/05/05/introduction-to-the-python-buffer-protocol/).
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

Head to [Getting Started](https://developmentseed.org/obstore/latest/getting-started/) to dig in.
