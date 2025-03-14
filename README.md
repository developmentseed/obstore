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

The simplest, highest-throughput [^1] Python interface to [S3][s3], [GCS][gcs], [Azure Storage][azure_storage], & other S3-compliant APIs, powered by Rust.

[s3]: https://aws.amazon.com/s3/
[gcs]: https://cloud.google.com/storage
[azure_storage]: https://learn.microsoft.com/en-us/azure/storage/common/storage-introduction

- Sync and async API with **full type hinting**.
- **Streaming downloads** with configurable chunking.
- **Streaming uploads** from files or async or sync iterators.
- **Streaming list**, with no need to paginate.
- Automatic [**multipart uploads**](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) for large file objects.
- Automatic **credential refresh** before expiration.
- File-like object API and [fsspec](https://github.com/fsspec/filesystem_spec) integration.
- Easy to install with **no required Python dependencies**.
- Optionally return list results in [Apache Arrow](https://arrow.apache.org/) format, which is faster and more memory-efficient than materializing Python `dict`s.
- Zero-copy data exchange between Rust and Python via the [buffer protocol](https://jakevdp.github.io/blog/2014/05/05/introduction-to-the-python-buffer-protocol/).

For Rust developers looking to add `object_store` support to their own Python packages, refer to [`pyo3-object_store`](https://docs.rs/pyo3-object_store/latest/pyo3_object_store/).

[^1]: Benchmarking is ongoing, but preliminary results indicate roughly [9x higher throughput than fsspec](https://github.com/geospatial-jeff/pyasyncio-benchmark/blob/fe8f290cb3282dcc3bc96cae06ed5f90ad326eff/test_results/cog_header_results.csv) and [2.8x higher throughput than aioboto3](https://github.com/geospatial-jeff/pyasyncio-benchmark/blob/40e67509a248c5102a6b1608bcb9773295691213/test_results/20250218_results/ec2_m5/aggregated_results.csv) for many concurrent, small, get requests from an async context.

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
