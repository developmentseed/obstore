# Changelog

## [0.4.0] - 2025-02-10

### New Features :magic_wand:

- **Support for pickling** & always manage store prefix by @kylebarron in https://github.com/developmentseed/obstore/pull/185, https://github.com/developmentseed/obstore/pull/239, https://github.com/developmentseed/obstore/pull/223
- **Add top-level `obstore.store.from_url` function**, which delegates to each store's `from_url` constructor by @kylebarron in https://github.com/developmentseed/obstore/pull/179, https://github.com/developmentseed/obstore/pull/201
- Add option to return Arrow from `list_with_delimiter` by @kylebarron in https://github.com/developmentseed/obstore/pull/238, https://github.com/developmentseed/obstore/pull/244
- (Provisional) **Enhanced loading of s3 credentials** using `aws-config` crate by @kylebarron in https://github.com/developmentseed/obstore/pull/203
- **Access config values out from stores** by @kylebarron in https://github.com/developmentseed/obstore/pull/210
- LocalStore updates:
  - Enable automatic cleanup for local store, when deleting directories by @kylebarron in https://github.com/developmentseed/obstore/pull/175
  - Optionally create root dir in LocalStore by @kylebarron in https://github.com/developmentseed/obstore/pull/177
- **File-like object** updates:
  - Add support for writable file-like objects by @kylebarron in https://github.com/developmentseed/obstore/pull/167
  - Updates to readable file API:

    - Support user-specified capacity in readable file-like objects by @kylebarron in https://github.com/developmentseed/obstore/pull/174
    - Expose `ObjectMeta` from readable file API by @kylebarron in https://github.com/developmentseed/obstore/pull/176
- Merge `config` and `kwargs` and validate that no configuration parameters have been passed multiple times. (https://github.com/developmentseed/obstore/pull/180, https://github.com/developmentseed/obstore/pull/182, https://github.com/developmentseed/obstore/pull/218)
- Add `__repr__` to `Bytes` class by @jessekrubin in https://github.com/developmentseed/obstore/pull/173

### Breaking changes :wrench:

- `get_range`, `get_range_async`, `get_ranges`, and `get_ranges_async` now require named parameters for `start`, `end`, and `length` to make the semantics of the range request fully explicit. by @kylebarron in https://github.com/developmentseed/obstore/pull/156
- Previously, individual stores did not manage a prefix path within the remote resource and [`PrefixStore`](https://developmentseed.org/obstore/v0.3.0/api/store/middleware/#obstore.store.PrefixStore) was used to enable this. As of 0.4.0, `PrefixStore` was removed and all stores manage an optional mount prefix natively.
- `obstore.open` has been renamed to `obstore.open_reader`.
- The `from_env` constructor has been removed from `S3Store`, `GCSStore`, and `AzureStore`. Now all constructors will read from environment variables. Use `__init__` or `from_url` instead. https://github.com/developmentseed/obstore/pull/189
- `obstore.exceptions.ObstoreError` renamed to `obstore.exceptions.BaseError` https://github.com/developmentseed/obstore/pull/200

### Bug fixes :bug:

- Fix pylance finding exceptions module by @kylebarron in https://github.com/developmentseed/obstore/pull/183
- Allow passing in partial retry/backoff config by @kylebarron in https://github.com/developmentseed/obstore/pull/205
- Fix returning None from async functions by @kylebarron in https://github.com/developmentseed/obstore/pull/245
- Fix LocalStore range request past end of file, by @kylebarron in https://github.com/developmentseed/obstore/pull/230

### Documentation :book:

- Update wording for fsspec docstring by @kylebarron in https://github.com/developmentseed/obstore/pull/195
- Add documentation about AWS region by @kylebarron in https://github.com/developmentseed/obstore/pull/213
- Add developer documentation for functional API choice by @kylebarron in https://github.com/developmentseed/obstore/pull/215
- Add `tqdm` progress bar example by @kylebarron in https://github.com/developmentseed/obstore/pull/237
- Add contributor, performance, integrations docs by @kylebarron in https://github.com/developmentseed/obstore/pull/227
- Add minio example by @kylebarron in https://github.com/developmentseed/obstore/pull/241

### Other

- Use manylinux 2_24 for aarch64 linux wheels by @kylebarron in https://github.com/developmentseed/obstore/pull/225

### New Contributors

- @vincentsarago made their first contribution in https://github.com/developmentseed/obstore/pull/168
- @jessekrubin made their first contribution in https://github.com/developmentseed/obstore/pull/173

**Full Changelog**: https://github.com/developmentseed/obstore/compare/py-v0.3.0...py-v0.4.0

## [0.3.0] - 2025-01-16

### New Features :magic_wand:

- **Streaming uploads**. `obstore.put` now supports iterable input, and `obstore.put_async` now supports async iterable input. This means you can pass the output of `obstore.get_async` directly into `obstore.put_async`. by @kylebarron in https://github.com/developmentseed/obstore/pull/54
- **Allow passing config options directly** as keyword arguments. Previously, you had to pass all options as a `dict` into the `config` parameter. Now you can pass the elements directly to the store constructor. by @kylebarron in https://github.com/developmentseed/obstore/pull/144
- **Readable file-like objects**. Open a readable file-like object with `obstore.open` and `obstore.open_async`. by @kylebarron in https://github.com/developmentseed/obstore/pull/33
- **Fsspec integration** by @martindurant in https://github.com/developmentseed/obstore/pull/63
- Prefix store by @kylebarron in https://github.com/developmentseed/obstore/pull/117
- Python 3.13 wheels by @kylebarron in https://github.com/developmentseed/obstore/pull/95
- Support python timedelta objects as duration config values by @kylebarron in https://github.com/developmentseed/obstore/pull/146
- Add class constructors for store builders. Each store now has an `__init__` method, for easier construction. by @kylebarron in https://github.com/developmentseed/obstore/pull/141

### Breaking changes :wrench:

- `get_range`, `get_range_async`, `get_ranges`, and `get_ranges_async` now use **start/end** instead of **offset/length**. This is for consistency with the `range` option of `obstore.get`. by @kylebarron in https://github.com/developmentseed/obstore/pull/71

* Return `Bytes` from `GetResult.bytes()` by @kylebarron in https://github.com/developmentseed/obstore/pull/134

### Bug fixes :bug:

- boto3 region name can be None by @kylebarron in https://github.com/developmentseed/obstore/pull/59
- add missing py.typed file by @gruebel in https://github.com/developmentseed/obstore/pull/115

### Documentation :book:

- FastAPI/Starlette example by @kylebarron in https://github.com/developmentseed/obstore/pull/145
- Add conda installation doc to README by @kylebarron in https://github.com/developmentseed/obstore/pull/78
- Document suggested lifecycle rules for aborted multipart uploads by @kylebarron in https://github.com/developmentseed/obstore/pull/139
- Add type hint and documentation for requester pays by @kylebarron in https://github.com/developmentseed/obstore/pull/131
- Add note that S3Store can be constructed without boto3 by @kylebarron in https://github.com/developmentseed/obstore/pull/108
- HTTP Store usage example by @kylebarron in https://github.com/developmentseed/obstore/pull/142

### What's Changed

- Improved docs for from_url by @kylebarron in https://github.com/developmentseed/obstore/pull/138
- Implement read_all for async iterable by @kylebarron in https://github.com/developmentseed/obstore/pull/140

### New Contributors

- @willemarcel made their first contribution in https://github.com/developmentseed/obstore/pull/64
- @martindurant made their first contribution in https://github.com/developmentseed/obstore/pull/63
- @norlandrhagen made their first contribution in https://github.com/developmentseed/obstore/pull/107
- @gruebel made their first contribution in https://github.com/developmentseed/obstore/pull/115

**Full Changelog**: https://github.com/developmentseed/obstore/compare/py-v0.2.0...py-v0.3.0

## [0.2.0] - 2024-10-25

### What's Changed

- Streaming list results. `list` now returns an async or sync generator. by @kylebarron in https://github.com/developmentseed/obstore/pull/35
- Optionally return list result as arrow. The `return_arrow` keyword argument returns chunks from `list` as Arrow RecordBatches, which is faster than materializing Python dicts/lists. by @kylebarron in https://github.com/developmentseed/obstore/pull/38
- Return buffer protocol object from `get_range` and `get_ranges`. Enables zero-copy data exchange from Rust into Python. by @kylebarron in https://github.com/developmentseed/obstore/pull/39
- Add put options. Enables custom tags and attributes, as well as "put if not exists". by @kylebarron in https://github.com/developmentseed/obstore/pull/50
- Rename to obstore by @kylebarron in https://github.com/developmentseed/obstore/pull/45
- Add custom exceptions. by @kylebarron in https://github.com/developmentseed/obstore/pull/48

**Full Changelog**: https://github.com/developmentseed/obstore/compare/py-v0.1.0...py-v0.2.0

## [0.1.0] - 2024-10-21

- Initial release.
