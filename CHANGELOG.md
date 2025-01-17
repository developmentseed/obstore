# Changelog

## [0.4.0] -

### Breaking changes :wrench:

- `get_range`, `get_range_async`, `get_ranges`, and `get_ranges_async` now require named parameters for `start`, `end`, and `length` to make the semantics of the range request fully explicit. by @kylebarron in https://github.com/developmentseed/obstore/pull/156

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

## What's Changed

- Improved docs for from_url by @kylebarron in https://github.com/developmentseed/obstore/pull/138
- Implement read_all for async iterable by @kylebarron in https://github.com/developmentseed/obstore/pull/140

## New Contributors

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
