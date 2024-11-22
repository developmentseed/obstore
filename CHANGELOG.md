# Changelog

## [0.3.0] -

### Breaking changes

- `get_range`, `get_range_async`, `get_ranges`, and `get_ranges_async` now use **start/end** instead of **offset/length**. This is for consistency with the `range` option of `obstore.get`.

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
