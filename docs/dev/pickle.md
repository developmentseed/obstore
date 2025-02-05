# Pickle Implementation

> Last edited 2025-02-04.

[Pickle support](https://github.com/developmentseed/obstore/issues/125) is important but hard to implement. It's important to support because it's commonly used from inside Dask and similar libraries to manage state across distributed workers.

## Background

There are two ways to implement pickle support.

1. Implementing [`__getstate__`](https://docs.python.org/3/library/pickle.html#object.__getstate__) and [`__setstate__`](https://docs.python.org/3/library/pickle.html#object.__setstate__). The return value of `__getstate__` can be pretty much anything I think, and that gets passed back into `__setstate__` to be unpacked and set as the internal state.
2. Implementing a constructor (tagged in Rust with `#[new]`) and [`__getnewargs_ex__`](https://docs.python.org/3/library/pickle.html#object.__getnewargs_ex__). `__getnewargs_ex__` must return a tuple of `(args: tuple, kwargs: dict)`, which can be passed to the `#[new]` constructor. This can be simpler when you already have a `#[new]` implemented and when the parameters into that `#[new]` function are easily serializable. However it might require (I wonder if there's a way to recursively pickle things?)

We can't extract the configuration out from a "finished" `object_store` instance like an [`AmazonS3`](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3.html). We could extract some configuration out of a builder instance like [`AmazonS3Builder`](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html), but then _every time we use that class_ we'd have to call `build()` on the Rust side, which would probably significantly hurt performance. Therefore, the only(?) possible way to implement pickle support inside obstore is to persist the store configuration separately inside the `#[pyclass]`.

## Implementation

- Store configuration information _a second time_ in the `#[pyclass]` of each store. (It's already implicitly stored by the underlying rust `ObjectStore` instance.)
- Handle prefixes automatically within each Python store class.
- Implement `__getnewargs_ex__`, returning the parameters passed to `#[new]`
- Implement `IntoPyObject` for each configuration object: the store-specific config, client options, and retry options

## Benefits

- It should be relatively straightforward to implement for any of the raw stores: `S3Store`, `GCSStore`, `AzureStore`, `LocalStore`, and `MemoryStore`.
- We're already validating the `PyAmazonS3Config`, `PyClientOptions`, and `PyRetryConfig`, so it isn't that much extra work just to store those on the Rust class. So we can serialize them to Python objects in `__getnewargs_ex__` and then have Python automatically pass them to `#[new]`.
- Using `__getnewargs_ex__` means we don't need to add serde support; we can use `IntoPyObject` and `FromPyObject` for all (de)serialization and only have a single code path.
- We can persist all of the store-specific config, the client options, and the retry config, so the pickled instances should be _exactly_ the same as the original instances.
- Supports any of the builder classmethods, e.g. `from_env`, `from_url`, etc,
- Most of the time, storing configuration information should be just a few strings. So ideally it'll increase memory usage only slightly, and won't affect runtime performance otherwise (assuming you reuse a store instead of creating a new one each time).
- Since we don't allow the store classes to be mutated after creation from Python, there's no risk of the two copies of the configuration getting out of sync.

## Drawbacks

- Because `url` has _deferred parsing_ in the `object_store` builders, we need to special-case `url` handling. Naturally, passing `url` to a store with `with_url` means that `object_store` doesn't actually parse the URL until the `build()` method, and at that point we can no longer access config information from the built `AmazonS3`. Without special-casing this URL handling, pickling would fail for instances created from `from_url`.

  Therefore, we handle this by vendoring the small amount of URL parsing from upstream. So we apply the URL parsing onto our config `HashMap`s and _then_ apply those to the builder. So our configs and those used by the raw stores stay in sync. See https://github.com/developmentseed/obstore/pull/209.
- Unclear how to support middleware, including `PrefixStore`, because those have to support an _arbitrary_ wrapped object. Is there a way to recursively pickle and unpickle the thing it's wrapping?
  - (**Implemented**) If we can't find a way to support pickling of arbitrary middleware, we could alternatively use a `PrefixStore` internally and automatically inside an `S3Store`, `GCSStore`, `AzureStore` (NOTE: we should maybe benchmark the overhead a `PrefixStore` causes, in case it's something we don't want to force on everyone? Well, if the `S3Store` stored an arbitrary `Arc<dyn ObjectStore>` then we could prefix when asked for and not prefix when not asked for, but maybe that would conflict with signing, if that requires a raw `object_store::AmazonS3` instance?). ~~Alternatively, we could create the `PrefixStore` on demand, since it [looks virtually free](https://github.com/apache/arrow-rs/blob/3bf29a2c7474e59722d885cd11fafd0dca13a28e/object_store/src/prefix.rs#L44-L49).~~
- We don't currently implement pickle support for `MemoryStore`, as we don't have a way to serialize the memory state across workers.
