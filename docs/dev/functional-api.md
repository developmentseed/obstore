# Functional API Design Choice

> Last edited 2025-02-04.
>
> See further discussion in [this issue](https://github.com/developmentseed/obstore/issues/160).

Obstore intentionally presents its main API as top-level functions. E.g. users must use the top level `obstore.put` function:

```py
import obstore as obs
from obstore.store import AzureStore

store = AzureStore()
obs.put(store, ....)
```

instead of a method on the store itself:

```py
import obstore as obs
from obstore.store import AzureStore

store = AzureStore()
store.put(....)
```

This page documents the design decisions for this API.

## Store-specific vs generic API

This presents a nice separation of concerns, in my opinion, between store-specific properties and a generic API that works for _every_ `ObjectStore`.

Python store classes such as `S3Store` have a few properties to access the _store-specific_ configuration, e.g. `S3Store.config` accesses the S3 credentials. Anything that's a property/method of the store class is specific to that type of store. Whereas any top-level method should work on _any_ store equally well.

## Simpler Rust code

On the Rust side, each Python class is a separate `struct`. A pyo3 `#[pyclass]` can't implement a trait, so the only way to implement the same methods on multiple Rust structs without copy-pasting is by having a macro. That isn't out of the question, however it does hamper extensibility, and having one and only one way to call commands is simpler to maintain.

## Simpler Middlewares

> The `PrefixStore` concept has since been taken out, in favor of natively handling store prefixes, but this argument still holds for other potential middlewares in the future.

In https://github.com/developmentseed/obstore/pull/117 we added a binding for `PrefixStore`.  Because we use object store classes functionally, we only needed 20 lines of Rust code:
https://github.com/developmentseed/obstore/blob/b40d59b4e060ba4fd3dc69468b3ba7da1149758e/pyo3-object_store/src/prefix.rs#L10-L25

If we exposed methods on an `S3Store`, then those methods would be lost whenever you apply a middleware around it, such as `PrefixStore(S3Store(...))`. So we'd have to ensure those same methods are also installed onto every middleware or other wrapper.

## External FFI for ObjectStore

There was recently [discussion on Discord](https://discord.com/channels/885562378132000778/885562378132000781/1328392836353360007) about the merits of having a stable FFI for `ObjectStore`. If this comes to fruition in the future, then by having a functional API we could seamlessly use _third party_ ObjectStore implementations or middlewares, with no Python overhead.

I use a similar functional API in other Python bindings, especially in cases with zero-copy FFI, such as https://kylebarron.dev/geo-index/latest/api/rtree/#geoindex_rs.rtree.search (where the spatial index is passed in as the first argument instead) and https://kylebarron.dev/arro3/latest/api/compute/#arro3.compute.cast where the `cast` is not a method on the Arrow Array.

## Smaller core for third-party Rust bindings

This repo has twin goals:

1. Provide bindings to `object_store` for _Python users_ who want a _Python API_.
2. Make it easier for other Rust developers who are making Python bindings, who are using `object_store` on the Rust side already, and who want to expose `ObjectStore` bindings to Python in their own projects.

The first goal is served by the `obstore` Python package and the second is served by the `pyo3-object_store` Rust crate. The latter provides builders for `S3Store`, `AzureStore`, `GCSStore`, which means that those third party Rust-Python bindings can have code as simple as:

```rs
#[pyfunction]
fn use_object_store(store: PyObjectStore) {
    let store: Arc<dyn ObjectStore> = store.into_inner();
}
```

Those third party bindings don't need the Python bindings to perform arbitrary `get`, `list`, `put` from Python. Instead, they use this to access a raw `Arc<dyn ObjectStore>` from the Rust side.

You'll notice that `S3Store`, `GCSStore`, and `AzureStore` **aren't** in the `obstore` library; they're in `pyo3-object_store`. We can't add methods to a pyclass from an external crate, so we couldn't leave those builders in `pyo3_object_store` while having the Python-facing operations live in `obstore`. Instead we'd have to put the entire content of the Python bindings in the `pyo3-object_store` crate. Then this would expose whatever class methods from the `obstore` Python API onto any external Rust-Python library that uses `pyo3-object_store`. I don't want to leak this abstraction nor make that public to other Rust consumers.
