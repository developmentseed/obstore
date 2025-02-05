# Contributor Documentation

## Prerequisites

Install [uv](https://docs.astral.sh/uv/) and [Rust](https://www.rust-lang.org/tools/install).

## Layout

- `pyo3-object_store/`: Logic for constructing `object_store` instances lives here, so that it can potentially be shared with other Rust-Python libraries in the future.
- `obstore/`: The primary Python-facing bindings of the `obstore` library. This re-exports the classes defined in `pyo3-object_store`. It also adds the top-level functions that form the `obstore` API.
- `pyo3-bytes`: A wrapper of [`bytes::Bytes`](https://docs.rs/bytes/latest/bytes/struct.Bytes.html) that is used inside `obstore` for zero-copy buffer exchange between Rust and Python but also is intended to be reusable for other Rust-Python libraries.

## Developing obstore

From the top-level directory, run

```
uv run maturin develop -m obstore/Cargo.toml
```

this will compile `obstore` and add it to the uv-managed Python environment.

If you wish to do any benchmarking, run

```
uv run maturin develop -m obstore/Cargo.toml --release
```

to compile `obstore` with release optimizations turned on.

### Tests

All obstore tests should go into the top-level `tests` directory.

## Publishing

Push a new tag to the main branch of the format `py-v*`. A new version will be published to PyPI automatically.

## Documentation website

The documentation website is generated with `mkdocs` and [`mkdocs-material`](https://squidfunk.github.io/mkdocs-material). You can serve the docs website locally with

```
uv run mkdocs serve
```

Publishing documentation happens automatically via CI when a new tag is published of the format `py-v*`. It can also be triggered manually through the Github Actions dashboard on [this page](https://github.com/developmentseed/obstore/actions/workflows/docs.yml). Note that publishing docs manually is **not advised if there have been new code additions since the last release** as the new functionality will be associated in the documentation with the tag of the _previous_ release. In this case, prefer publishing a new patch or minor release, which will publish both a new Python package and the new documentation for it.
