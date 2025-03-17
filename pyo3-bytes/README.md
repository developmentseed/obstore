# pyo3-bytes

Integration between [`bytes`](https://docs.rs/bytes) and [`pyo3`](https://github.com/PyO3/pyo3).

This provides [`PyBytes`], a wrapper around [`Bytes`][::bytes::Bytes] that supports the [Python buffer protocol](https://docs.python.org/3/c-api/buffer.html).

This uses the new [`Bytes::from_owner` API](https://docs.rs/bytes/latest/bytes/struct.Bytes.html#method.from_owner) introduced in `bytes` 1.9.

Since this integration uses the Python buffer protocol, any library that uses `pyo3-bytes` must set the feature flags for the `pyo3` dependency correctly. `pyo3` must either _not_ have an [`abi3` feature flag](https://pyo3.rs/v0.23.4/features.html#abi3) (in which case maturin will generate wheels per Python version), or have `abi3-py311` (which supports only Python 3.11+), since the buffer protocol became part of the Python stable ABI [as of Python 3.11](https://docs.python.org/3/c-api/buffer.html#c.Py_buffer).

## Importing buffers to Rust

Just use `PyBytes` as a type in your functions or methods exposed to Python.

```rs
use pyo3_bytes::PyBytes;
use bytes::Bytes;

#[pyfunction]
pub fn use_bytes(buffer: PyBytes) {
    let buffer: Bytes = buffer.into_inner();
}
```

## Exporting buffers to Python

Return the `PyBytes` class from your function.

```rs
use pyo3_bytes::PyBytes;
use bytes::Bytes;

#[pyfunction]
pub fn return_bytes() -> PyBytes {
    let buffer = Bytes::from_static(b"hello");
    PyBytes::new(buffer)
}
```

## Safety

Unfortunately, this interface cannot be 100% safe, as the Python buffer protocol does not enforce buffer immutability.

The Python user must take care to not mutate the buffers that have been passed
to Rust.

For more reading:

- <https://alexgaynor.net/2022/oct/23/buffers-on-the-edge/>
- <https://github.com/PyO3/pyo3/issues/2824>
- <https://github.com/PyO3/pyo3/issues/4736>

## Python type hints

On the Python side, the exported `Bytes` class implements many of the same
methods (with the same signature) as the Python `bytes` object.

The Python type hints are available in the Github repo in the file `bytes.pyi`.
I don't know the best way to distribute this to downstream projects. If you have
an idea, create an issue to discuss.

## Version compatibility

| pyo3-bytes | pyo3 |
| ---------- | ---- |
| 0.1.x      | 0.23 |
| 0.2.x      | 0.24 |
