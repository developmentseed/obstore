# pyo3-object_store

Integration between [`object_store`](https://docs.rs/object_store) and [`pyo3`](https://github.com/PyO3/pyo3).

This provides Python builder classes so that Python users can easily create `Arc<dyn ObjectStore>` instances, which can then be used in pure-Rust code.

## Usage

1. Register the builders.

    ```rs
    #[pymodule]
    fn python_module(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
        pyo3_object_store::register_store_module(py, m, "python_module")?;
        pyo3_object_store::register_exceptions_module(py, m, "python_module")?;
    }
    ```

    This exports the underlying Python classes from your own Rust-Python library.

2. Accept `PyObjectStore` as a parameter in your function exported to Python. Its `into_inner` method gives you an `Arc<dyn ObjectStore>`.

    ```rs
    #[pyfunction]
    pub fn use_object_store(store: PyObjectStore) {
        let store: Arc<dyn ObjectStore> = store.into_inner();
    }
    ```

## Example

The `obstore` Python library gives a full real-world example of using `pyo3-object_store`. It

## ABI stability

Note about not being able to use these across Python packages. It has to be used with the exported classes from your own library.

## Type hints
