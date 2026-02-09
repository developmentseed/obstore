# Changelog

## [0.9.0] - 2026-02-09

- Bump to pyo3 0.28.

## [0.8.0] - 2026-01-23

- Bump to object_store 0.13.

## [0.7.0] - 2025-10-23

- Bump to pyo3 0.27.

## [0.6.0] - 2025-09-02

### Breaking changes :wrench:

- Don't percent-encode paths. The implementation of `FromPyObject` for `PyPath` now uses `Path::parse` instead of `Path::from` under the hood. #524
- Bump to pyo3 0.26.

### Other

- Configurable warning on PyExternalObjectStore creation #550

## [0.5.0] - 2025-05-19

- Bump to pyo3 0.25.

## [0.4.0] - 2025-03-24

Compatibility release to use `pyo3-object_store` with `object_store` 0.11 and `pyo3` 0.24.

## [0.3.0] - 2025-03-24

Compatibility release to use `pyo3-object_store` with `object_store` 0.11 and `pyo3` 0.23.

### Breaking changes :wrench:

#### Store constructors

- In the `AzureStore` constructor, the `container` positional argument was renamed to `container_name` to match the `container_name` key in `AzureConfig`.

  This is a breaking change if you had been calling `AzureStore(container="my container name")`. This is not breaking if you had been using it as a positional argument `AzureStore("my container name")` or if you had already been using `AzureStore(container_name="my container name")`.

## [0.2.0] - 2025-03-14

- Bump to pyo3 0.24.

## [0.1.0] - 2025-03-14

- Initial release.
