# Changelog

## [0.3.0] -

### Breaking changes :wrench:

#### Store constructors

- In the `AzureStore` constructor, the `container` positional argument was renamed to `container_name` to match the `container_name` key in `AzureConfig`.

  This is a breaking change if you had been calling `AzureStore(container="my container name")`. This is not breaking if you had been using it as a positional argument `AzureStore("my container name")` or if you had already been using `AzureStore(container_name="my container name")`.

## [0.2.0] - 2025-03-14

- Bump to pyo3 0.24.

## [0.1.0] - 2025-03-14

- Initial release.
