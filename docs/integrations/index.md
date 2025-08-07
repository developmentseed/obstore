# External Integrations

Various integrations with external libraries exist:

- [`dagster`](https://dagster.io/): Refer to [`dagster-obstore`](https://github.com/dagster-io/community-integrations/tree/main/libraries/dagster-obstore).
- [`fsspec`](https://github.com/fsspec/filesystem_spec): Use the [`obstore.fsspec`][obstore.fsspec] module.
- [`zarr-python`](https://zarr.readthedocs.io/en/stable/): Use [`zarr.storage.ObjectStore`](https://zarr.readthedocs.io/en/stable/user-guide/storage.html#object-store), included as of Zarr version `3.0.7` and later. See also the [Obstore-Zarr example](../examples/zarr.md).

And obstore is used internally in more projects:

- [litData](https://github.com/Lightning-AI/litData)
- [VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr)

Know of an integration that doesn't exist here? [Edit this document](https://github.com/developmentseed/obstore/edit/main/docs/integrations.md).
