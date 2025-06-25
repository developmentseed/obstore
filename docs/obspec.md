# Generic storage abstractions with Obspec

Obstore provides an implementation for accessing Amazon S3, Google Cloud Storage, and Azure Storage, but some libraries may want to support other backends, such as HTTP clients or more obscure things like SFTP or HDFS filesystems.

There's a bunch of useful behavior that could exist on top of Obstore: caching, metrics, globbing, bulk operations. While all of those operations are useful, we want to keep the core Obstore library as small as possible, tightly coupled with the underlying Rust `object_store` library.

[Obspec](https://developmentseed.org/obspec/) exists to provide the abstractions for generic programming against object store backends. Obspec is essentially a formalization and generalization of the Obstore API, so if you're already using Obstore, very few changes are needed to use Obspec instead.

Downstream libraries can program against the Obspec API to be fully generic around what underlying backend is used at runtime.

For further information, refer to the [Obspec documentation](https://developmentseed.org/obspec/latest/) and the [Obspec announcement blog post](https://developmentseed.org/obspec/latest/blog/2025/06/25/introducing-obspec-a-python-protocol-for-interfacing-with-object-storage/).
