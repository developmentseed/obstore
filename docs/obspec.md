# Generic storage abstractions with Obspec

## Motivating example

## Difference between Obstore and Obspec

What obstore is:

- sdf

What obspec does:

- Allows external implementations to match

How they differ:

- Obstore is a concrete implementation that happens to be backed by the Rust `object_store` library.
- Obspec is a pure-python set of protocols (duck types) that any Python library,
- Versioned separately

There's a bunch of useful behavior that could exist on top of obstore: caching, globbing, bulk downloads, uploads and deletes. But all of those operations are generally useful.

Further useful behavior on top of obstore will come in

Motivating example:

let's say you want to

For further information, refer to the obspec documentation and the obspec introductory blog post.
