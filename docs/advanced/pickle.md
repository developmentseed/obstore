# Pickle Support

Obstore supports [pickle](https://docs.python.org/3/library/pickle.html), which is commonly used from inside [Dask](https://www.dask.org/) and similar libraries to manage state across distributed workers.

## Not for persistence

The format used to pickle stores may change across versions. Pickle support is intended for execution frameworks like [Dask](https://www.dask.org/) that need to share state across workers that are using the same environments, including the same version of Python and obstore.

## Middlewares

Obstore expects to support some sort of middleware in the future, such as for recording request metrics. It's unlikely that middlewares will support pickle.

## MemoryStore not implemented

Pickling isn't supported for [`MemoryStore`][obstore.store.MemoryStore] because we don't have a way to access the raw state of the store.

## Custom authentication

As of obstore 0.5.0, [custom authentication](../authentication.md#custom-authentication) is supported.

Pickling works with a custom authentication provider so long as that Python callback can itself be pickled.

So, for example, the [boto3 provider][obstore.auth.boto3.Boto3CredentialProvider] cannot be pickled, because a [`boto3.session.Session`][] cannot be pickled, but a simple function can be.
