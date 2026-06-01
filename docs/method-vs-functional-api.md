# Method vs Functional API

Obstore presents two **nearly-identical APIs** for user convenience, a [method-based API](#method-based-api) and a [functional API](#functional-api).

## Method-based API

The method-based API allows for using methods on store classes.

```py
from obstore.store import S3Store

store = S3Store("bucket", ...)
buffer = store.get_range("path", start=0, end=16384)
```

This is slightly cleaner to use and less verbose than the functional API.

### Obspec compatibility

We also prefer the method-based API because it integrates with [Obspec], generic protocols that define the Object Storage interface. This makes it possible to write code once that works with Obstore and other arbitrary backends.

See the [sibling user guide page](obspec.md) and [Obspec documentation][Obspec] for more information.

[Obspec]: https://developmentseed.org/obspec/

## Functional API

The functional API provides top-level functions to interact with obstore operations.

```py
import obstore as obs
from obstore.store import S3Store

store = S3Store("bucket", ...)
buffer = obs.get_range(store, "path", start=0, end=16384)
```

## Differences

The main difference between the two APIs is verbosity: the functional API is more verbose because it requires passing `store` as an argument into each operation.

#### Signed URLs

[Creating signed URLs](api/sign.md) is only supported via the functional API because not all object stores support signing.

#### File-like Objects

[Creating file-like objects](api/file.md) is only supported via the functional API because we want to slightly nudge users away from that behavior, if it's possible to achieve their goals with native methods, which should provide better performance.
