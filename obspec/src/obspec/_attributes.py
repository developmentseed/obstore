from __future__ import annotations

from typing import Literal, TypeAlias

Attribute: TypeAlias = (
    Literal[
        "Content-Disposition",
        "Content-Encoding",
        "Content-Language",
        "Content-Type",
        "Cache-Control",
    ]
    | str
)
"""Additional object attribute types.

- `"Content-Disposition"`: Specifies how the object should be handled by a browser.

    See [Content-Disposition](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition).

- `"Content-Encoding"`: Specifies the encodings applied to the object.

    See [Content-Encoding](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding).

- `"Content-Language"`: Specifies the language of the object.

    See [Content-Language](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Language).

- `"Content-Type"`: Specifies the MIME type of the object.

    This takes precedence over any client configuration.

    See [Content-Type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type).

- `"Cache-Control"`: Overrides cache control policy of the object.

    See [Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control).

Any other string key specifies a user-defined metadata field for the object.
"""

Attributes: TypeAlias = dict[Attribute, str]
"""Additional attributes of an object

Attributes can be specified in [`Put`][obspec.Put]/[`PutAsync`][obspec.PutAsync] and
retrieved from [`Get`][obspec.Get]/[`GetAsync`][obspec.GetAsync].

Unlike ObjectMeta, Attributes are not returned by listing APIs
"""
