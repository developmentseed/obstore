import sys
from typing import Literal

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

Attribute: TypeAlias = (
    Literal[
        "Content-Disposition",
        "Content-Encoding",
        "Content-Language",
        "Content-Type",
        "Cache-Control",
        "Storage-Class",
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

- `"Storage-Class"`: Specifies the storage class of the object.

    See [AWS](https://aws.amazon.com/s3/storage-classes/),
    [GCP](https://cloud.google.com/storage/docs/storage-classes), and
    [Azure](https://learn.microsoft.com/en-us/rest/api/storageservices/set-blob-tier)
    documentation. `Storage-Class` is used as the name for this attribute because two of
    the three storage providers use that name

Any other string key specifies a user-defined metadata field for the object.

!!! warning "Not importable at runtime"

    To use this type hint in your code, import it within a `TYPE_CHECKING` block:

    ```py
    from __future__ import annotations
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from obstore import Attribute
    ```
"""

Attributes: TypeAlias = dict[Attribute, str]
"""Additional attributes of an object

Attributes can be specified in [`put`][obstore.put]/[`put_async`][obstore.put_async] and
retrieved from [`get`][obstore.get]/[`get_async`][obstore.get_async].

Unlike ObjectMeta, Attributes are not returned by listing APIs

!!! warning "Not importable at runtime"

    To use this type hint in your code, import it within a `TYPE_CHECKING` block:

    ```py
    from __future__ import annotations
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from obstore import Attributes
    ```
"""
