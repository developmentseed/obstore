import sys
from collections.abc import Awaitable, Callable
from typing import Union

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

SignerResult: TypeAlias = tuple[str, dict[str, str]]
"""The signed `(uri, headers)` returned by a [`Signer`][obstore.store.Signer]."""

Signer: TypeAlias = Callable[
    [str, str, dict[str, str]],
    Union[SignerResult, Awaitable[SignerResult]],
]
"""A callback that signs a single S3 request.

It is called with `(method, uri, headers)` immediately before each request is
dispatched, and must return the signed `(uri, headers)`. It may be synchronous or
asynchronous.
"""

class RemoteSignedS3Store:
    """An S3-compatible store that has each request signed by a remote service."""

    def __init__(self, url: str, signer: Signer) -> None:
        """Construct a new RemoteSignedS3Store.

        Args:
            url: Base URL including the bucket and optional object prefix.
            signer: Callback that signs each request.

        """
