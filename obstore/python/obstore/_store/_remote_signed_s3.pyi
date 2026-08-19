import sys
from collections.abc import Awaitable, Callable

from ._client import ClientConfig
from ._retry import RetryConfig

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

SignerResult: TypeAlias = tuple[str, dict[str, str]]
"""The signed `(uri, headers)` returned by a [`Signer`][obstore.store.Signer]."""

Signer: TypeAlias = Callable[
    [str, str, dict[str, str]],
    SignerResult | Awaitable[SignerResult],
]
"""A callback that signs a single S3 request.

It is called with `(method, uri, headers)` immediately before each request is
dispatched, and must return the signed `(uri, headers)`. It may be synchronous or
asynchronous.

The headers it returns **replace** the headers it was given, rather than being merged
into them, because a signature only covers the headers the signer chose to sign. A
signer must therefore echo back every header it was passed that the request still needs
(such as `range` or `content-length`) alongside the ones it adds.
"""

class RemoteSignedS3Store:
    """An S3-compatible store that has each request signed by a remote service."""

    def __init__(
        self,
        url: str,
        signer: Signer,
        *,
        virtual_hosted_style_request: bool = False,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
        """Construct a new RemoteSignedS3Store.

        Args:
            url: Base URL including the bucket and optional object prefix.
            signer: Callback that signs each request.

        Keyword Args:
            virtual_hosted_style_request: Whether the bucket is named by the URL's host
                rather than its first path segment. Defaults to `False`.
            client_options: HTTP client options, such as timeouts and `allow_http`.
            retry_config: How to retry failed requests. Every retry is signed again.

        """
    @classmethod
    def from_s3_url(
        cls,
        url: str,
        signer: Signer,
        *,
        endpoint: str,
        virtual_hosted_style_request: bool = False,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
    ) -> Self:
        """Construct a store from an `s3://` location and the endpoint that serves it.

        Catalogs hand out locations as `s3://bucket/key` with the S3 endpoint configured
        separately, so this saves you assembling the HTTPS URL yourself.

        ```py
        store = RemoteSignedS3Store.from_s3_url(
            "s3://warehouse/zarr/my-array",
            signer,
            endpoint="https://s3.eu-west-1.amazonaws.com",
        )
        ```

        Args:
            url: An `s3://bucket/prefix` or `s3a://bucket/prefix` location.
            signer: Callback that signs each request.

        Keyword Args:
            endpoint: The `http://` or `https://` origin of the S3 endpoint serving the
                bucket, such as `https://s3.eu-west-1.amazonaws.com`. It must not include
                a path, since that would make the split between endpoint, bucket and key
                prefix ambiguous; build the URL yourself in that case.
            virtual_hosted_style_request: Set to `True` to address the bucket as a
                subdomain of `endpoint`'s host rather than as its first path segment.
                Defaults to `False`.
            client_options: HTTP client options, such as timeouts and `allow_http`.
            retry_config: How to retry failed requests. Every retry is signed again.

        """
    def __eq__(self, other: object) -> bool: ...
    @property
    def url(self) -> str:
        """The base URL this store was constructed with."""
    @property
    def bucket(self) -> str:
        """The bucket name, taken from `url`'s first path segment or its host."""
    @property
    def prefix(self) -> str | None:
        """The key prefix implied by `url`, or `None` if it names only the bucket."""
    @property
    def signer(self) -> Signer:
        """The signer callback passed to the constructor."""
    @property
    def virtual_hosted_style_request(self) -> bool:
        """Whether the bucket is named by the URL's host."""
    @property
    def client_options(self) -> ClientConfig | None:
        """Get the store's client configuration."""
    @property
    def retry_config(self) -> RetryConfig | None:
        """Get the store's retry configuration."""
