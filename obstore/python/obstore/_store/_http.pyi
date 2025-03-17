import sys

from ._client import ClientConfig
from ._retry import RetryConfig

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

class HTTPStore:
    """Configure a connection to a generic HTTP server."""

    def __init__(
        self,
        url: str,
        *,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
        """Construct a new HTTPStore from a URL.

        Args:
            url: The base URL to use for the store.

        Keyword Args:
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            HTTPStore

        """

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
    ) -> Self:
        """Construct a new HTTPStore from a URL.

        This is an alias of [`HTTPStore.__init__`][obstore.store.HTTPStore.__init__].
        """

    def __eq__(self, value: object) -> bool: ...
    def __getnewargs_ex__(self): ...
    @property
    def url(self) -> str:
        """Get the base url of this store."""
    @property
    def client_options(self) -> ClientConfig | None:
        """Get the store's client configuration."""
    @property
    def retry_config(self) -> RetryConfig | None:
        """Get the store's retry configuration."""
