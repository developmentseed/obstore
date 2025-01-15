from ._client import ClientConfig
from ._retry import RetryConfig

class HTTPStore:
    """Configure a connection to a generic HTTP server

    **Example**

    Accessing the number of stars for a repo:

    ```py
    import json

    import obstore as obs
    from obstore.store import HTTPStore

    store = HTTPStore.from_url("https://api.github.com")
    resp = obs.get(store, "repos/developmentseed/obstore")
    data = json.loads(resp.bytes())
    print(data["stargazers_count"])
    ```
    """

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
    ) -> HTTPStore:
        """Construct a new HTTPStore from a URL

        !!! note
            Note that in contrast to the other stores, `from_url` **will** use the full
            URL provided here as a prefix for further operations.

        Args:
            url: The base URL to use for the store.

        Keyword Args:
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            HTTPStore
        """

    def __repr__(self) -> str: ...
