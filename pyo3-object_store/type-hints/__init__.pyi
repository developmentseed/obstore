# TODO: move to reusable types package
from pathlib import Path

from ._aws import S3ConfigKey as S3ConfigKey
from ._aws import S3Store as S3Store
from ._azure import AzureConfigKey as AzureConfigKey
from ._azure import AzureStore as AzureStore
from ._client import ClientConfigKey as ClientConfigKey
from ._gcs import GCSConfigKey as GCSConfigKey
from ._gcs import GCSStore as GCSStore
from ._http import HTTPStore as HTTPStore
from ._retry import BackoffConfig as BackoffConfig
from ._retry import RetryConfig as RetryConfig

class LocalStore:
    """
    Local filesystem storage providing an ObjectStore interface to files on local disk.
    Can optionally be created with a directory prefix.

    ```py
    from pathlib import Path

    store = LocalStore()
    store = LocalStore(prefix="/path/to/directory")
    store = LocalStore(prefix=Path("."))
    ```
    """
    def __init__(self, prefix: str | Path | None = None) -> None: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_url(cls, url: str) -> LocalStore:
        """Construct a new LocalStore from a `file://` URL.

        **Examples:**

        Construct a new store pointing to the root of your filesystem:
        ```py
        url = "file:///"
        store = LocalStore.from_url(url)
        ```

        Construct a new store with a directory prefix:
        ```py
        url = "file:///Users/kyle/"
        store = LocalStore.from_url(url)
        ```
        """

class MemoryStore:
    """A fully in-memory implementation of ObjectStore.

    Create a new in-memory store:
    ```py
    store = MemoryStore()
    ```
    """
    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...

ObjectStore = AzureStore | GCSStore | HTTPStore | S3Store | LocalStore | MemoryStore
"""All supported ObjectStore implementations."""
