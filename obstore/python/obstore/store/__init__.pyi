# TODO: move to reusable types package
from pathlib import Path
from typing import Any, Unpack, overload

from ._aws import S3Config as S3Config
from ._aws import S3Store as S3Store
from ._azure import AzureConfig as AzureConfig
from ._azure import AzureStore as AzureStore
from ._client import ClientConfig as ClientConfig
from ._gcs import GCSConfig as GCSConfig
from ._gcs import GCSStore as GCSStore
from ._http import HTTPStore as HTTPStore
from ._prefix import PrefixStore as PrefixStore
from ._retry import BackoffConfig as BackoffConfig
from ._retry import RetryConfig as RetryConfig

@overload
def new_store(
    url: str, *, config: S3Config | None = None, **kwargs: Unpack[S3Config]
) -> PrefixStore: ...
@overload
def new_store(
    url: str, *, config: GCSConfig | None = None, **kwargs: Unpack[GCSConfig]
) -> PrefixStore: ...
@overload
def new_store(
    url: str, *, config: AzureConfig | None = None, **kwargs: Unpack[AzureConfig]
) -> PrefixStore: ...
def new_store(
    url: str, *, config: S3Config | GCSConfig | AzureConfig | None = None, **kwargs: Any
) -> PrefixStore:
    """Easy construction of store by URL

    Recognizes various URL formats, identifying the relevant [`ObjectStore`]

    Supported formats:

    - `file:///path/to/my/file` -> [`LocalFileSystem`]
    - `memory:///` -> [`InMemory`]
    - `s3://bucket/path` -> [`AmazonS3`](crate::aws::AmazonS3) (also supports `s3a`)
    - `gs://bucket/path` -> [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
    - `az://account/container/path` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure) (also supports `adl`, `azure`, `abfs`, `abfss`)
    - `http://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)
    - `https://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)

    There are also special cases for AWS and Azure for `https://{host?}/path` paths:

    - `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, `blob.fabric.microsoft.com` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
    - `amazonaws.com` -> [`AmazonS3`](crate::aws::AmazonS3)
    - `r2.cloudflarestorage.com` -> [`AmazonS3`](crate::aws::AmazonS3)
    """

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
    def __init__(
        self,
        prefix: str | Path | None = None,
        *,
        automatic_cleanup: bool = False,
        mkdir: bool = False,
    ) -> None:
        """Create a new LocalStore.

        Args:
            prefix: Use the specified prefix applied to all paths. Defaults to `None`.

        Keyword Args:
            automatic_cleanup: if `True`, enables automatic cleanup of empty directories when deleting files. Defaults to False.
            mkdir: if `True` and `prefix` is not `None`, the directory at `prefix` will attempt to be created. Note that this root directory will not be cleaned up, even if `automatic_cleanup` is `True`.
        """
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

ObjectStore = (
    AzureStore | GCSStore | HTTPStore | S3Store | LocalStore | MemoryStore | PrefixStore
)
"""All supported ObjectStore implementations."""
