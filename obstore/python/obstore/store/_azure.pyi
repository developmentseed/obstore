from collections.abc import Coroutine
from datetime import datetime
from typing import Any, Protocol, TypeAlias, TypedDict, Unpack

from ._client import ClientConfig
from ._retry import RetryConfig

# TODO: add these parameters to config
# azure_storage_authority_host
# azure_fabric_token_service_url
# azure_fabric_workload_host
# "azure_fabric_session_token",
# "azure_fabric_cluster_identifier",
class AzureConfig(TypedDict, total=False):
    """Configuration parameters for AzureStore.

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import AzureConfig
        ```
    """

    storage_account_name: str
    """The name of the azure storage account"""
    storage_account_key: str
    """Master key for accessing storage account"""
    storage_client_id: str
    """Service principal client id for authorizing requests"""
    storage_client_secret: str
    """Service principal client secret for authorizing requests"""
    storage_tenant_id: str
    """Tenant id used in oauth flows"""
    # azure_storage_authority_host
    storage_sas_key: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    storage_token: str
    """Bearer token"""
    storage_use_emulator: bool
    """Use object store with azurite storage emulator"""
    use_fabric_endpoint: bool
    """Use object store with url scheme account.dfs.fabric.microsoft.com"""
    storage_endpoint: str
    """Override the endpoint used to communicate with blob storage"""
    msi_endpoint: str
    """Endpoint to request a imds managed identity token"""
    object_id: str
    """Object id for use with managed identity authentication"""
    msi_resource_id: str
    """Msi resource id for use with managed identity authentication"""
    federated_token_file: str
    """File containing token for Azure AD workload identity federation"""
    use_azure_cli: bool
    """Use azure cli for acquiring access token"""
    skip_signature: bool
    """Skip signing requests"""
    container_name: str
    """Container name"""
    disable_tagging: bool
    """Disables tagging objects"""
    # fabric_token_service_url
    # fabric_workload_host
    # fabric_session_token
    # fabric_cluster_identifier

class AzureAccessKey(TypedDict):
    """A shared Azure Storage Account Key.

    <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import AzureAccessKey
        ```
    """

    access_key: str
    """Access key value."""

    expires_at: datetime | None
    """Expiry datetime of credential. The datetime should have time zone set.

    If None, the credential will never expire.
    """

class AzureSASToken(TypedDict):
    """A shared access signature.

    <https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature>

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import AzureSASToken
        ```
    """

    sas_token: str | list[tuple[str, str]]
    """SAS token."""

    expires_at: datetime | None
    """Expiry datetime of credential. The datetime should have time zone set.

    If None, the credential will never expire.
    """

class AzureBearerToken(TypedDict):
    """An authorization token.

    <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-azure-active-directory>

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import AzureBearerToken
        ```
    """

    token: str
    """Bearer token."""

    expires_at: datetime | None
    """Expiry datetime of credential. The datetime should have time zone set.

    If None, the credential will never expire.
    """

AzureCredential: TypeAlias = AzureAccessKey | AzureSASToken | AzureBearerToken
"""A type alias for supported azure credentials to be returned from `AzureCredentialProvider`.

!!! warning "Not importable at runtime"

    To use this type hint in your code, import it within a `TYPE_CHECKING` block:

    ```py
    from __future__ import annotations
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from obstore.store import AzureCredential
    ```
"""

class AzureCredentialProvider(Protocol):
    """A type hint for a synchronous or asynchronous callback to provide custom Azure credentials.

    This should be passed into the `credential_provider` parameter of `AzureStore`.

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import AzureCredentialProvider
        ```
    """

    @staticmethod
    def __call__() -> AzureCredential | Coroutine[Any, Any, AzureCredential]:
        """Return an `AzureCredential`."""

class AzureStore:
    """Interface to a Microsoft Azure Blob Storage container.

    All constructors will check for environment variables. All environment variables
    starting with `AZURE_` will be evaluated. Names must match keys from
    [`AzureConfig`][obstore.store.AzureConfig]. Only upper-case environment variables
    are accepted.

    Some examples of variables extracted from environment:

    - `AZURE_STORAGE_ACCOUNT_NAME`: storage account name
    - `AZURE_STORAGE_ACCOUNT_KEY`: storage account master key
    - `AZURE_STORAGE_ACCESS_KEY`: alias for `AZURE_STORAGE_ACCOUNT_KEY`
    - `AZURE_STORAGE_CLIENT_ID` -> client id for service principal authorization
    - `AZURE_STORAGE_CLIENT_SECRET` -> client secret for service principal authorization
    - `AZURE_STORAGE_TENANT_ID` -> tenant id used in oauth flows
    """

    def __init__(
        self,
        container: str | None = None,
        *,
        prefix: str | None = None,
        config: AzureConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: AzureCredentialProvider | None = None,
        **kwargs: Unpack[AzureConfig],
    ) -> None:
        """Construct a new AzureStore.

        Args:
            container: the name of the container.

        Keyword Args:
            prefix: A prefix within the bucket to use for all operations.
            config: Azure Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.
            credential_provider: A callback to provide custom Azure credentials.
            kwargs: Azure configuration values. Supports the same values as `config`, but as named keyword args.

        Returns:
            AzureStore

        """

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        prefix: str | None = None,
        config: AzureConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: AzureCredentialProvider | None = None,
        **kwargs: Unpack[AzureConfig],
    ) -> AzureStore:
        """Construct a new AzureStore with values populated from a well-known storage URL.

        The supported url schemes are:

        - `abfs[s]://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
        - `abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>`
        - `abfs[s]://<file_system>@<account_name>.dfs.fabric.microsoft.com/<path>`
        - `az://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
        - `adl://<container>/<path>` (according to [fsspec](https://github.com/fsspec/adlfs))
        - `azure://<container>/<path>` (custom)
        - `https://<account>.dfs.core.windows.net`
        - `https://<account>.blob.core.windows.net`
        - `https://<account>.blob.core.windows.net/<container>`
        - `https://<account>.dfs.fabric.microsoft.com`
        - `https://<account>.dfs.fabric.microsoft.com/<container>`
        - `https://<account>.blob.fabric.microsoft.com`
        - `https://<account>.blob.fabric.microsoft.com/<container>`

        Args:
            url: well-known storage URL.

        Keyword Args:
            prefix: A prefix within the bucket to use for all operations.
            config: Azure Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.
            credential_provider: A callback to provide custom Azure credentials.
            kwargs: Azure configuration values. Supports the same values as `config`, but as named keyword args.

        Returns:
            AzureStore

        """

    def __getnewargs_ex__(self): ...
    @property
    def prefix(self) -> str | None:
        """Get the prefix applied to all operations in this store, if any."""
    @property
    def config(self) -> AzureConfig:
        """Get the underlying Azure config parameters."""
    @property
    def client_options(self) -> ClientConfig | None:
        """Get the store's client configuration."""
    @property
    def retry_config(self) -> RetryConfig | None:
        """Get the store's retry configuration."""
