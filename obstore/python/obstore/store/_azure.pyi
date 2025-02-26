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
    """Configuration parameters returned from [AzureStore.config][obstore.store.AzureStore.config].

    Note that this is a strict subset of the keys allowed for _input_ into the store,
    see [AzureConfigInput][obstore.store.AzureConfigInput].
    """

    azure_storage_account_name: str
    """The name of the azure storage account"""
    azure_storage_account_key: str
    """Master key for accessing storage account"""
    azure_storage_client_id: str
    """Service principal client id for authorizing requests"""
    azure_storage_client_secret: str
    """Service principal client secret for authorizing requests"""
    azure_storage_tenant_id: str
    """Tenant id used in oauth flows"""
    azure_storage_sas_key: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    azure_storage_token: str
    """Bearer token"""
    azure_storage_use_emulator: bool
    """Use object store with azurite storage emulator"""
    azure_use_fabric_endpoint: bool
    """Use object store with url scheme account.dfs.fabric.microsoft.com"""
    azure_storage_endpoint: str
    """Override the endpoint used to communicate with blob storage"""
    azure_msi_endpoint: str
    """Endpoint to request a imds managed identity token"""
    azure_object_id: str
    """Object id for use with managed identity authentication"""
    azure_msi_resource_id: str
    """Msi resource id for use with managed identity authentication"""
    azure_federated_token_file: str
    """File containing token for Azure AD workload identity federation"""
    azure_use_azure_cli: bool
    """Use azure cli for acquiring access token"""
    azure_skip_signature: bool
    """Skip signing requests"""
    azure_container_name: str
    """Container name"""
    azure_disable_tagging: bool
    """Disables tagging objects"""

class AzureConfigInput(TypedDict, total=False):
    """Configuration parameters for AzureStore.

    There are duplicates of many parameters, and parameters can be either upper or lower
    case. Not all parameters are required.
    """

    access_key: str
    """Master key for accessing storage account"""
    account_key: str
    """Master key for accessing storage account"""
    account_name: str
    """The name of the azure storage account"""
    authority_id: str
    """Tenant id used in oauth flows"""
    azure_authority_id: str
    """Tenant id used in oauth flows"""
    azure_client_id: str
    """Service principal client id for authorizing requests"""
    azure_client_secret: str
    """Service principal client secret for authorizing requests"""
    azure_container_name: str
    """Container name"""
    azure_disable_tagging: bool
    """Disables tagging objects"""
    azure_endpoint: str
    """Override the endpoint used to communicate with blob storage"""
    azure_federated_token_file: str
    """File containing token for Azure AD workload identity federation"""
    azure_identity_endpoint: str
    """Endpoint to request a imds managed identity token"""
    azure_msi_endpoint: str
    """Endpoint to request a imds managed identity token"""
    azure_msi_resource_id: str
    """Msi resource id for use with managed identity authentication"""
    azure_object_id: str
    """Object id for use with managed identity authentication"""
    azure_skip_signature: bool
    """Skip signing requests"""
    azure_storage_access_key: str
    """Master key for accessing storage account"""
    azure_storage_account_key: str
    """Master key for accessing storage account"""
    azure_storage_account_name: str
    """The name of the azure storage account"""
    azure_storage_authority_id: str
    """Tenant id used in oauth flows"""
    azure_storage_client_id: str
    """Service principal client id for authorizing requests"""
    azure_storage_client_secret: str
    """Service principal client secret for authorizing requests"""
    azure_storage_endpoint: str
    """Override the endpoint used to communicate with blob storage"""
    azure_storage_master_key: str
    """Master key for accessing storage account"""
    azure_storage_sas_key: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    azure_storage_sas_token: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    azure_storage_tenant_id: str
    """Tenant id used in oauth flows"""
    azure_storage_token: str
    """Bearer token"""
    azure_storage_use_emulator: bool
    """Use object store with azurite storage emulator"""
    azure_tenant_id: str
    """Tenant id used in oauth flows"""
    azure_use_azure_cli: bool
    """Use azure cli for acquiring access token"""
    azure_use_fabric_endpoint: bool
    """Use object store with url scheme account.dfs.fabric.microsoft.com"""
    bearer_token: str
    """Bearer token"""
    client_id: str
    """Service principal client id for authorizing requests"""
    client_secret: str
    """Service principal client secret for authorizing requests"""
    container_name: str
    """Container name"""
    disable_tagging: bool
    """Disables tagging objects"""
    endpoint: str
    """Override the endpoint used to communicate with blob storage"""
    federated_token_file: str
    """File containing token for Azure AD workload identity federation"""
    identity_endpoint: str
    """Endpoint to request a imds managed identity token"""
    master_key: str
    """Master key for accessing storage account"""
    msi_endpoint: str
    """Endpoint to request a imds managed identity token"""
    msi_resource_id: str
    """Msi resource id for use with managed identity authentication"""
    object_id: str
    """Object id for use with managed identity authentication"""
    sas_key: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    sas_token: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    skip_signature: bool
    """Skip signing requests"""
    tenant_id: str
    """Tenant id used in oauth flows"""
    token: str
    """Bearer token"""
    use_azure_cli: bool
    """Use azure cli for acquiring access token"""
    use_emulator: bool
    """Use object store with azurite storage emulator"""
    use_fabric_endpoint: bool
    """Use object store with url scheme account.dfs.fabric.microsoft.com"""
    ACCESS_KEY: str
    """Master key for accessing storage account"""
    ACCOUNT_KEY: str
    """Master key for accessing storage account"""
    ACCOUNT_NAME: str
    """The name of the azure storage account"""
    AUTHORITY_ID: str
    """Tenant id used in oauth flows"""
    AZURE_AUTHORITY_ID: str
    """Tenant id used in oauth flows"""
    AZURE_CLIENT_ID: str
    """Service principal client id for authorizing requests"""
    AZURE_CLIENT_SECRET: str
    """Service principal client secret for authorizing requests"""
    AZURE_CONTAINER_NAME: str
    """Container name"""
    AZURE_DISABLE_TAGGING: bool
    """Disables tagging objects"""
    AZURE_ENDPOINT: str
    """Override the endpoint used to communicate with blob storage"""
    AZURE_FEDERATED_TOKEN_FILE: str
    """File containing token for Azure AD workload identity federation"""
    AZURE_IDENTITY_ENDPOINT: str
    """Endpoint to request a imds managed identity token"""
    AZURE_MSI_ENDPOINT: str
    """Endpoint to request a imds managed identity token"""
    AZURE_MSI_RESOURCE_ID: str
    """Msi resource id for use with managed identity authentication"""
    AZURE_OBJECT_ID: str
    """Object id for use with managed identity authentication"""
    AZURE_SKIP_SIGNATURE: bool
    """Skip signing requests"""
    AZURE_STORAGE_ACCESS_KEY: str
    """Master key for accessing storage account"""
    AZURE_STORAGE_ACCOUNT_KEY: str
    """Master key for accessing storage account"""
    AZURE_STORAGE_ACCOUNT_NAME: str
    """The name of the azure storage account"""
    AZURE_STORAGE_AUTHORITY_ID: str
    """Tenant id used in oauth flows"""
    AZURE_STORAGE_CLIENT_ID: str
    """Service principal client id for authorizing requests"""
    AZURE_STORAGE_CLIENT_SECRET: str
    """Service principal client secret for authorizing requests"""
    AZURE_STORAGE_ENDPOINT: str
    """Override the endpoint used to communicate with blob storage"""
    AZURE_STORAGE_MASTER_KEY: str
    """Master key for accessing storage account"""
    AZURE_STORAGE_SAS_KEY: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    AZURE_STORAGE_SAS_TOKEN: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    AZURE_STORAGE_TENANT_ID: str
    """Tenant id used in oauth flows"""
    AZURE_STORAGE_TOKEN: str
    """Bearer token"""
    AZURE_STORAGE_USE_EMULATOR: bool
    """Use object store with azurite storage emulator"""
    AZURE_TENANT_ID: str
    """Tenant id used in oauth flows"""
    AZURE_USE_AZURE_CLI: bool
    """Use azure cli for acquiring access token"""
    AZURE_USE_FABRIC_ENDPOINT: bool
    """Use object store with url scheme account.dfs.fabric.microsoft.com"""
    BEARER_TOKEN: str
    """Bearer token"""
    CLIENT_ID: str
    """Service principal client id for authorizing requests"""
    CLIENT_SECRET: str
    """Service principal client secret for authorizing requests"""
    CONTAINER_NAME: str
    """Container name"""
    DISABLE_TAGGING: bool
    """Disables tagging objects"""
    ENDPOINT: str
    """Override the endpoint used to communicate with blob storage"""
    FEDERATED_TOKEN_FILE: str
    """File containing token for Azure AD workload identity federation"""
    IDENTITY_ENDPOINT: str
    """Endpoint to request a imds managed identity token"""
    MASTER_KEY: str
    """Master key for accessing storage account"""
    MSI_ENDPOINT: str
    """Endpoint to request a imds managed identity token"""
    MSI_RESOURCE_ID: str
    """Msi resource id for use with managed identity authentication"""
    OBJECT_ID: str
    """Object id for use with managed identity authentication"""
    SAS_KEY: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    SAS_TOKEN: str
    """
    Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in
    the azure storage explorer or azure portal.
    """
    SKIP_SIGNATURE: bool
    """Skip signing requests"""
    TENANT_ID: str
    """Tenant id used in oauth flows"""
    TOKEN: str
    """Bearer token"""
    USE_AZURE_CLI: bool
    """Use azure cli for acquiring access token"""
    USE_EMULATOR: bool
    """Use object store with azurite storage emulator"""
    USE_FABRIC_ENDPOINT: bool
    """Use object store with url scheme account.dfs.fabric.microsoft.com"""

class AzureAccessKey(TypedDict):
    """A shared Azure Storage Account Key.

    <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
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
    """

    token: str
    """Bearer token."""

    expires_at: datetime | None
    """Expiry datetime of credential. The datetime should have time zone set.

    If None, the credential will never expire.
    """

AzureCredential: TypeAlias = AzureAccessKey | AzureSASToken | AzureBearerToken
"""A type alias for supported azure credentials to be returned from `AzureCredentialProvider`."""

class AzureCredentialProvider(Protocol):
    """A type hint for a synchronous or asynchronous callback to provide custom Azure credentials.

    This should be passed into the `credential_provider` parameter of `AzureStore`.
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
        config: AzureConfig | AzureConfigInput | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: AzureCredentialProvider | None = None,
        **kwargs: Unpack[AzureConfigInput],
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
        config: AzureConfig | AzureConfigInput | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: AzureCredentialProvider | None = None,
        **kwargs: Unpack[AzureConfigInput],
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
