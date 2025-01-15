from typing import Dict, TypedDict, Unpack

from ._client import ClientConfigKey
from ._retry import RetryConfig

class AzureConfig(TypedDict, total=False):
    access_key: str
    account_key: str
    account_name: str
    authority_id: str
    azure_authority_id: str
    azure_client_id: str
    azure_client_secret: str
    azure_container_name: str
    azure_disable_tagging: str
    azure_endpoint: str
    azure_federated_token_file: str
    azure_identity_endpoint: str
    azure_msi_endpoint: str
    azure_msi_resource_id: str
    azure_object_id: str
    azure_skip_signature: str
    azure_storage_access_key: str
    azure_storage_account_key: str
    azure_storage_account_name: str
    azure_storage_authority_id: str
    azure_storage_client_id: str
    azure_storage_client_secret: str
    azure_storage_endpoint: str
    azure_storage_master_key: str
    azure_storage_sas_key: str
    azure_storage_sas_token: str
    azure_storage_tenant_id: str
    azure_storage_token: str
    azure_storage_use_emulator: str
    azure_tenant_id: str
    azure_use_azure_cli: str
    azure_use_fabric_endpoint: str
    bearer_token: str
    client_id: str
    client_secret: str
    container_name: str
    disable_tagging: str
    endpoint: str
    federated_token_file: str
    identity_endpoint: str
    master_key: str
    msi_endpoint: str
    msi_resource_id: str
    object_id: str
    sas_key: str
    sas_token: str
    skip_signature: str
    tenant_id: str
    token: str
    use_azure_cli: str
    use_emulator: str
    use_fabric_endpoint: str
    ACCESS_KEY: str
    ACCOUNT_KEY: str
    ACCOUNT_NAME: str
    AUTHORITY_ID: str
    AZURE_AUTHORITY_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_CONTAINER_NAME: str
    AZURE_DISABLE_TAGGING: str
    AZURE_ENDPOINT: str
    AZURE_FEDERATED_TOKEN_FILE: str
    AZURE_IDENTITY_ENDPOINT: str
    AZURE_MSI_ENDPOINT: str
    AZURE_MSI_RESOURCE_ID: str
    AZURE_OBJECT_ID: str
    AZURE_SKIP_SIGNATURE: str
    AZURE_STORAGE_ACCESS_KEY: str
    AZURE_STORAGE_ACCOUNT_KEY: str
    AZURE_STORAGE_ACCOUNT_NAME: str
    AZURE_STORAGE_AUTHORITY_ID: str
    AZURE_STORAGE_CLIENT_ID: str
    AZURE_STORAGE_CLIENT_SECRET: str
    AZURE_STORAGE_ENDPOINT: str
    AZURE_STORAGE_MASTER_KEY: str
    AZURE_STORAGE_SAS_KEY: str
    AZURE_STORAGE_SAS_TOKEN: str
    AZURE_STORAGE_TENANT_ID: str
    AZURE_STORAGE_TOKEN: str
    AZURE_STORAGE_USE_EMULATOR: str
    AZURE_TENANT_ID: str
    AZURE_USE_AZURE_CLI: str
    AZURE_USE_FABRIC_ENDPOINT: str
    BEARER_TOKEN: str
    CLIENT_ID: str
    CLIENT_SECRET: str
    CONTAINER_NAME: str
    DISABLE_TAGGING: str
    ENDPOINT: str
    FEDERATED_TOKEN_FILE: str
    IDENTITY_ENDPOINT: str
    MASTER_KEY: str
    MSI_ENDPOINT: str
    MSI_RESOURCE_ID: str
    OBJECT_ID: str
    SAS_KEY: str
    SAS_TOKEN: str
    SKIP_SIGNATURE: str
    TENANT_ID: str
    TOKEN: str
    USE_AZURE_CLI: str
    USE_EMULATOR: str
    USE_FABRIC_ENDPOINT: str

"""Valid Azure storage configuration keys

Either lower case or upper case strings are accepted.

- `"azure_storage_account_key"`, `"azure_storage_access_key"`, `"azure_storage_master_key"`, `"master_key"`, `"account_key"`, `"access_key"`: Master key for accessing storage account
- `"azure_storage_account_name"`, `"account_name"`: The name of the azure storage account
- `"azure_storage_client_id"`, `"azure_client_id"`, `"client_id"`: Service principal client id for authorizing requests
- `"azure_storage_client_secret"`, `"azure_client_secret"`, `"client_secret"`: Service principal client secret for authorizing requests
- `"azure_storage_tenant_id"`, `"azure_storage_authority_id"`, `"azure_tenant_id"`, `"azure_authority_id"`, `"tenant_id"`, `"authority_id"`: Tenant id used in oauth flows
- `"azure_storage_sas_key"`, `"azure_storage_sas_token"`, `"sas_key"`, `"sas_token"`: Shared access signature.

    The signature is expected to be percent-encoded, `much `like they are provided in the azure storage explorer or azure portal.

- `"azure_storage_token"`, `"bearer_token"`, `"token"`: Bearer token
- `"azure_storage_use_emulator"`, `"use_emulator"`: Use object store with azurite storage emulator
- `"azure_storage_endpoint"`, `"azure_endpoint"`, `"endpoint"`: Override the endpoint used to communicate with blob storage
- `"azure_msi_endpoint"`, `"azure_identity_endpoint"`, `"identity_endpoint"`, `"msi_endpoint"`: Endpoint to request a imds managed identity token
- `"azure_object_id"`, `"object_id"`: Object id for use with managed identity authentication
- `"azure_msi_resource_id"`, `"msi_resource_id"`: Msi resource id for use with managed identity authentication
- `"azure_federated_token_file"`, `"federated_token_file"`: File containing token for Azure AD workload identity federation
- `"azure_use_fabric_endpoint"`, `"use_fabric_endpoint"`: Use object store with url scheme account.dfs.fabric.microsoft.com
- `"azure_use_azure_cli"`, `"use_azure_cli"`: Use azure cli for acquiring access token
- `"azure_skip_signature"`, `"skip_signature"`: Skip signing requests
- `"azure_container_name"`, `"container_name"`: Container name
- `"azure_disable_tagging"`, `"disable_tagging"`: Disables tagging objects
"""

class AzureStore:
    """Configure a connection to Microsoft Azure Blob Storage container using the specified credentials."""

    def __init__(
        self,
        container: str,
        *,
        config: AzureConfig | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[AzureConfig],
    ) -> None:
        """Construct a new AzureStore.

        Args:
            container: _description_

        Keyword Args:
            config: Azure Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            AzureStore
        """

    @classmethod
    def from_env(
        cls,
        container: str,
        *,
        config: AzureConfig | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[AzureConfig],
    ) -> AzureStore:
        """Construct a new AzureStore with values pre-populated from environment variables.

        Variables extracted from environment:

        - `AZURE_STORAGE_ACCOUNT_NAME`: storage account name
        - `AZURE_STORAGE_ACCOUNT_KEY`: storage account master key
        - `AZURE_STORAGE_ACCESS_KEY`: alias for `AZURE_STORAGE_ACCOUNT_KEY`
        - `AZURE_STORAGE_CLIENT_ID` -> client id for service principal authorization
        - `AZURE_STORAGE_CLIENT_SECRET` -> client secret for service principal authorization
        - `AZURE_STORAGE_TENANT_ID` -> tenant id used in oauth flows

        Args:
            container: _description_

        Keyword Args:
            config: Azure Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            AzureStore
        """

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: AzureConfig | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
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

        !!! note
            Note that `from_url` will not use any additional parts of the path as a
            bucket prefix. It will only extract the container name, account name, and
            whether it's a fabric endpoint. If you wish to use a path prefix, consider
            wrapping this with `PrefixStore`.

        Args:
            url: well-known storage URL.

        Keyword Args:
            config: Azure Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            AzureStore
        """

    def __repr__(self) -> str: ...
