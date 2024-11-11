from typing import Dict, Literal

from ._client import ClientConfigKey
from ._retry import RetryConfig

AzureConfigKey = Literal[
    "access_key",
    "account_key",
    "account_name",
    "authority_id",
    "azure_authority_id",
    "azure_client_id",
    "azure_client_secret",
    "azure_container_name",
    "azure_disable_tagging",
    "azure_endpoint",
    "azure_federated_token_file",
    "azure_identity_endpoint",
    "azure_msi_endpoint",
    "azure_msi_resource_id",
    "azure_object_id",
    "azure_skip_signature",
    "azure_storage_access_key",
    "azure_storage_account_key",
    "azure_storage_account_name",
    "azure_storage_authority_id",
    "azure_storage_client_id",
    "azure_storage_client_secret",
    "azure_storage_endpoint",
    "azure_storage_master_key",
    "azure_storage_sas_key",
    "azure_storage_sas_token",
    "azure_storage_tenant_id",
    "azure_storage_token",
    "azure_storage_use_emulator",
    "azure_tenant_id",
    "azure_use_azure_cli",
    "azure_use_fabric_endpoint",
    "bearer_token",
    "client_id",
    "client_secret",
    "container_name",
    "disable_tagging",
    "endpoint",
    "federated_token_file",
    "identity_endpoint",
    "master_key",
    "msi_endpoint",
    "msi_resource_id",
    "object_id",
    "sas_key",
    "sas_token",
    "skip_signature",
    "tenant_id",
    "token",
    "use_azure_cli",
    "use_emulator",
    "use_fabric_endpoint",
    "ACCESS_KEY",
    "ACCOUNT_KEY",
    "ACCOUNT_NAME",
    "AUTHORITY_ID",
    "AZURE_AUTHORITY_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "AZURE_CONTAINER_NAME",
    "AZURE_DISABLE_TAGGING",
    "AZURE_ENDPOINT",
    "AZURE_FEDERATED_TOKEN_FILE",
    "AZURE_IDENTITY_ENDPOINT",
    "AZURE_MSI_ENDPOINT",
    "AZURE_MSI_RESOURCE_ID",
    "AZURE_OBJECT_ID",
    "AZURE_SKIP_SIGNATURE",
    "AZURE_STORAGE_ACCESS_KEY",
    "AZURE_STORAGE_ACCOUNT_KEY",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_AUTHORITY_ID",
    "AZURE_STORAGE_CLIENT_ID",
    "AZURE_STORAGE_CLIENT_SECRET",
    "AZURE_STORAGE_ENDPOINT",
    "AZURE_STORAGE_MASTER_KEY",
    "AZURE_STORAGE_SAS_KEY",
    "AZURE_STORAGE_SAS_TOKEN",
    "AZURE_STORAGE_TENANT_ID",
    "AZURE_STORAGE_TOKEN",
    "AZURE_STORAGE_USE_EMULATOR",
    "AZURE_TENANT_ID",
    "AZURE_USE_AZURE_CLI",
    "AZURE_USE_FABRIC_ENDPOINT",
    "BEARER_TOKEN",
    "CLIENT_ID",
    "CLIENT_SECRET",
    "CONTAINER_NAME",
    "DISABLE_TAGGING",
    "ENDPOINT",
    "FEDERATED_TOKEN_FILE",
    "IDENTITY_ENDPOINT",
    "MASTER_KEY",
    "MSI_ENDPOINT",
    "MSI_RESOURCE_ID",
    "OBJECT_ID",
    "SAS_KEY",
    "SAS_TOKEN",
    "SKIP_SIGNATURE",
    "TENANT_ID",
    "TOKEN",
    "USE_AZURE_CLI",
    "USE_EMULATOR",
    "USE_FABRIC_ENDPOINT",
]
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

    @classmethod
    def from_env(
        cls,
        container: str,
        *,
        config: Dict[AzureConfigKey, str] | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
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
        config: Dict[AzureConfigKey, str] | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
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
            config: Azure Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            AzureStore
        """

    def __repr__(self) -> str: ...
