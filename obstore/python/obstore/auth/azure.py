"""Credential providers for Azure Cloud Storage that use [`azure.identity`][].

[`azure.identity`]: https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import azure.identity
import azure.identity.aio

if TYPE_CHECKING:
    from obstore.store import AzureCredential

    AzureCredentialUnionType = (
        azure.identity.AuthorizationCodeCredential
        | azure.identity.AzureCliCredential
        | azure.identity.AzureDeveloperCliCredential
        | azure.identity.AzurePipelinesCredential
        | azure.identity.AzurePowerShellCredential
        | azure.identity.CertificateCredential
        | azure.identity.ChainedTokenCredential
        | azure.identity.ClientAssertionCredential
        | azure.identity.ClientSecretCredential
        | azure.identity.DefaultAzureCredential
        | azure.identity.DeviceCodeCredential
        | azure.identity.EnvironmentCredential
        | azure.identity.InteractiveBrowserCredential
        | azure.identity.ManagedIdentityCredential
        | azure.identity.OnBehalfOfCredential
        | azure.identity.SharedTokenCacheCredential
        | azure.identity.UsernamePasswordCredential
        | azure.identity.VisualStudioCodeCredential
        | azure.identity.WorkloadIdentityCredential
    )

    AzureAsyncCredentialUnionType = (
        azure.identity.aio.AuthorizationCodeCredential
        | azure.identity.aio.AzureCliCredential
        | azure.identity.aio.AzureDeveloperCliCredential
        | azure.identity.aio.AzurePipelinesCredential
        | azure.identity.aio.AzurePowerShellCredential
        | azure.identity.aio.CertificateCredential
        | azure.identity.aio.ChainedTokenCredential
        | azure.identity.aio.ClientAssertionCredential
        | azure.identity.aio.ClientSecretCredential
        | azure.identity.aio.DefaultAzureCredential
        | azure.identity.aio.EnvironmentCredential
        | azure.identity.aio.ManagedIdentityCredential
        | azure.identity.aio.OnBehalfOfCredential
        | azure.identity.aio.SharedTokenCacheCredential
        | azure.identity.aio.VisualStudioCodeCredential
        | azure.identity.aio.WorkloadIdentityCredential
    )


class AzureAuthCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] that uses [`azure.identity`][].

    This credential provider uses `azure-identity`, and will error if this cannot
    be imported.

    **Example:**

    ```py
    from obstore.auth.azure import AzureAuthCredentialProvider
    from obstore.store import AzureStore

    credential_provider = AzureAuthCredentialProvider(credential=...)
    store = AzureStore("container", credential_provider=credential_provider)
    ```

    [`azure.identity`]: https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme
    """  # noqa: E501

    credential: AzureCredentialUnionType

    def __init__(
        self,
        credential: AzureCredentialUnionType | None = None,
        scopes: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """Create a new AzureAuthCredentialProvider.

        Args:
            credential: Credential to use for this provider. Defaults to `None`,
                in which case [`azure.identity.DefaultAzureCredential`][] will be
                called to find default credentials.
            scopes: Scopes required by the access token. If not specified,
                ["https://storage.azure.com/.default"] will be used by default.
            tenant_id: Optionally specify the Azure Tenant ID which will be passed to
                the credential's `get_token` method.

        [`azure.identity.DefaultAzureCredential`]: https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential

        """
        self.credential = credential or azure.identity.DefaultAzureCredential()

        # Use the Azure Storage scope by default
        self.scopes = scopes or ["https://storage.azure.com/.default"]

        self.tenant_id = tenant_id

        # Token cache
        self.token = None

    def __call__(self) -> AzureCredential:
        """Fetch the credential."""
        # Fetch new token if the cached token does not exist or expires
        # in less than 5 minutes.
        if self.token:
            token_expires_in = datetime.fromtimestamp(
                self.token.expires_on,
                UTC,
            ) - datetime.now(UTC)
        if not self.token or token_expires_in < timedelta(minutes=5):
            self.token = self.credential.get_token(
                *self.scopes,
                tenant_id=self.tenant_id,
            )

        return {
            "token": self.token.token,
            "expires_at": datetime.fromtimestamp(self.token.expires_on, UTC),
        }


class AzureAuthAsyncCredentialProvider:
    """An async CredentialProvider for [AzureStore][obstore.store.AzureStore] that uses [`azure.identity`][].

    This credential provider uses `azure-identity` and `aiohttp`, and will error if
    these cannot be imported.

    **Example:**

    ```py
    from obstore.auth.azure import AzureAuthAsyncCredentialProvider
    from obstore.store import AzureStore

    credential_provider = AzureAuthAsyncCredentialProvider(credential=...)
    store = AzureStore("container", credential_provider=credential_provider)
    ```

    [`azure.identity`]: https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme
    """  # noqa: E501

    credential: AzureAsyncCredentialUnionType

    def __init__(
        self,
        credential: AzureAsyncCredentialUnionType | None = None,
        scopes: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """Create a new AzureAuthAsyncCredentialProvider.

        Args:
            credential: Credential to use for this provider. Defaults to `None`,
                in which case [`azure.identity.aio.DefaultAzureCredential`][] will be
                called to find default credentials.
            scopes: Scopes required by the access token. If not specified,
                ["https://storage.azure.com/.default"] will be used by default.
            tenant_id: Optionally specify the Azure Tenant ID which will be passed to
                the credential's `get_token` method.

        [`azure.identity.aio.DefaultAzureCredential`]: https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.aio.defaultazurecredential

        """
        self.credential = credential or azure.identity.aio.DefaultAzureCredential()

        # Use the Azure Storage scope by default
        self.scopes = scopes or ["https://storage.azure.com/.default"]

        self.tenant_id = tenant_id

        # Token cache
        self.token = None

    async def __call__(self) -> AzureCredential:
        """Fetch the credential."""
        # Fetch new token if the cached token does not exist or expires
        # in less than 5 minutes.
        if self.token:
            token_expires_in = datetime.fromtimestamp(
                self.token.expires_on,
                UTC,
            ) - datetime.now(UTC)
        if not self.token or token_expires_in < timedelta(minutes=5):
            self.token = await self.credential.get_token(
                *self.scopes,
                tenant_id=self.tenant_id,
            )

        return {
            "token": self.token.token,
            "expires_at": datetime.fromtimestamp(self.token.expires_on, UTC),
        }
