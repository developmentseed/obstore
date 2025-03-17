"""Credential providers for Azure Cloud Storage that use [`azure.identity`][].

[`azure.identity`]: https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import azure.identity
import azure.identity.aio

if TYPE_CHECKING:
    from collections.abc import Iterable

    from obstore.store import AzureCredential


DEFAULT_SCOPES = ("https://storage.azure.com/.default",)
"""Default scopes used for Azure credential providers."""


class AzureCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] that uses [`azure.identity`][].

    This credential provider uses `azure-identity`, and will error if this cannot
    be imported.

    **Example:**

    ```py
    from obstore.auth.azure import AzureCredentialProvider
    from obstore.store import AzureStore

    credential_provider = AzureCredentialProvider(credential=...)
    store = AzureStore("container", credential_provider=credential_provider)
    ```
    """  # noqa: E501

    def __init__(
        self,
        credential: azure.identity.AuthorizationCodeCredential
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
        | None = None,
        scopes: Iterable[str] = DEFAULT_SCOPES,
        tenant_id: str | None = None,
    ) -> None:
        """Create a new AzureCredentialProvider.

        Args:
            credential: Credential to use for this provider. Defaults to `None`,
                in which case [`azure.identity.DefaultAzureCredential`][] will be
                called to find default credentials.
            scopes: Scopes required by the access token.
            tenant_id: Optionally specify the Azure Tenant ID which will be passed to
                the credential's `get_token` method.

        [`azure.identity.DefaultAzureCredential`]: https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential

        """
        self.credential = credential or azure.identity.DefaultAzureCredential()
        self.scopes = scopes
        self.tenant_id = tenant_id

    def __call__(self) -> AzureCredential:
        """Fetch the credential."""
        self.token = self.credential.get_token(
            *self.scopes,
            tenant_id=self.tenant_id,
        )

        return {
            "token": self.token.token,
            "expires_at": datetime.fromtimestamp(self.token.expires_on, UTC),
        }


class AzureAsyncCredentialProvider:
    """An async CredentialProvider for [AzureStore][obstore.store.AzureStore] that uses [`azure.identity`][].

    This credential provider uses `azure-identity` and `aiohttp`, and will error if
    these cannot be imported.

    **Example:**

    ```py
    import asyncio
    import obstore
    from obstore.auth.azure import AzureAsyncCredentialProvider
    from obstore.store import AzureStore

    credential_provider = AzureAsyncCredentialProvider(credential=...)
    store = AzureStore("container", credential_provider=credential_provider)

    async def fetch_blobs():
        blobs = await obstore.list(store).collect_async()
        print(blobs)

    asyncio.run(fetch_blobs())
    ```
    """  # noqa: E501

    def __init__(
        self,
        credential: azure.identity.aio.AuthorizationCodeCredential
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
        | None = None,
        scopes: Iterable[str] = DEFAULT_SCOPES,
        tenant_id: str | None = None,
    ) -> None:
        """Create a new AzureAsyncCredentialProvider.

        Args:
            credential: Credential to use for this provider. Defaults to `None`,
                in which case [`azure.identity.aio.DefaultAzureCredential`][] will be
                called to find default credentials.
            scopes: Scopes required by the access token.
            tenant_id: Optionally specify the Azure Tenant ID which will be passed to
                the credential's `get_token` method.

        [`azure.identity.aio.DefaultAzureCredential`]: https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.aio.defaultazurecredential

        """
        self.credential = credential or azure.identity.aio.DefaultAzureCredential()
        self.scopes = scopes
        self.tenant_id = tenant_id

    async def __call__(self) -> AzureCredential:
        """Fetch the credential."""
        self.token = await self.credential.get_token(
            *self.scopes,
            tenant_id=self.tenant_id,
        )

        return {
            "token": self.token.token,
            "expires_at": datetime.fromtimestamp(self.token.expires_on, UTC),
        }
