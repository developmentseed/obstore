"""Planetary computer credential providers."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import ParseResult, urlparse, urlunparse

import requests
import requests.adapters
import urllib3
import urllib3.util.retry

from ._settings import Settings

BLOB_STORAGE_DOMAIN = ".blob.core.windows.net"

if TYPE_CHECKING:
    from obstore.store import AzureCredential, AzureSASToken


class PlanetaryComputerCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] for accessing Planetary Computer."""  # noqa: E501

    def __init__(
        self,
        *,
        url: str | None = None,
        container: str | None = None,
        account: str | None = None,
    ) -> None:
        """Construct a new PlanetaryComputerCredentialProvider."""
        self.settings = Settings.get()

        if url is not None:
            assert container is None and account is None

            parsed_url = urlparse(url.rstrip("/"))

            self.account, self.container = parse_blob_url(parsed_url)
        else:
            assert container is not None and account is not None
            self.account = account
            self.container = container

    def __call__(self) -> AzureCredential:
        """Fetch a new token."""
        return _get_token_sync(
            account_name=self.account,
            container_name=self.container,
            settings=self.settings,
        )


def _get_token_sync(
    *,
    account_name: str,
    container_name: str,
    settings: Settings,
    retry_total: int = 10,
    retry_backoff_factor: float = 0.8,
) -> AzureSASToken:
    """Get a token for a container in a storage account.

    Args:
        account_name: The storage account name.
        container_name: The storage container name.
        settings: Planetary computer API settings.
        retry_total: The number of allowable retry attempts for REST API calls.
            Use retry_total=0 to disable retries. A backoff factor to apply between
            attempts.
        retry_backoff_factor: A backoff factor to apply between attempts
            after the second try (most errors are resolved immediately by a second
            try without a delay). Retry policy will sleep for:

            ``{backoff factor} * (2 ** ({number of total retries} - 1))`` seconds.
            If the backoff_factor is 0.1, then the retry will sleep for
            [0.0s, 0.2s, 0.4s, ...] between retries. The default value is 0.8.

    Returns:
        SASToken: the generated token

    """
    token_request_url = f"{settings.sas_url}/{account_name}/{container_name}"

    session = requests.Session()
    retry = urllib3.util.retry.Retry(
        total=retry_total,
        backoff_factor=retry_backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    response = session.get(
        token_request_url,
        headers=(
            {"Ocp-Apim-Subscription-Key": settings.subscription_key}
            if settings.subscription_key
            else None
        ),
    )
    response.raise_for_status()

    d = response.json()
    expires_at = datetime.fromisoformat(d["msft:expiry"].replace("Z", "+00:00"))

    return {
        "sas_token": d["token"],
        "expires_at": expires_at,
    }


def parse_blob_url(parsed_url: ParseResult) -> tuple[str, str]:
    """Find the account and container in a blob URL.

    Returns:
        Tuple of the account name and container name

    """
    try:
        account_name = parsed_url.netloc.split(".")[0]
        path_blob = parsed_url.path.lstrip("/").split("/", 1)
        container_name = path_blob[-2]
    except Exception as failed_parse:
        msg = f"Invalid blob URL: {urlunparse(parsed_url)}"
        raise ValueError(msg) from failed_parse

    return account_name, container_name
