"""Planetary computer credential providers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import ParseResult, urlparse, urlunparse

if TYPE_CHECKING:
    import sys

    import requests

    from obstore.store import AzureCredential, AzureSASToken

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

_SETTINGS_ENV_STR = "~/.planetarycomputer/settings.env"
_SETTINGS_ENV_FILE = Path(_SETTINGS_ENV_STR).expanduser()

_DEFAULT_SAS_TOKEN_ENDPOINT = "https://planetarycomputer.microsoft.com/api/sas/v1/token"  # noqa: S105

__all__ = ["PlanetaryComputerCredentialProvider"]


class PlanetaryComputerCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] for accessing Planetary Computer."""  # noqa: E501

    def __init__(
        self,
        *,
        account_name: str | None = None,
        container_name: str | None = None,
        sas_url: str | None = None,
        session: requests.Session | None = None,
        subscription_key: str | None = None,
        url: str | None = None,
    ) -> None:
        """Construct a new PlanetaryComputerCredentialProvider."""
        import requests
        import requests.adapters
        import urllib3
        import urllib3.util.retry

        self.settings = _Settings.load(
            subscription_key=subscription_key,
            sas_url=sas_url,
        )

        if session is None:
            # Upstream docstring in case we want to expose these values publicly
            # retry_total: The number of allowable retry attempts for REST API calls.
            #     Use retry_total=0 to disable retries. A backoff factor to apply
            #     between attempts.
            # retry_backoff_factor: A backoff factor to apply between attempts
            #     after the second try (most errors are resolved immediately by a second
            #     try without a delay). Retry policy will sleep for:

            #     ``{backoff factor} * (2 ** ({number of total retries} - 1))`` seconds.
            #     If the backoff_factor is 0.1, then the retry will sleep for
            #     [0.0s, 0.2s, 0.4s, ...] between retries. The default value is 0.8.
            retry_total = 10
            retry_backoff_factor = 0.8

            session = requests.Session()
            retry = urllib3.util.retry.Retry(
                total=retry_total,
                backoff_factor=retry_backoff_factor,  # type: ignore (invalid upstream typing)
                status_forcelist=[429, 500, 502, 503, 504],
            )

            adapter = requests.adapters.HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self.session = session
        else:
            self.session = session

        if url is not None:
            assert container_name is None and account_name is None

            parsed_url = urlparse(url.rstrip("/"))

            self.account, self.container = parse_blob_url(parsed_url)
        else:
            assert container_name is not None and account_name is not None
            self.account = account_name
            self.container = container_name

    def __call__(self) -> AzureCredential:
        """Fetch a new token."""
        return _get_token_sync(
            account_name=self.account,
            container_name=self.container,
            settings=self.settings,
            session=self.session,
        )


def _get_token_sync(
    *,
    account_name: str,
    container_name: str,
    settings: _Settings,
    session: requests.Session,
) -> AzureSASToken:
    """Get a token for a container in a storage account.

    Returns:
        SASToken: the generated token

    """
    token_request_url = settings.token_request_url(
        account_name=account_name,
        container_name=container_name,
    )

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


@dataclass
class _Settings:
    """Planetary Computer configuration settings."""

    subscription_key: str | None
    sas_url: str

    @classmethod
    def load(cls, *, subscription_key: str | None, sas_url: str | None) -> Self:
        """Load settings values.

        Order of precedence:

        1. Passed in values by the user.
        2. Environment variables
        3. Dotenv file
        4. Defaults

        """
        return cls(
            subscription_key=subscription_key or _subscription_key_default(),
            sas_url=sas_url or _sas_url_default(),
        )

    def token_request_url(
        self,
        *,
        account_name: str,
        container_name: str,
    ) -> str:
        return f"{self.sas_url}/{account_name}/{container_name}"


def _from_env(key: str) -> str | None:
    value = os.environ.get(key)
    if value is not None:
        return value

    if _SETTINGS_ENV_FILE.exists():
        try:
            import dotenv
        except ImportError as e:
            msg = f"python-dotenv dependency required to read from {_SETTINGS_ENV_STR}"
            raise ImportError(msg) from e

        values = dotenv.dotenv_values(_SETTINGS_ENV_FILE)
        return values.get(key)

    return None


def _subscription_key_default() -> str | None:
    return _from_env("PC_SDK_SUBSCRIPTION_KEY")


def _sas_url_default() -> str:
    return _from_env("PC_SDK_SAS_URL") or _DEFAULT_SAS_TOKEN_ENDPOINT
