"""Planetary computer credential providers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import ParseResult, urlparse, urlunparse
from warnings import warn

if TYPE_CHECKING:
    import sys

    import aiohttp
    import requests

    from obstore.store import AzureConfig, AzureSASToken

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

_BLOB_STORAGE_DOMAIN = ".blob.core.windows.net"
_SETTINGS_ENV_STR = "~/.planetarycomputer/settings.env"
_SETTINGS_ENV_FILE = Path(_SETTINGS_ENV_STR).expanduser()

_DEFAULT_SAS_TOKEN_ENDPOINT = "https://planetarycomputer.microsoft.com/api/sas/v1/token"  # noqa: S105

__all__ = ["PlanetaryComputerCredentialProvider"]


class PlanetaryComputerCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] for accessing Planetary Computer."""  # noqa: E501

    config: AzureConfig
    prefix: str | None

    def __init__(  # noqa: PLR0913
        self,
        url: str | None = None,
        *,
        account_name: str | None = None,
        container_name: str | None = None,
        session: requests.Session | None = None,
        sas_url: str | None = None,
        subscription_key: str | None = None,
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

        self.account, self.container, self.prefix = (
            _validate_url_container_account_input(
                url=url,
                account_name=account_name,
                container_name=container_name,
            )
        )
        self.config = {"account_name": self.account, "container_name": self.container}

    def __call__(self) -> AzureSASToken:
        """Fetch a new token."""
        token_request_url = self.settings.token_request_url(
            account_name=self.account,
            container_name=self.container,
        )

        response = self.session.get(
            token_request_url,
            headers=(
                {"Ocp-Apim-Subscription-Key": self.settings.subscription_key}
                if self.settings.subscription_key
                else None
            ),
        )
        response.raise_for_status()
        return _parse_json_response(response.json())


class PlanetaryComputerAsyncCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] for accessing Planetary Computer."""  # noqa: E501

    config: AzureConfig
    prefix: str | None

    def __init__(  # noqa: PLR0913
        self,
        url: str | None = None,
        *,
        account_name: str | None = None,
        container_name: str | None = None,
        session: aiohttp.ClientSession | None = None,
        sas_url: str | None = None,
        subscription_key: str | None = None,
    ) -> None:
        """Construct a new PlanetaryComputerAsyncCredentialProvider."""
        self.settings = _Settings.load(
            subscription_key=subscription_key,
            sas_url=sas_url,
        )

        if session is None:
            try:
                from aiohttp_retry import ExponentialRetry, RetryClient

                retry_options = ExponentialRetry(attempts=1)
                retry_client = RetryClient(
                    raise_for_status=False,
                    retry_options=retry_options,
                )
                self.session = retry_client
            except ImportError:
                from aiohttp import ClientSession

                # Put this after validating that we can import aiohttp
                warn(
                    "aiohttp_retry not installed and custom client not provided. "
                    "Planetary Computer authentication will not be retried.",
                    RuntimeWarning,
                    stacklevel=3,
                )

                self.session = ClientSession()

        else:
            self.session = session

        self.account, self.container, self.prefix = (
            _validate_url_container_account_input(
                url=url,
                account_name=account_name,
                container_name=container_name,
            )
        )
        self.config = {"account_name": self.account, "container_name": self.container}

    async def __call__(self) -> AzureSASToken:
        """Fetch a new token."""
        token_request_url = self.settings.token_request_url(
            account_name=self.account,
            container_name=self.container,
        )

        headers = (
            {"Ocp-Apim-Subscription-Key": self.settings.subscription_key}
            if self.settings.subscription_key
            else None
        )
        async with self.session.get(token_request_url, headers=headers) as resp:
            resp.raise_for_status()
            return _parse_json_response(await resp.json())


def _validate_url_container_account_input(
    *,
    url: str | None,
    account_name: str | None,
    container_name: str | None,
) -> tuple[str, str, str | None]:
    if url is not None:
        if container_name is not None or account_name is not None:
            raise ValueError(
                "Cannot pass container_name or account_name when passing url.",
            )

        parsed_url = urlparse(url.rstrip("/"))
        return _parse_blob_url(parsed_url)

    if container_name is None or account_name is None:
        msg = (
            "Must pass both container_name and account_name when url is not passed.",
        )
        raise ValueError(msg)

    return account_name, container_name, None


def _parse_blob_url(parsed_url: ParseResult) -> tuple[str, str, str | None]:
    """Find the account and container in a blob URL.

    Returns:
        Tuple of the account name and container name

    """
    if not parsed_url.netloc.endswith(_BLOB_STORAGE_DOMAIN):
        msg = (
            f"Invalid blob URL: {urlunparse(parsed_url)}\n"
            f"Could not parse account name from {parsed_url.netloc}.\n"
            f"Expected to end with {_BLOB_STORAGE_DOMAIN}."
        )
        raise ValueError(msg)

    try:
        account_name = parsed_url.netloc.split(".")[0]
        parsed_path = parsed_url.path.lstrip("/").split("/", 1)
        if len(parsed_path) == 1:
            container_name = parsed_path[0]
            prefix = None
        else:
            container_name, prefix = parsed_path

    except Exception as failed_parse:
        msg = f"Invalid blob URL: {urlunparse(parsed_url)}"
        raise ValueError(msg) from failed_parse

    return account_name, container_name, prefix


def _parse_json_response(d: dict[str, str]) -> AzureSASToken:
    expires_at = datetime.fromisoformat(d["msft:expiry"].replace("Z", "+00:00"))
    return {
        "sas_token": d["token"],
        "expires_at": expires_at,
    }


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
