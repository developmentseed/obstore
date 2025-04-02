"""Planetary computer credential providers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, urlparse, urlunparse
from warnings import warn

from obstore import __version__

if TYPE_CHECKING:
    import sys

    import aiohttp
    import aiohttp_retry
    import pystac
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

_USER_AGENT = f"obstore-v{__version__}"

__all__ = [
    "PlanetaryComputerAsyncCredentialProvider",
    "PlanetaryComputerCredentialProvider",
]


class PlanetaryComputerCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] for accessing [Planetary Computer] data resources.

    [Planetary Computer]: https://planetarycomputer.microsoft.com/

    This credential provider uses `requests`, and will error if that cannot be imported.

    Examples:
        ```py
        from obstore.store import AzureStore
        from obstore.auth.planetary_computer import PlanetaryComputerCredentialProvider

        url = "https://naipeuwest.blob.core.windows.net/naip/v002/mt/2023/mt_060cm_2023/"

        # Construct an AzureStore with this credential provider.
        #
        # The account, container, and container prefix are passed down to AzureStore
        # automatically.
        store = AzureStore(credential_provider=PlanetaryComputerCredentialProvider(url))

        # List some items in the container
        items = next(store.list())

        # Fetch a thumbnail
        path = "44106/m_4410602_nw_13_060_20230712_20240103.200.jpg"
        image_content = store.get(path).bytes()

        # Write out the image content to a file in the current directory
        with open("thumbnail.jpg", "wb") as f:
            f.write(image_content)
        ```

    """  # noqa: E501

    config: AzureConfig
    prefix: str | None

    def __init__(  # noqa: PLR0913
        self,
        url: str | None = None,
        *,
        account_name: str | None = None,
        container_name: str | None = None,
        session: requests.Session | None = None,
        subscription_key: str | None = None,
        sas_url: str | None = None,
    ) -> None:
        """Construct a new PlanetaryComputerCredentialProvider.

        Args:
            url: Either the `https` or `abfs` URL of blob storage to mount to, such as
                `"https://daymeteuwest.blob.core.windows.net/daymet-zarr/daily"` or `"abfs://daymet-zarr/daily/hi.zarr"`.

                For `abfs` URLs, `account_name` must be provided.

                For `https` URLs, neither `account_name` nor `container_name` may be
                provided.

                If `url` is not provided, `account_name` and `container_name` must be
                provided. Defaults to `None`.

        Keyword Args:
            account_name: The Azure storage account name. Must be provided for `abfs`
                URLs. If `url` is not provided, both this and `container_name` must be
                provided. Defaults to `None`.
            container_name: The Azure storage container name. If `url` is not provided,
                both this and `account_name` must be provided. Defaults to `None`.
            session: The requests session to use for making requests to the Planetary
                Computer token API. Defaults to `None`.
            subscription_key: A Planetary Computer subscription key.

                Precedence is as follows:

                1. Uses the passed-in value if not `None`.
                2. Uses the environment variable `PC_SDK_SUBSCRIPTION_KEY` if set.
                3. Uses the value of `PC_SDK_SUBSCRIPTION_KEY` in
                   `~/.planetarycomputer/settings.env`, if that file exists (requires
                   `python-dotenv` as a dependency).
                4. Defaults to `None`, which may apply request throttling.

            sas_url: The URL base for requesting new Planetary Computer SAS tokens.

                Precedence is as follows:

                1. Uses the passed-in value if not `None`.
                2. Uses the environment variable `PC_SDK_SAS_URL` if set.
                3. Uses the value of `PC_SDK_SAS_URL` in
                   `~/.planetarycomputer/settings.env`, if that file exists (requires
                   `python-dotenv` as a dependency).
                4. Defaults to `"https://planetarycomputer.microsoft.com/api/sas/v1/token"`.

        """
        self._settings = _Settings.load(
            subscription_key=subscription_key,
            sas_url=sas_url,
        )

        if session is None:
            self._session = _default_requests_session()
        else:
            self._session = session

        self._account, self._container, self.prefix = (
            _validate_url_container_account_input(
                url=url,
                account_name=account_name,
                container_name=container_name,
            )
        )
        self.config = {"account_name": self._account, "container_name": self._container}

    @classmethod
    def from_asset(
        cls,
        asset: pystac.Asset | dict[str, Any],
        *,
        session: requests.Session | None = None,
        subscription_key: str | None = None,
        sas_url: str | None = None,
    ) -> Self:
        """Create from a STAC Asset.

        Args:
            asset: Planetary Computer STAC Asset.

        Keyword Args:
            session: The requests session, passed on as a keyword argument to
                `__init__`.
            subscription_key: A Planetary Computer subscription key, passed on as a
                keyword argument to `__init__`.
            sas_url: The URL base for requesting new Planetary Computer SAS tokens,
                passed on as a keyword argument to `__init__`.

        Examples:
            ```py
            import pystac_client

            from obstore.auth.planetary_computer import PlanetaryComputerCredentialProvider

            stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/"
            catalog = pystac_client.Client.open(stac_url)

            collection = catalog.get_collection("daymet-daily-hi")
            asset = collection.assets["zarr-abfs"]

            credential_provider = PlanetaryComputerCredentialProvider.from_asset(asset)
            ```

        """  # noqa: E501
        url, account_name = _parse_asset(asset)
        return cls(
            url=url,
            account_name=account_name,
            session=session,
            subscription_key=subscription_key,
            sas_url=sas_url,
        )

    def __call__(self) -> AzureSASToken:
        """Fetch a new token."""
        token_request_url = self._settings.token_request_url(
            account_name=self._account,
            container_name=self._container,
        )

        headers = {"User-Agent": _USER_AGENT}
        if self._settings.subscription_key:
            headers["Ocp-Apim-Subscription-Key"] = self._settings.subscription_key
        response = self._session.get(token_request_url, headers=headers)
        response.raise_for_status()
        return _parse_json_response(response.json())


class PlanetaryComputerAsyncCredentialProvider:
    """A CredentialProvider for [AzureStore][obstore.store.AzureStore] for accessing [Planetary Computer][] data resources.

    [Planetary Computer]: https://planetarycomputer.microsoft.com/
    """  # noqa: E501

    config: AzureConfig
    prefix: str | None

    def __init__(  # noqa: PLR0913
        self,
        url: str | None = None,
        *,
        account_name: str | None = None,
        container_name: str | None = None,
        session: aiohttp.ClientSession | None = None,
        subscription_key: str | None = None,
        sas_url: str | None = None,
    ) -> None:
        """Construct a new PlanetaryComputerAsyncCredentialProvider.

        This credential provider uses `aiohttp`, and will error if that cannot be
        imported.

        Refer to
        [PlanetaryComputerCredentialProvider][obstore.auth.planetary_computer.PlanetaryComputerCredentialProvider.__init__]
        for argument explanations.
        """
        self._settings = _Settings.load(
            subscription_key=subscription_key,
            sas_url=sas_url,
        )

        if session is None:
            self._session = _default_aiohttp_session()
        else:
            self._session = session

        self._account, self._container, self.prefix = (
            _validate_url_container_account_input(
                url=url,
                account_name=account_name,
                container_name=container_name,
            )
        )
        self.config = {"account_name": self._account, "container_name": self._container}

    @classmethod
    def from_asset(
        cls,
        asset: pystac.Asset | dict[str, Any],
        *,
        session: aiohttp.ClientSession | None = None,
        subscription_key: str | None = None,
        sas_url: str | None = None,
    ) -> Self:
        """Create from a STAC Asset.

        Refer to
        [PlanetaryComputerCredentialProvider.from_asset][obstore.auth.planetary_computer.PlanetaryComputerCredentialProvider.from_asset]
        for argument explanations.
        """
        url, account_name = _parse_asset(asset)
        return cls(
            url=url,
            account_name=account_name,
            session=session,
            subscription_key=subscription_key,
            sas_url=sas_url,
        )

    async def __call__(self) -> AzureSASToken:
        """Fetch a new token."""
        token_request_url = self._settings.token_request_url(
            account_name=self._account,
            container_name=self._container,
        )

        headers = {"User-Agent": _USER_AGENT}
        if self._settings.subscription_key:
            headers["Ocp-Apim-Subscription-Key"] = self._settings.subscription_key

        async with self._session.get(token_request_url, headers=headers) as resp:
            resp.raise_for_status()
            return _parse_json_response(await resp.json())


def _parse_asset(asset: pystac.Asset | dict[str, Any]) -> tuple[str, str | None]:
    if (
        asset.__class__.__module__.startswith("pystac")
        and asset.__class__.__name__ == "Asset"
    ):
        d = asset.__dict__
    else:
        assert isinstance(asset, dict)
        d = asset

    extra_fields = d.get("extra_fields", {})
    if (
        isinstance(extra_fields, dict)
        and (
            (storage_options := extra_fields.get("xarray:storage_options"))
            or (storage_options := extra_fields.get("table:storage_options"))
        )
        and isinstance(storage_options, dict)
    ):
        account_name = storage_options.get("account_name")
    else:
        account_name = None

    return d["href"], account_name


def _validate_url_container_account_input(
    *,
    url: str | None,
    account_name: str | None,
    container_name: str | None,
) -> tuple[str, str, str | None]:
    if url is not None:
        if container_name is not None:
            raise ValueError(
                "Cannot pass container_name when passing url.",
            )

        parsed_url = urlparse(url.rstrip("/"))
        if parsed_url.scheme == "abfs":
            if not account_name:
                raise ValueError(
                    "account_name must be passed for abfs urls.",
                )

            return _parse_abfs_url(parsed_url, account_name)

        if account_name is not None:
            raise ValueError(
                "Cannot pass account_name when passing HTTPS blob storage url.",
            )

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


def _parse_abfs_url(
    parsed_url: ParseResult,
    account_name: str,
) -> tuple[str, str, str | None]:
    assert parsed_url.scheme == "abfs", "Expected abfs url in _parse_abfs_url"
    return account_name, parsed_url.netloc, parsed_url.path.lstrip("/")


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


def _default_requests_session() -> requests.Session:
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

    import requests
    import requests.adapters
    import urllib3
    import urllib3.util.retry

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
    return session


def _default_aiohttp_session() -> aiohttp_retry.RetryClient | aiohttp.ClientSession:
    try:
        from aiohttp_retry import ExponentialRetry, RetryClient

        retry_options = ExponentialRetry(attempts=1)
        return RetryClient(
            raise_for_status=False,
            retry_options=retry_options,
        )
    except ImportError:
        from aiohttp import ClientSession

        # Put this after validating that we can import aiohttp
        warn(
            "aiohttp_retry not installed and custom client not provided. "
            "Planetary Computer authentication will not be retried.",
            RuntimeWarning,
            stacklevel=3,
        )

        return ClientSession()
