"""Credential providers for accessing [NASA Earthdata].

[NASA Earthdata]: https://www.earthdata.nasa.gov/
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from obstore.auth._http import default_aiohttp_session, default_requests_session

if TYPE_CHECKING:
    from collections.abc import Coroutine, Mapping
    from typing import Any, Callable

    import aiohttp
    import aiohttp_retry
    import requests

    from obstore.store import S3Config, S3Credential


EARTHDATA_HOST_OPS = "urs.earthdata.nasa.gov"
EARTHDATA_HOST_UAT = "uat.urs.earthdata.nasa.gov"


__all__ = [
    "EARTHDATA_HOST_OPS",
    "EARTHDATA_HOST_UAT",
    "NasaEarthdataAsyncCredentialProvider",
    "NasaEarthdataCredentialProvider",
]


class NasaEarthdataCredentialProvider:
    """An [S3Store][obstore.store.S3Store] credential provider for accessing [NASA Earthdata] data resources.

    This credential provider uses `requests`, and will error if that cannot be imported.

    NASA Earthdata supports public [in-region direct S3 access]. This
    credential provider automatically manages the S3 credentials.

    !!! note

        You must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    Examples:
        ```py
        import obstore.store
        from obstore.auth.earthdata import NasaEarthdataCredentialProvider

        # Obtain an S3 credentials URL and an S3 data/download URL, typically
        # via metadata returned from a NASA CMR collection or granule query.
        credentials_url = "https://data.ornldaac.earthdata.nasa.gov/s3credentials"
        data_url = "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/GEDI04_A_2024332225741_O33764_03_T01289_02_004_01_V002.h5"
        data_prefix_url, filename = data_url.rsplit("/", 1)

        # Since no Earthdata credentials are specified, environment variables or netrc
        # will be used to locate them in order to obtain S3 credentials from the URL.
        credential_provider = NasaEarthdataCredentialProvider(credentials_url)
        store = obstore.store.from_url(data_prefix_url, credential_provider=credential_provider)

        # Download the file by streaming chunks
        try:
            result = obstore.get(store, filename)
            with open(filename, "wb") as f:
                for chunk in iter(result):
                    f.write(chunk)
        finally:
            credential_provider.close()
        ```

    [NASA Earthdata]: https://www.earthdata.nasa.gov/
    [in-region direct S3 access]: https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME

    """  # noqa: E501

    config: S3Config

    def __init__(
        self,
        credentials_url: str,
        *,
        host: str | None = None,
        auth: str | tuple[str, str] | None = None,
        session: requests.Session | None = None,
    ) -> None:
        """Construct a new NasaEarthdataCredentialProvider.

        Args:
            credentials_url: Endpoint for obtaining S3 credentials from a NASA DAAC
                hosting data of interest.  NASA Earthdata credentials are required for
                obtaining S3 credentials from this endpoint.

        Keyword Args:
            host: Hostname for NASA Earthdata authentication.

                Precedence is as follows:

                1. Uses the specified value, if not `None`.
                2. Uses the environment variable `EARTHDATA_HOST`, if set.
                3. Uses the NASA Earthdata Operational host: [EARTHDATA_HOST_OPS][]

            auth: Authentication information; can be a NASA Earthdata token (`str`),
                NASA Earthdata username/password (tuple), or `None`.  Defaults to
                `None`, in which case, environment variables are used, if set.

                Precedence is as follows:

                1. Uses the specified value, if not `None`.
                2. Uses the environment variable `EARTHDATA_TOKEN`, if set.
                3. Uses the environment variables `EARTHDATA_USERNAME` and
                   `EARTHDATA_PASSWORD`, if both are set.
                4. Uses netrc to locate a username and password for `host`.
                   Uses the environment variable `NETRC`, if set, to locate a
                   netrc file; otherwise, uses the default netrc file location
                   (`~/.netrc` on non-Windows OS or `~/_netrc` on Windows).

            session: The requests session to use for making requests to obtain S3
                credentials. Defaults to `None`, in which case a default session
                is created internally.  In this case, use this credential
                provider's `close` method to release resources when you are
                finished with it.

        """
        self.config = {"region": "us-west-2"}
        self._session: requests.Session | None = session or default_requests_session()
        # Avoid closing a user-supplied session (the user is responsible for that)
        self._close = None if session else self._session.close
        self._get_credentials = _make_get_credentials(
            credentials_url,
            host or _default_host(),
            auth or _default_auth(),
        )

    def __call__(self) -> S3Credential:
        """Request updated credentials."""
        if self._session is None:
            msg = "credential provider was closed"
            raise ValueError(msg)

        metadata = self._get_credentials(self._session)

        return _metadata_to_s3_credential(metadata)

    def close(self) -> None:
        """Close the underlying session.

        You should call this method after you've finished all obstore calls to close the
        underlying [requests.Session][].  If you supplied your own session object,
        it will not be closed.
        """
        if self._close:
            self._session = None
            self._close()


def _make_get_credentials(
    credentials_url: str,
    host: str,
    auth: str | tuple[str, str] | None,
) -> Callable[[requests.Session], Mapping[str, str]]:
    if isinstance(auth, str):
        return lambda session: _get_with_token(session, credentials_url, auth)
    return lambda session: _get_with_basic_auth(session, credentials_url, host, auth)


def _get_with_token(
    session: requests.Session,
    credentials_url: str,
    token: str,
) -> Mapping[str, str]:
    with session.get(
        credentials_url,
        allow_redirects=False,
        headers={"Authorization": f"Bearer {token}"},
    ) as r:
        r.raise_for_status()
        _translate_redirect_as_unauthorized(r)

        return r.json()


def _get_with_basic_auth(
    session: requests.Session,
    credentials_url: str,
    host: str,
    basic_auth: tuple[str, str] | None,
) -> Mapping[str, str]:
    with session.get(credentials_url, allow_redirects=False) as r:
        r.raise_for_status()
        location = r.headers["location"]

    # We were redirected, so we must use basic auth credentials with the
    # redirect location.  If the host of the redirect is the same host we have
    # creds for, pass them along; otherwise, netrc will be used (implicitly),
    # if the session's trust_env attribute is set to True (default).

    redirect_host = str(urlparse(location).hostname)
    auth = basic_auth if redirect_host == host else None

    with session.get(location, auth=auth) as r:
        r.raise_for_status()
        _translate_redirect_as_unauthorized(r)

        return r.json()


def _translate_redirect_as_unauthorized(r: requests.Response) -> requests.Response:
    if r.is_redirect:
        # We were redirected; basic auth creds are invalid or not found via netrc
        r.status_code = 401
        r.reason = "Unauthorized"
        r.raise_for_status()

    return r


class NasaEarthdataAsyncCredentialProvider:
    """A credential provider for accessing [NASA Earthdata] to be used with an [S3Store][obstore.store.S3Store].

    This credential provider uses `aiohttp`, and will error if that cannot be imported.

    NASA Earthdata supports public [in-region direct S3 access]. This
    credential provider automatically manages the S3 credentials.

    !!! note

        You must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    Examples:
        ```py
        import obstore.store
        from obstore.auth.earthdata import NasaEarthdataCredentialProvider

        # Obtain an S3 credentials URL and an S3 data/download URL, typically
        # via metadata returned from a NASA CMR collection or granule query.
        credentials_url = "https://data.ornldaac.earthdata.nasa.gov/s3credentials"
        data_url = "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/GEDI04_A_2024332225741_O33764_03_T01289_02_004_01_V002.h5"
        data_prefix_url, filename = data_url.rsplit("/", 1)

        # Since no Earthdata credentials are specified, environment variables or netrc
        # will be used to locate them in order to obtain S3 credentials from the URL.
        credential_provider = NasaEarthdataAsyncCredentialProvider(credentials_url)
        store = obstore.store.from_url(data_prefix_url, credential_provider=credential_provider)

        # Download the file by streaming chunks
        try:
            result = await obstore.get_async(store, filename)
            with open(filename, "wb") as f:
                async for chunk in aiter(result):
                    f.write(chunk)
        finally:
            await credential_provider.close()
        ```

    [NASA Earthdata]: https://www.earthdata.nasa.gov/
    [in-region direct S3 access]: https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME

    """  # noqa: E501

    config: S3Config

    def __init__(
        self,
        credentials_url: str,
        *,
        host: str | None = None,
        auth: str | tuple[str, str] | None = None,
        session: aiohttp.ClientSession | aiohttp_retry.RetryClient | None = None,
    ) -> None:
        """Construct a new NasaEarthdataAsyncCredentialProvider.

        This credential provider uses `aiohttp`, and will error if that cannot be
        imported.

        Refer to
        [NasaEarthdataCredentialProvider][obstore.auth.earthdata.NasaEarthdataCredentialProvider.__init__]
        for argument explanations.

        """
        self.config = {"region": "us-west-2"}
        self._session: aiohttp.ClientSession | aiohttp_retry.RetryClient | None = (
            session or default_aiohttp_session()
        )
        # Avoid closing a user-supplied session (the user is responsible for that)
        self._close = None if session else self._session.close
        self._get_credentials_async = _make_get_credentials_async(
            credentials_url,
            host or _default_host(),
            auth or _default_auth(),
        )

    async def __call__(self) -> S3Credential:
        """Request updated credentials."""
        if self._session is None:
            msg = "credential provider was closed"
            raise ValueError(msg)

        metadata = await self._get_credentials_async(self._session)

        return _metadata_to_s3_credential(metadata)

    async def close(self) -> None:
        """Close the underlying session, if it was not supplied by the user.

        You should call this method after you've finished all obstore calls to close the
        underlying [aiohttp.ClientSession][].  If you supplied your own session object,
        it will not be closed.
        """
        if self._close:
            self._session = None
            await self._close()


def _make_get_credentials_async(
    credentials_url: str,
    host: str,
    auth: str | tuple[str, str] | None,
) -> Callable[
    [aiohttp.ClientSession | aiohttp_retry.RetryClient],
    Coroutine[Any, Any, Mapping[str, str]],
]:
    import aiohttp

    async def get_with_token(
        session: aiohttp.ClientSession | aiohttp_retry.RetryClient,
    ) -> Mapping[str, str]:
        return await _get_with_token_async(session, credentials_url, auth)  # type: ignore[arg-type]

    async def get_with_basic_auth(
        session: aiohttp.ClientSession | aiohttp_retry.RetryClient,
    ) -> Mapping[str, str]:
        return await _get_with_basic_auth_async(session, credentials_url, host, bauth)

    bauth = aiohttp.BasicAuth(*auth) if isinstance(auth, tuple) else None

    return get_with_token if isinstance(auth, str) else get_with_basic_auth


async def _get_with_token_async(
    session: aiohttp.ClientSession | aiohttp_retry.RetryClient,
    credentials_url: str,
    token: str,
) -> Mapping[str, str]:
    async with session.get(
        credentials_url,
        allow_redirects=False,
        headers={"Authorization": f"Bearer {token}"},
    ) as r:
        r.raise_for_status()
        _translate_aiohttp_redirect_as_unauthorized(r)

        return await r.json(content_type=None)


async def _get_with_basic_auth_async(
    session: aiohttp.ClientSession | aiohttp_retry.RetryClient,
    credentials_url: str,
    host: str,
    basic_auth: aiohttp.BasicAuth | None,
) -> Mapping[str, str]:
    async with session.get(credentials_url, allow_redirects=False) as r:
        r.raise_for_status()
        location = r.headers["location"]

    # We were redirected, so we must use basic auth credentials with the
    # redirect location.  If the host of the redirect is the same host we have
    # creds for, pass them along; otherwise, netrc will be used (implicitly),
    # if the session's trust_env attribute is set to True (default).

    redirect_host = str(urlparse(location).hostname)
    auth = basic_auth if redirect_host == host else None

    async with session.get(location, auth=auth) as r:
        r.raise_for_status()
        _translate_aiohttp_redirect_as_unauthorized(r)

        return await r.json(content_type=None)


def _translate_aiohttp_redirect_as_unauthorized(
    r: aiohttp.ClientResponse,
) -> aiohttp.ClientResponse:
    temporary_redirect_status = 307

    if r.status == temporary_redirect_status:
        # We were redirected; basic auth creds are invalid or not found via netrc
        r.status = 401
        r.reason = "Unauthorized"
        r.raise_for_status()

    return r


def _default_host() -> str:
    return os.environ.get("EARTHDATA_HOST", EARTHDATA_HOST_OPS)


def _default_auth() -> str | tuple[str, str] | None:
    if token := os.environ.get("EARTHDATA_TOKEN"):
        return token

    if (username := os.environ.get("EARTHDATA_USERNAME")) and (
        password := os.environ.get("EARTHDATA_PASSWORD")
    ):
        return (username, password)

    return None


def _metadata_to_s3_credential(metadata: Mapping[str, str]) -> S3Credential:
    return {
        "access_key_id": metadata["accessKeyId"],
        "secret_access_key": metadata["secretAccessKey"],
        "token": metadata["sessionToken"],
        "expires_at": datetime.fromisoformat(metadata["expiration"]),
    }
