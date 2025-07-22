"""Credential providers for accessing [NASA Earthdata].

[NASA Earthdata]: https://www.earthdata.nasa.gov/
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from obstore.auth._http import default_aiohttp_session, default_requests_session

if TYPE_CHECKING:
    import sys
    from collections.abc import Mapping

    if sys.version_info >= (3, 11):
        from typing import Never
    else:
        from typing_extensions import Never

    import aiohttp
    import aiohttp_retry
    import requests

    from obstore.store import S3Config, S3Credential


DEFAULT_EARTHDATA_HOST = "urs.earthdata.nasa.gov"
"""Hostname of the NASA Earthdata Login primary operational (OPS) system.

This is the default host used during credential authorization.
"""


__all__ = [
    "DEFAULT_EARTHDATA_HOST",
    "NasaEarthdataAsyncCredentialProvider",
    "NasaEarthdataCredentialProvider",
]


class NasaEarthdataCredentialProvider:
    """A credential provider for accessing [NASA Earthdata] data resources with an [S3Store][obstore.store.S3Store].

    This credential provider uses `requests`, and will error if that cannot be imported.

    NASA Earthdata supports public [in-region direct S3 access]. This
    credential provider automatically manages the S3 credentials.

    !!! note

        You must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    [NASA Earthdata]:
        https://www.earthdata.nasa.gov/
    [in-region direct S3 access]:
        https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME

    Examples:
        ```py
        from obstore.store import S3Store
        from obstore.auth.earthdata import NasaEarthdataCredentialProvider

        # Obtain an S3 credentials URL and an S3 data/download URL, typically
        # via metadata returned from a NASA CMR collection or granule query.
        credentials_url = "https://data.ornldaac.earthdata.nasa.gov/s3credentials"
        data_url = (
            "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/"
            "GEDI04_A_2024332225741_O33764_03_T01289_02_004_01_V002.h5"
        )
        data_prefix_url, filename = data_url.rsplit("/", 1)

        # Since no NASA Earthdata credentials are specified in this example,
        # environment variables or netrc will be used to locate them in order to
        # obtain S3 credentials from the URL.
        cp = NasaEarthdataCredentialProvider(credentials_url)

        store = S3Store.from_url(data_prefix_url, credential_provider=cp)

        # Download the file by streaming chunks
        try:
            result = obstore.get(store, filename)
            with open(filename, "wb") as f:
                for chunk in iter(result):
                    f.write(chunk)
        finally:
            cp.close()
        ```

    """  # noqa: E501

    config: S3Config
    _session: requests.Session | None

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
                3. Uses the NASA Earthdata operational host:
                   [DEFAULT_EARTHDATA_HOST][obstore.auth.earthdata.DEFAULT_EARTHDATA_HOST]

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
        self._session = session or default_requests_session()
        # Avoid closing a user-supplied session (the user is responsible for that)
        self._close = None if session else self._session.close

        self._auth = auth or _read_auth_from_env()
        self._credentials_url = credentials_url
        self._host = host or _read_host_from_env()

    @property
    def session(self) -> requests.Session:
        """Return the underlying session used for HTTP requests.

        Return either the session supplied at construction, or the default session
        created during initialization, if no session was supplied.

        Returns:
            The session used for HTTP requests.

        Raises:
            ValueError: If the credential provider has been closed and the session is
                unavailable.

        """
        if self._session is None:
            msg = "credential provider was closed"
            raise ValueError(msg)

        return self._session

    def __call__(self) -> S3Credential:
        """Request updated credentials."""
        if isinstance(self._auth, str):
            credentials = self._refresh_with_token(self._auth)
        else:
            credentials = self._refresh_with_basic_auth(self._auth)

        return _parse_credentials(credentials)

    def _refresh_with_token(self, token: str) -> Mapping[str, str]:
        with self.session.get(
            self._credentials_url,
            allow_redirects=False,
            headers={"Authorization": f"Bearer {token}"},
        ) as r:
            r.raise_for_status()

            if r.is_redirect:
                # We were redirected; basic auth creds are invalid or not given/found
                _raise_unauthorized_requests(r)

            return r.json()

    def _refresh_with_basic_auth(
        self,
        auth: tuple[str, str] | None,
    ) -> Mapping[str, str]:
        with self.session.get(self._credentials_url, allow_redirects=False) as r:
            r.raise_for_status()
            location = r.headers["location"]

        # We were redirected, so we must use basic auth credentials with the
        # redirect location.  If the host of the redirect is the same host we have
        # creds for, pass them along; otherwise, netrc will be used (implicitly),
        # if the session's trust_env attribute is set to True (default).

        redirect_host = str(urlparse(location).hostname)
        auth = auth if redirect_host == self._host else None

        with self.session.get(location, auth=auth) as r:
            r.raise_for_status()

            try:
                return r.json()
            except json.JSONDecodeError:
                # Content is not JSON; basic auth creds are invalid or not given/found
                _raise_unauthorized_requests(r)

    def close(self) -> None:
        """Release resources created by this credential provider."""
        if self._close:
            self._session = None
            self._close()


class NasaEarthdataAsyncCredentialProvider:
    """A credential provider for accessing [NASA Earthdata] data resources with an [S3Store][obstore.store.S3Store].

    This credential provider uses `aiohttp`, and will error if that cannot be imported.

    NASA Earthdata supports public [in-region direct S3 access]. This
    credential provider automatically manages the S3 credentials.

    !!! note

        You must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    [NASA Earthdata]:
        https://www.earthdata.nasa.gov/
    [in-region direct S3 access]:
        https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME

    Examples:
        ```py
        import obstore
        from obstore.auth.earthdata import NasaEarthdataCredentialProvider

        # Obtain an S3 credentials URL and an S3 data/download URL, typically
        # via metadata returned from a NASA CMR collection or granule query.
        credentials_url = "https://data.ornldaac.earthdata.nasa.gov/s3credentials"
        data_url = (
            "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/"
            "GEDI04_A_2024332225741_O33764_03_T01289_02_004_01_V002.h5"
        )
        data_prefix_url, filename = data_url.rsplit("/", 1)

        # Since no NASA Earthdata credentials are specified in this example,
        # environment variables or netrc will be used to locate them in order to
        # obtain S3 credentials from the URL.
        cp = NasaEarthdataAsyncCredentialProvider(credentials_url)

        store = obstore.store.from_url(data_prefix_url, credential_provider=cp)

        # Download the file by streaming chunks
        try:
            result = await obstore.get_async(store, filename)
            with open(filename, "wb") as f:
                async for chunk in aiter(result):
                    f.write(chunk)
        finally:
            await cp.close()
        ```

    """  # noqa: E501

    config: S3Config
    _session: aiohttp.ClientSession | aiohttp_retry.RetryClient | None

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
        import aiohttp

        self.config = {"region": "us-west-2"}
        self._session = session or default_aiohttp_session()
        # Avoid closing a user-supplied session (the user is responsible for that)
        self._close = None if session else self._session.close

        auth = auth or _read_auth_from_env()
        self._auth = aiohttp.BasicAuth(*auth) if isinstance(auth, tuple) else auth
        self._credentials_url = credentials_url
        self._host = host or _read_host_from_env()

    @property
    def session(self) -> aiohttp.ClientSession | aiohttp_retry.RetryClient:
        """Return the underlying session used for HTTP requests.

        Return either the session supplied at construction, or the default session
        created during initialization, if no session was supplied.

        Returns:
            The session used for HTTP requests.

        Raises:
            ValueError: If the credential provider has been closed and the session is
                unavailable.

        """
        if self._session is None:
            msg = "credential provider was closed"
            raise ValueError(msg)

        return self._session

    async def __call__(self) -> S3Credential:
        """Request updated credentials."""
        if isinstance(self._auth, str):
            credentials = await self._refresh_with_token(self._auth)
        else:
            credentials = await self._refresh_with_basic_auth(self._auth)

        return _parse_credentials(credentials)

    async def _refresh_with_token(self, token: str) -> Mapping[str, str]:
        async with self.session.get(
            self._credentials_url,
            allow_redirects=False,
            headers={"Authorization": f"Bearer {token}"},
            raise_for_status=True,
        ) as r:
            temporary_redirect_status = 307

            if r.status == temporary_redirect_status:
                # We were redirected; token is invalid
                _raise_unauthorized_aiohttp(r)

            return await r.json(content_type=None)

    async def _refresh_with_basic_auth(
        self,
        auth: aiohttp.BasicAuth | None,
    ) -> Mapping[str, str]:
        async with self.session.get(
            self._credentials_url,
            allow_redirects=False,
            raise_for_status=True,
        ) as r:
            location = r.headers["location"]

        # We were redirected, so we must use basic auth credentials with the
        # redirect location.  If the host of the redirect is the same host we have
        # creds for, pass them along; otherwise, netrc will be used (implicitly),
        # if the session's trust_env attribute is set to True (default).

        redirect_host = str(urlparse(location).hostname)
        auth = auth if redirect_host == self._host else None

        async with self.session.get(location, auth=auth, raise_for_status=True) as r:
            try:
                return await r.json(content_type=None)
            except json.JSONDecodeError:
                # Content is not JSON; basic auth creds are invalid or not given/found
                _raise_unauthorized_aiohttp(r)

    async def close(self) -> None:
        """Release resources created by this credential provider."""
        if self._close:
            self._session = None
            await self._close()


def _read_host_from_env() -> str:
    return os.environ.get("EARTHDATA_HOST", DEFAULT_EARTHDATA_HOST)


def _read_auth_from_env() -> str | tuple[str, str] | None:
    if token := os.environ.get("EARTHDATA_TOKEN"):
        return token

    if (username := os.environ.get("EARTHDATA_USERNAME")) and (
        password := os.environ.get("EARTHDATA_PASSWORD")
    ):
        return (username, password)

    return None


def _raise_unauthorized_requests(r: requests.Response) -> Never:
    r.status_code = 401
    r.reason = "Unauthorized"
    r.raise_for_status()

    # Required for type checkers since they cannot tell that the call to
    # raise_for_status above will always raise.
    raise AssertionError from None


def _raise_unauthorized_aiohttp(r: aiohttp.ClientResponse) -> Never:
    r.status = 401
    r.reason = "Unauthorized"
    r.raise_for_status()

    # Required for type checkers since they cannot tell that the call to
    # raise_for_status above will always raise.
    raise AssertionError from None


def _parse_credentials(credentials: Mapping[str, str]) -> S3Credential:
    return {
        "access_key_id": credentials["accessKeyId"],
        "secret_access_key": credentials["secretAccessKey"],
        "token": credentials["sessionToken"],
        "expires_at": datetime.fromisoformat(credentials["expiration"]),
    }
