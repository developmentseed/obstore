"""Credential providers for accessing [NASA Earthdata].

[NASA Earthdata]: https://www.earthdata.nasa.gov/
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from obstore.store import S3Config, S3Credential

CREDENTIALS_API = "https://archive.podaac.earthdata.nasa.gov/s3credentials"


class NasaEarthdataCredentialProvider:
    """A credential provider for accessing [NASA Earthdata] to be used with [S3Store][obstore.store.S3Store].

    This credential provider uses `requests`, and will error if that cannot be imported.

    NASA Earthdata supports public [in-region direct S3
    access](https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME). This
    credential provider automatically manages the S3 credentials.

    !!! note

        Note that you must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    **Example**:

    ```py
    from obstore.auth.earthdata import NasaEarthdataCredentialProvider
    from obstore.store import S3Store

    credential_provider = NasaEarthdataCredentialProvider(username="...", password="...")
    store = S3Store("bucket_name", credential_provider=credential_provider)
    ```

    [NASA Earthdata]: https://www.earthdata.nasa.gov/
    """  # noqa: E501

    config: S3Config

    def __init__(
        self,
        username: str,
        password: str,
    ) -> None:
        """Create a new NasaEarthdataCredentialProvider.

        Args:
            username: Username to NASA Earthdata.
            password: Password to NASA Earthdata.

        """
        import requests

        # Pass region default
        self.config = {"region": "us-west-2"}
        self.session = requests.Session()
        self.session.auth = (username, password)

    def __call__(self) -> S3Credential:
        """Request updated credentials."""
        resp = self.session.get(CREDENTIALS_API, allow_redirects=True, timeout=15)
        auth_resp = self.session.get(resp.url, allow_redirects=True, timeout=15)
        creds = auth_resp.json()
        return {
            "access_key_id": creds["accessKeyId"],
            "secret_access_key": creds["secretAccessKey"],
            "token": creds["sessionToken"],
            "expires_at": datetime.fromisoformat(creds["expiration"]),
        }

    def close(self) -> None:
        """Close the underlying session.

        You should call this method after you've finished all obstore calls to close the
        underlying [requests.Session][].
        """
        self.session.close()


class NasaEarthdataAsyncCredentialProvider:
    """An async credential provider for accessing [NASA Earthdata] to be used with [S3Store][obstore.store.S3Store].

    This credential provider should be preferred over the synchronous
    [NasaEarthdataCredentialProvider][obstore.auth.earthdata.NasaEarthdataCredentialProvider]
    whenever you're using async obstore methods.

    This credential provider uses `aiohttp`, and will error if that cannot be imported.

    NASA Earthdata supports public [in-region direct S3
    access](https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME). This
    credential provider automatically manages the S3 credentials.

    !!! note

        Note that you must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    **Example**:

    ```py
    from obstore.auth.earthdata import NasaEarthdataAsyncCredentialProvider
    from obstore.store import S3Store

    credential_provider = NasaEarthdataAsyncCredentialProvider(
        username="...",
        password="...",
    )
    store = S3Store("bucket_name", credential_provider=credential_provider)
    ```

    [NASA Earthdata]: https://www.earthdata.nasa.gov/
    """  # noqa: E501

    config: S3Config

    def __init__(
        self,
        username: str,
        password: str,
    ) -> None:
        """Create a new NasaEarthdataAsyncCredentialProvider.

        Args:
            username: Username to NASA Earthdata.
            password: Password to NASA Earthdata.

        """
        from aiohttp import BasicAuth, ClientSession

        # Pass region default
        self.config = {"region": "us-west-2"}
        self.session = ClientSession(auth=BasicAuth(username, password))

    async def __call__(self) -> S3Credential:
        """Request updated credentials."""
        async with self.session.get(CREDENTIALS_API, allow_redirects=True) as resp:
            auth_url = resp.url
        async with self.session.get(auth_url, allow_redirects=True) as auth_resp:
            # Note: We parse the JSON manually instead of using `resp.json()` because
            # the response mimetype is incorrectly set to text/html.
            creds = json.loads(await auth_resp.text())
        return {
            "access_key_id": creds["accessKeyId"],
            "secret_access_key": creds["secretAccessKey"],
            "token": creds["sessionToken"],
            "expires_at": datetime.fromisoformat(creds["expiration"]),
        }

    async def close(self) -> None:
        """Close the underlying session.

        You should call this method after you've finished all obstore calls to close the
        underlying [aiohttp.ClientSession][].
        """
        await self.session.close()
