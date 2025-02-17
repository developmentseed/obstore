# Authentication

## Native Authentication

- Environment variables
- Passed in via `config`
- Passed in as keyword parameters.

Note that many authentication variants are already supported natively.

AWS:

-
- IMDSv2

Azure:

- Azure CLI

This "default" authentication process is most efficient, as Obstore never needs to call into Python to update credentials.

## Official-SDK Authentication

### Using `boto3`

### Using `google.auth`

Refer to `obstore.google.auth`.

### Using `azure.identity`

## Custom Authentication

There's a long tail of possible authentication mechanisms. Obstore allows you to provide your own custom authentication callback.

You can provide **either a synchronous or asynchronous callback** for your custom authentication function.

!!! note

    Provide an asynchronous credential provider for optimal performance.

### Custom AWS Auth

Must return an [`S3Credential`][obstore.store.S3Credential].

```py
def get_credentials() -> S3Credential:
    return {
        "access_key_id": "...",
        "secret_access_key": "...",
        "token": "..." | None,
        "expires_at": datetime | None,
    }
```

#### Example

Below is an example custom credential provider for accessing [NASA Earthdata].

NASA Earthdata supports public [in-region direct S3
access](https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME). This
credential provider automatically manages refreshing the S3 credentials before
they expire.

Note that you must be in the same AWS region (`us-west-2`) to use this provider.

[NASA Earthdata]: https://www.earthdata.nasa.gov/

```py
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from obstore.store import S3Credential

CREDENTIALS_API = "https://archive.podaac.earthdata.nasa.gov/s3credentials"


class NasaEarthdataCredentialProvider:
    """A credential provider for accessing [NASA Earthdata].

    NASA Earthdata supports public [in-region direct S3
    access](https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME). This
    credential provider automatically manages the S3 credentials.

    !!! note

        Note that you must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    [NASA Earthdata]: https://www.earthdata.nasa.gov/
    """

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
```

Or asynchronously:

```py
from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from aiohttp import BasicAuth, ClientSession

if TYPE_CHECKING:
    from obstore.store import S3Credential

CREDENTIALS_API = "https://archive.podaac.earthdata.nasa.gov/s3credentials"


class NasaEarthdataAsyncCredentialProvider:
    """A credential provider for accessing [NASA Earthdata].

    NASA Earthdata supports public [in-region direct S3
    access](https://archive.podaac.earthdata.nasa.gov/s3credentialsREADME). This
    credential provider automatically manages the S3 credentials.

    !!! note

        Note that you must be in the same AWS region (`us-west-2`) to use the
        credentials returned from this provider.

    [NASA Earthdata]: https://www.earthdata.nasa.gov/
    """

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

    async def close(self):
        """Close the underlying session.

        You should call this method after you've finished all obstore calls.
        """
        await self.session.close()
```

### Custom GCP Auth


### Custom Azure Auth
