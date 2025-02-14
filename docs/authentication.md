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

import base64
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

        Note that you must be in the same AWS region (`us-west-2`) to use this provider.

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
        self.username = username
        self.password = password

    def __call__(self) -> S3Credential:
        login_resp = requests.get(CREDENTIALS_API, allow_redirects=False)
        login_resp.raise_for_status()

        encoded_auth = base64.b64encode(
            f"{self.username}:{self.password}".encode("ascii"),
        )
        auth_redirect = requests.post(
            login_resp.headers["location"],
            data={"credentials": encoded_auth},
            headers={"Origin": CREDENTIALS_API},
            allow_redirects=False,
        )
        auth_redirect.raise_for_status()

        final = requests.get(auth_redirect.headers["location"], allow_redirects=False)
        results = requests.get(
            CREDENTIALS_API, cookies={"accessToken": final.cookies["accessToken"]}
        )
        results.raise_for_status()

        creds = results.json()
        return {
            "access_key_id": creds["accessKeyId"],
            "secret_access_key": creds["secretAccessKey"],
            "token": creds["sessionToken"],
            "expires_at": datetime.fromisoformat(creds["expiration"]),
        }
```

### Custom GCP Auth


### Custom Azure Auth
