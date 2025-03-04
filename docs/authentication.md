# Authentication

Authentication tends to be among the trickiest but most important elements of connecting to object storage. Obstore supports various native and custom authentication methods.

## Native Authentication

"Native" authentication refers to authentication methods that are natively supported by the underlying Rust `object_store` library.

Native authentication is most efficient, as obstore never needs to call into Python to update credentials.

### Order of application

Native authentication is checked in order:

- Environment variables
- Passed in via `config`/keyword parameters.

So any parameters passed by the `config` parameter or by keyword parameters will override any values found from environment variables.

### Native Authentication Variants

Note that many authentication variants are already supported natively.

#### AWS

- Basic authentication, where an access key ID, secret access key, and optionally token are passed in via environment variables or configuration parameters.
- [WebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html). This requires the `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN` environment variables to be set. Additionally, `AWS_ROLE_SESSION_NAME` can be set to specify a session name.
- [Container credentials](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html). Ensure you pass [`aws_container_credentials_relative_uri`][obstore.store.S3ConfigInput.aws_container_credentials_relative_uri] to the `S3Store`.
- [Instance credentials](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html).

(A transcription of [this underlying code](https://github.com/apache/arrow-rs/blob/a00f9f43a0530b9255e4f9940e43121deedb0cc7/object_store/src/aws/builder.rs#L900-L970)).

#### Google Cloud Storage

- Service account credentials
- Application default credentials
- Instance credentials

(A transcription of [this underlying code](https://github.com/apache/arrow-rs/blob/a00f9f43a0530b9255e4f9940e43121deedb0cc7/object_store/src/gcp/builder.rs#L451-L504)).

#### Azure

- Fabric OAuth2, using `fabric_token_service_url`, `fabric_workload_host`, `fabric_session_token`, and `fabric_cluster_identifier` passed in by the user
- Workload identity OAuth2, using a `client_id`, `tenant_id`, and `federated_token_file` passed in by the user
- OAuth2, using a `client_id`, `client_secret`, and `tenant_id` passed in by the user
- A SAS key passed in by the user.
- Azure CLI. (If you want to ensure the IMDS authentication is used below, pass [`use_azure_cli=False`][obstore.store.AzureConfigInput.use_azure_cli] to `AzureStore`.)
- IMDS Managed Identity Provider.

(A transcription of [this underlying code](https://github.com/apache/arrow-rs/blob/a00f9f43a0530b9255e4f9940e43121deedb0cc7/object_store/src/azure/builder.rs#L942-L1019)).

## Credential Providers

Obstore allows for passing in credential providers. These are **Python callbacks** that allow for full control over credential generation. Passing in a credential provider will override any native credentials.

### "Official" SDK credential providers

#### boto3

You can use the [`Boto3CredentialProvider`][obstore.auth.boto3.Boto3CredentialProvider] to use [`boto3.Session`][boto3.session.Session] to handle credentials.

```py
from boto3 import Session
from obstore.auth.boto3 import Boto3CredentialProvider
from obstore.store import S3Store

session = Session(...)
credential_provider = Boto3CredentialProvider(session)
store = S3Store("bucket_name", credential_provider=credential_provider)
```

<!-- SSO authentication.

Run `aws sso login`, potentially adding `--sso-session SSO_NAME`. Then start `boto3.Session` with `profile_name`, providing the profile that is associated with your sso session name. -->

Refer to [`obstore.auth.boto3`](api/auth/boto3.md).

#### google.auth

You can use the [`GoogleAuthCredentialProvider`][obstore.auth.google.GoogleAuthCredentialProvider] to use [`google.auth`][] to handle credentials.

```py
from obstore.auth.google import GoogleAuthCredentialProvider
from obstore.store import GCSStore

credential_provider = GoogleAuthCredentialProvider(credentials=...)
store = GCSStore("bucket_name", credential_provider=credential_provider)
```

Refer to [`obstore.auth.google`](api/auth/google.md).

<!-- #### Using `azure.identity` -->

### Custom Authentication

There's a long tail of possible authentication mechanisms. Obstore allows you to provide your own custom authentication callback.

You can provide **either a synchronous or asynchronous callback** for your custom authentication function.

!!! note

    Provide an asynchronous credential provider for optimal performance.

- A custom AWS credential provider, passed in to [`S3Store`][obstore.store.S3Store] must return an [`S3Credential`][obstore.store.S3Credential].
- A custom GCS credential provider, passed in to [`GCSStore`][obstore.store.GCSStore] must return a [`GCSCredential`][obstore.store.GCSCredential].
- A custom Azure credential provider, passed in to [`AzureStore`][obstore.store.AzureStore] must return an [`AzureCredential`][obstore.store.AzureCredential].

#### Basic Example

The simplest custom credential provider can be just a synchronous or asynchronous function callback:

```py
from datetime import datetime, timedelta, UTC

def get_credentials() -> S3Credential:
    return {
        "access_key_id": "...",
        "secret_access_key": "...",
        # Not always required
        "token": "...",
        "expires_at": datetime.now(UTC) + timedelta(minutes=30),
    }
```

Then just pass that function into `credential_provider`:

```py
S3Store(..., credential_provider=get_credentials)
```

#### Advanced Example

More advanced credential providers can be class based. A class can act as a
callable, just like a function callback, as long as it implements a `__call__`
method.

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

Then call it with

```py
credential_provider = NasaEarthdataCredentialProvider(username="...", password="...")
store = S3Store("bucket_name", credential_provider=credential_provider)
```
