"""Credential providers for Google Cloud Storage that use [`google.auth`][]."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast

import google.auth
import google.auth.credentials
from google.auth._default_async import default_async

if TYPE_CHECKING:
    from google.auth.credentials import Credentials
    from google.auth.transport._aiohttp_requests import Request as AsyncRequest
    from google.auth.transport.requests import Request

    from obstore.store import GCSCredential


class GoogleCredentialProvider:
    """A CredentialProvider for [GCSStore][obstore.store.GCSStore] that uses [`google.auth`][].

    This credential provider uses `google-auth` and `requests`, and will error if those
    cannot be imported.

    **Example:**

    ```py
    from obstore.auth.google import GoogleCredentialProvider
    from obstore.store import GCSStore

    credential_provider = GoogleCredentialProvider(credentials=...)
    store = GCSStore("bucket_name", credential_provider=credential_provider)
    ```
    """  # noqa: E501

    request: Request
    credentials: Credentials

    def __init__(
        self,
        credentials: Credentials | None = None,
        *,
        request: Request | None = None,
        # https://github.com/googleapis/google-auth-library-python/blob/446c8e79b20b7c063d6aa142857a126a7efa1fc3/google/auth/_helpers.py#L26-L28
        refresh_threshold: timedelta = timedelta(minutes=3, seconds=45),
    ) -> None:
        """Create a new GoogleCredentialProvider.

        Args:
            credentials: Credentials to use for this provider. Defaults to `None`, in
                which case [`google.auth.default`][] will be called to find application
                default credentials.

        Keyword Args:
            request: The Request instance to use for refreshing the token. This can be
                set to reuse an existing [`requests.Session`][]. Defaults to `None`, in
                which case a new [`Request`][google.auth.transport.requests.Request]
                will be instantiated.
            refresh_threshold: The length of time before the token timeout when a new
                token should be requested. Defaults to `timedelta(minutes=3,
                seconds=45)` ([suggested here](https://github.com/googleapis/google-auth-library-python/blob/446c8e79b20b7c063d6aa142857a126a7efa1fc3/google/auth/_helpers.py#L26-L28)).

        """
        from google.auth.transport.requests import Request

        if credentials is not None:
            self.credentials = credentials
        else:
            self.credentials, _ = google.auth.default()  # type: ignore # noqa: PGH003
        self.request = request or Request()
        self.refresh_threshold = refresh_threshold

    def __call__(self) -> GCSCredential:
        """Fetch the credentials."""
        self.credentials.refresh(self.request)
        return {
            # self.credentials.token is a str
            "token": cast("str", self.credentials.token),
            "expires_at": _replace_expiry_timezone_utc(self.credentials.expiry),
        }


class GoogleAsyncCredentialProvider:
    """An async CredentialProvider for [GCSStore][obstore.store.GCSStore] that uses [`google.auth`][].

    This credential provider should be preferred over the synchronous
    [GoogleCredentialProvider][obstore.auth.google.GoogleCredentialProvider]
    whenever you're using async obstore methods.

    This credential provider uses `google-auth` and `aiohttp`, and will error if those
    cannot be imported.

    **Example:**

    ```py
    from obstore.auth.google import GoogleAsyncCredentialProvider
    from obstore.store import GCSStore

    credential_provider = GoogleAsyncCredentialProvider(credentials=...)
    store = GCSStore("bucket_name", credential_provider=credential_provider)
    ```

    """  # noqa: E501

    async_request: AsyncRequest
    credentials: Credentials

    def __init__(
        self,
        credentials: Credentials | None = None,
        *,
        request: AsyncRequest | None = None,
        # https://github.com/googleapis/google-auth-library-python/blob/446c8e79b20b7c063d6aa142857a126a7efa1fc3/google/auth/_helpers.py#L26-L28
        refresh_threshold: timedelta = timedelta(minutes=3, seconds=45),
    ) -> None:
        """Create a new GoogleCredentialProvider.

        Args:
            credentials: Credentials to use for this provider. Defaults to `None`, in
                which case `google.auth._default_async.default_async` will be called to
                find application default credentials.

        Keyword Args:
            request: The Request instance to use for refreshing the token. This can be
                set to reuse an existing [`aiohttp.ClientSession`][]. Defaults to
                `None`, in which case a new
                `google.auth.transport._aiohttp_requests.Request` will be
                instantiated.
            refresh_threshold: The length of time before the token timeout when a new
                token should be requested. Defaults to `timedelta(minutes=3,
                seconds=45)` ([suggested here](https://github.com/googleapis/google-auth-library-python/blob/446c8e79b20b7c063d6aa142857a126a7efa1fc3/google/auth/_helpers.py#L26-L28)).

        """
        from google.auth.transport._aiohttp_requests import Request as AsyncRequest

        if credentials is not None:
            self.credentials = credentials
        else:
            self.credentials, _ = default_async()  # type: ignore # noqa: PGH003
        self.async_request = request or AsyncRequest()
        self.refresh_threshold = refresh_threshold

    async def __call__(self) -> GCSCredential:
        """Fetch the credentials."""
        await self.credentials.refresh(self.async_request)
        return {
            # self.credentials.token is a str
            "token": cast("str", self.credentials.token),
            "expires_at": _replace_expiry_timezone_utc(self.credentials.expiry),
        }


def _replace_expiry_timezone_utc(expiry: datetime | None) -> datetime | None:
    """Assign UTC timezone onto the expiry time."""
    if expiry is None:
        return None

    return expiry.replace(tzinfo=timezone.utc) if expiry.tzinfo is None else expiry
