# import pytest

# from obstore.exceptions import BaseError
# from obstore.store import GCSStore


# def test_overlapping_config_keys():
#     with pytest.raises(BaseError, match="Duplicate key"):
#         GCSStore(google_bucket="bucket", GOOGLE_BUCKET="bucket")

#     with pytest.raises(BaseError, match="Duplicate key"):
#         GCSStore(config={"google_bucket": "test", "GOOGLE_BUCKET": "test"})


import zoneinfo
from datetime import datetime, timedelta
from typing import TypedDict

import google.auth
import google.auth.credentials
from google.auth._default_async import default_async
from google.auth.credentials import Credentials, Scoped
from google.auth.transport._aiohttp_requests import Request as AsyncRequest
from google.auth.transport.requests import Request


class GCSCredential(TypedDict):
    token: str
    expiry: datetime | None


class BaseGoogleAuthCredentialProvider:
    credentials: Scoped
    refresh_threshold: timedelta

    def __init__(
        self,
        *,
        refresh_threshold: timedelta = timedelta(minutes=3, seconds=45),
    ) -> None:
        """_summary_

        Args:
            refresh_threshold: default google auth refresh threshold https://github.com/googleapis/google-auth-library-python/blob/446c8e79b20b7c063d6aa142857a126a7efa1fc3/google/auth/_helpers.py#L26-L28. Defaults to timedelta(minutes=3, seconds=45).

        """
        self.refresh_threshold = refresh_threshold

    def _replace_expiry_timezone_utc(self, expiry: datetime | None) -> datetime | None:
        if expiry is None:
            return None

        return (
            expiry.replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
            if expiry.tzinfo is None
            else expiry
        )


class GoogleAuthCredentialProvider(BaseGoogleAuthCredentialProvider):
    request: Request
    credentials: Credentials

    def __init__(
        self,
        credentials: Scoped | None = None,
        *,
        refresh_threshold: timedelta = timedelta(minutes=3, seconds=45),
    ) -> None:
        super().__init__(refresh_threshold=refresh_threshold)
        if credentials is not None:
            self.credentials = credentials
        else:
            self.credentials, _project_id = google.auth.default()
        self.request = Request()

    def __call__(self) -> GCSCredential:
        self.credentials.refresh(self.request)
        expiry: datetime = self._replace_expiry_timezone_utc(self.credentials.expiry)
        return {"token": self.credentials.token, "expiry": expiry}


class GoogleAuthAsyncCredentialProvider(BaseGoogleAuthCredentialProvider):
    request: AsyncRequest
    credentials: Credentials

    def __init__(
        self,
        credentials: Scoped | None = None,
        *,
        refresh_threshold: timedelta = timedelta(minutes=3, seconds=45),
    ) -> None:
        super().__init__(refresh_threshold=refresh_threshold)
        if credentials is not None:
            self.credentials = credentials
        else:
            self.credentials, _project_id = default_async()
        self.request = AsyncRequest()

    async def __call__(self) -> GCSCredential:
        await self.credentials.refresh(async_request)
        expiry: datetime = self._replace_expiry_timezone_utc(self.credentials.expiry)
        return {"token": self.credentials.token, "expiry": expiry}
