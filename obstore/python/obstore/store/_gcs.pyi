from collections.abc import Coroutine
from datetime import datetime
from typing import Any, Protocol, TypedDict, Unpack

from ._client import ClientConfig
from ._retry import RetryConfig

class GCSConfig(TypedDict, total=False):
    """Configuration parameters returned from [GCSStore.config][obstore.store.GCSStore.config].

    Note that this is a strict subset of the keys allowed for _input_ into the store,
    see [GCSConfigInput][obstore.store.GCSConfigInput].
    """

    google_service_account: str
    """Path to the service account file."""

    google_service_account_key: str
    """The serialized service account key"""

    google_bucket: str
    """Bucket name."""

    google_application_credentials: str
    """Application credentials path.

    See <https://cloud.google.com/docs/authentication/provide-credentials-adc>."""

# Note: we removed `bucket` because it overlaps with an existing named arg in the
# constructors
class GCSConfigInput(TypedDict, total=False):
    """Configuration parameters for GCSStore.

    There are duplicates of many parameters, and parameters can be either upper or lower
    case. Not all parameters are required.
    """

    bucket_name: str
    """Bucket name."""
    google_application_credentials: str
    """Application credentials path.

    See <https://cloud.google.com/docs/authentication/provide-credentials-adc>."""
    google_bucket_name: str
    """Bucket name."""
    google_bucket: str
    """Bucket name."""
    google_service_account_key: str
    """The serialized service account key"""
    google_service_account_path: str
    """Path to the service account file."""
    google_service_account: str
    """Path to the service account file."""
    service_account_key: str
    """The serialized service account key"""
    service_account_path: str
    """Path to the service account file."""
    service_account: str
    """Path to the service account file."""
    BUCKET_NAME: str
    """Bucket name."""
    BUCKET: str
    """Bucket name."""
    GOOGLE_APPLICATION_CREDENTIALS: str
    """Application credentials path.

    See <https://cloud.google.com/docs/authentication/provide-credentials-adc>."""
    GOOGLE_BUCKET_NAME: str
    """Bucket name."""
    GOOGLE_BUCKET: str
    """Bucket name."""
    GOOGLE_SERVICE_ACCOUNT_KEY: str
    """The serialized service account key"""
    GOOGLE_SERVICE_ACCOUNT_PATH: str
    """Path to the service account file."""
    GOOGLE_SERVICE_ACCOUNT: str
    """Path to the service account file."""
    SERVICE_ACCOUNT_KEY: str
    """The serialized service account key"""
    SERVICE_ACCOUNT_PATH: str
    """Path to the service account file."""
    SERVICE_ACCOUNT: str
    """Path to the service account file."""

class GCSCredential(TypedDict):
    """A Google Cloud Storage Credential."""

    token: str
    """An HTTP bearer token."""

    expires_at: datetime | None
    """Expiry datetime of credential. The datetime should have time zone set.

    If None, the credential will never expire.
    """

class GCSCredentialProvider(Protocol):
    """A type hint for a synchronous or asynchronous callback to provide custom Google Cloud Storage credentials.

    This should be passed into the `credential_provider` parameter of `GCSStore`.
    """

    @staticmethod
    def __call__() -> GCSCredential | Coroutine[Any, Any, GCSCredential]:
        """Return a `GCSCredential`."""

class GCSStore:
    """Interface to Google Cloud Storage.

    All constructors will check for environment variables. All environment variables
    starting with `GOOGLE_` will be evaluated. Names must match keys from
    [`GCSConfig`][obstore.store.GCSConfig]. Only upper-case environment variables are
    accepted.

    Some examples of variables extracted from environment:

    - `GOOGLE_SERVICE_ACCOUNT`: location of service account file
    - `GOOGLE_SERVICE_ACCOUNT_PATH`: (alias) location of service account file
    - `SERVICE_ACCOUNT`: (alias) location of service account file
    - `GOOGLE_SERVICE_ACCOUNT_KEY`: JSON serialized service account key
    - `GOOGLE_BUCKET`: bucket name
    - `GOOGLE_BUCKET_NAME`: (alias) bucket name

    If no credentials are explicitly provided, they will be sourced from the environment
    as documented
    [here](https://cloud.google.com/docs/authentication/application-default-credentials).
    """

    def __init__(
        self,
        bucket: str | None = None,
        *,
        prefix: str | None = None,
        config: GCSConfig | GCSConfigInput | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: GCSCredentialProvider | None = None,
        **kwargs: Unpack[GCSConfigInput],
    ) -> None:
        """Construct a new GCSStore.

        Args:
            bucket: The GCS bucket to use.

        Keyword Args:
            prefix: A prefix within the bucket to use for all operations.
            config: GCS Configuration. Values in this config will override values inferred from the environment. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.
            credential_provider: A callback to provide custom Google credentials.
            kwargs: GCS configuration values. Supports the same values as `config`, but as named keyword args.

        Returns:
            GCSStore

        """

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        prefix: str | None = None,
        config: GCSConfig | GCSConfigInput | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: GCSCredentialProvider | None = None,
        **kwargs: Unpack[GCSConfigInput],
    ) -> GCSStore:
        """Construct a new GCSStore with values populated from a well-known storage URL.

        The supported url schemes are:

        - `gs://<bucket>/<path>`

        Args:
            url: well-known storage URL.

        Keyword Args:
            prefix: A prefix within the bucket to use for all operations.
            config: GCS Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.
            credential_provider: A callback to provide custom Google credentials.
            kwargs: GCS configuration values. Supports the same values as `config`, but as named keyword args.

        Returns:
            GCSStore

        """

    def __getnewargs_ex__(self): ...
    @property
    def prefix(self) -> str | None:
        """Get the prefix applied to all operations in this store, if any."""
    @property
    def config(self) -> GCSConfig:
        """Get the underlying GCS config parameters."""
    @property
    def client_options(self) -> ClientConfig | None:
        """Get the store's client configuration."""
    @property
    def retry_config(self) -> RetryConfig | None:
        """Get the store's retry configuration."""
