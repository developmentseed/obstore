from typing import TypedDict, Unpack

from ._client import ClientConfig
from ._retry import RetryConfig

# Note: we removed `bucket` because it overlaps with an existing named arg in the
# constructors
class GCSConfig(TypedDict, total=False):
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

class GCSStore:
    """Configure a connection to Google Cloud Storage.

    If no credentials are explicitly provided, they will be sourced from the environment
    as documented
    [here](https://cloud.google.com/docs/authentication/application-default-credentials).
    """

    def __init__(
        self,
        bucket: str,
        *,
        config: GCSConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[GCSConfig],
    ) -> None:
        """Construct a new GCSStore.

        Args:
            bucket: The GCS bucket to use.

        Keyword Args:
            config: GCS Configuration. Values in this config will override values inferred from the environment. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            GCSStore
        """

    @classmethod
    def from_env(
        cls,
        bucket: str,
        *,
        config: GCSConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[GCSConfig],
    ) -> GCSStore:
        """Construct a new GCSStore with values pre-populated from environment variables.

        Variables extracted from environment:

        - `GOOGLE_SERVICE_ACCOUNT`: location of service account file
        - `GOOGLE_SERVICE_ACCOUNT_PATH`: (alias) location of service account file
        - `SERVICE_ACCOUNT`: (alias) location of service account file
        - `GOOGLE_SERVICE_ACCOUNT_KEY`: JSON serialized service account key
        - `GOOGLE_BUCKET`: bucket name
        - `GOOGLE_BUCKET_NAME`: (alias) bucket name

        Args:
            bucket: The GCS bucket to use.

        Keyword Args:
            config: GCS Configuration. Values in this config will override values inferred from the environment. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            GCSStore
        """

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: GCSConfig | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[GCSConfig],
    ) -> GCSStore:
        """Construct a new GCSStore with values populated from a well-known storage URL.

        The supported url schemes are:

        - `gs://<bucket>/<path>`

        !!! note
            Note that `from_url` will not use any additional parts of the path as a
            bucket prefix. It will only extract the bucket name. If you wish to use a
            path prefix, consider wrapping this with `PrefixStore`.

        Args:
            url: well-known storage URL.

        Keyword Args:
            config: GCS Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            GCSStore
        """

    def __repr__(self) -> str: ...
