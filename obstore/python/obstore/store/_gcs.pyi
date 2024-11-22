from typing import Dict, Literal

from ._client import ClientConfigKey
from ._retry import RetryConfig

GCSConfigKey = Literal[
    "bucket_name",
    "bucket",
    "google_application_credentials",
    "google_bucket_name",
    "google_bucket",
    "google_service_account_key",
    "google_service_account_path",
    "google_service_account",
    "service_account_key",
    "service_account_path",
    "service_account",
    "BUCKET_NAME",
    "BUCKET",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_BUCKET_NAME",
    "GOOGLE_BUCKET",
    "GOOGLE_SERVICE_ACCOUNT_KEY",
    "GOOGLE_SERVICE_ACCOUNT_PATH",
    "GOOGLE_SERVICE_ACCOUNT",
    "SERVICE_ACCOUNT_KEY",
    "SERVICE_ACCOUNT_PATH",
    "SERVICE_ACCOUNT",
]
"""Valid Google Cloud Storage configuration keys

Either lower case or upper case strings are accepted.

- `"google_service_account"` or `"service_account"` or `"google_service_account_path"` or "service_account_path":  Path to the service account file.
- `"google_service_account_key"` or `"service_account_key"`: The serialized service account key
- `"google_bucket"` or `"google_bucket_name"` or `"bucket"` or `"bucket_name"`: Bucket name.
- `"google_application_credentials"`: Application credentials path. See <https://cloud.google.com/docs/authentication/provide-credentials-adc>.
"""

class GCSStore:
    """Configure a connection to Google Cloud Storage.

    If no credentials are explicitly provided, they will be sourced from the environment
    as documented
    [here](https://cloud.google.com/docs/authentication/application-default-credentials).
    """

    @classmethod
    def from_env(
        cls,
        bucket: str,
        *,
        config: Dict[GCSConfigKey, str] | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
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
        config: Dict[GCSConfigKey, str] | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> GCSStore:
        """Construct a new GCSStore with values populated from a well-known storage URL.

        The supported url schemes are:

        - `gs://<bucket>/<path>`

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
