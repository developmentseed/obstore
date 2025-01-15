from typing import Dict, TypedDict, Unpack

from ._client import ClientConfigKey
from ._retry import RetryConfig

# Note: we removed `bucket` because it overlaps with an existing named arg in the
# constructors
class GCSConfig(TypedDict, total=False):
    bucket_name: str
    google_application_credentials: str
    google_bucket_name: str
    google_bucket: str
    google_service_account_key: str
    google_service_account_path: str
    google_service_account: str
    service_account_key: str
    service_account_path: str
    service_account: str
    BUCKET_NAME: str
    BUCKET: str
    GOOGLE_APPLICATION_CREDENTIALS: str
    GOOGLE_BUCKET_NAME: str
    GOOGLE_BUCKET: str
    GOOGLE_SERVICE_ACCOUNT_KEY: str
    GOOGLE_SERVICE_ACCOUNT_PATH: str
    GOOGLE_SERVICE_ACCOUNT: str
    SERVICE_ACCOUNT_KEY: str
    SERVICE_ACCOUNT_PATH: str
    SERVICE_ACCOUNT: str

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

    def __init__(
        self,
        bucket: str,
        *,
        config: GCSConfig | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
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
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
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
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
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
