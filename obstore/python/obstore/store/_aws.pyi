from collections.abc import Coroutine
from datetime import datetime
from typing import Any, Literal, NotRequired, Protocol, TypeAlias, TypedDict, Unpack

from ._client import ClientConfig
from ._retry import RetryConfig

# To update s3 region list:
# import pandas as pd  # noqa: ERA001
# url = "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html"  # noqa: ERA001
# result = pd.read_html(url)  # noqa: ERA001
# sorted(result[0]["Region"])  # noqa: ERA001
S3Regions: TypeAlias = Literal[
    "af-south-1",
    "ap-east-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-7",
    "ca-central-1",
    "ca-west-1",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "il-central-1",
    "me-central-1",
    "me-south-1",
    "mx-central-1",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-gov-east-1",
    "us-gov-west-1",
    "us-west-1",
    "us-west-2",
]
"""AWS regions."""

S3ChecksumAlgorithm: TypeAlias = Literal[
    "CRC64NVME",
    "CRC32",
    "CRC32C",
    "SHA1",
    "SHA256",
]
"""S3 Checksum algorithms

From https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#using-additional-checksums
"""

S3EncryptionAlgorithm: TypeAlias = Literal[
    "AES256",
    "aws:kms",
    "aws:kms:dsse",
    "sse-c",
]

class S3Config(TypedDict, total=False):
    """Configuration parameters for S3Store.

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import S3Config
        ```
    """

    access_key_id: str
    """AWS Access Key"""
    bucket: str
    """Bucket name"""
    checksum_algorithm: S3ChecksumAlgorithm | str
    """
    Sets the [checksum algorithm] which has to be used for object integrity check during upload.

    [checksum algorithm]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
    """
    conditional_put: str
    """Configure how to provide conditional put support

    Supported values:

    - `"etag"` (default): Supported for S3-compatible stores that support conditional
        put using the standard [HTTP precondition] headers `If-Match` and
        `If-None-Match`.

        [HTTP precondition]: https://datatracker.ietf.org/doc/html/rfc9110#name-preconditions

    - `"dynamo:<TABLE_NAME>"` or `"dynamo:<TABLE_NAME>:<TIMEOUT_MILLIS>"`: The name of a DynamoDB table to use for coordination.

        This will use the same region, credentials and endpoint as configured for S3.
    """
    container_credentials_relative_uri: str
    """Set the container credentials relative URI

    <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
    """
    copy_if_not_exists: Literal["multipart"] | str
    """Configure how to provide "copy if not exists".

    Supported values:

    - `"multipart"`:

        Native Amazon S3 supports copy if not exists through a multipart upload
        where the upload copies an existing object and is completed only if the
        new object does not already exist.

        !!! warning
            When using this mode, `copy_if_not_exists` does not copy tags
            or attributes from the source object.

        !!! warning
            When using this mode, `copy_if_not_exists` makes only a best
            effort attempt to clean up the multipart upload if the copy operation
            fails. Consider using a lifecycle rule to automatically clean up
            abandoned multipart uploads.

    - `"header:<HEADER_NAME>:<HEADER_VALUE>"`:

        Some S3-compatible stores, such as Cloudflare R2, support copy if not exists
        semantics through custom headers.

        If set, `copy_if_not_exists` will perform a normal copy operation with the
        provided header pair, and expect the store to fail with `412 Precondition
        Failed` if the destination file already exists.

        For example `header: cf-copy-destination-if-none-match: *`, would set
        the header `cf-copy-destination-if-none-match` to `*`.

    - `"header-with-status:<HEADER_NAME>:<HEADER_VALUE>:<STATUS>"`:

        The same as the header variant above but allows custom status code checking, for
        object stores that return values other than 412.

    - `"dynamo:<TABLE_NAME>"` or `"dynamo:<TABLE_NAME>:<TIMEOUT_MILLIS>"`:

        The name of a DynamoDB table to use for coordination.

        The default timeout is used if not specified. This will use the same region,
        credentials and endpoint as configured for S3.
    """
    default_region: S3Regions | str
    """Default region"""
    disable_tagging: bool
    """Disable tagging objects. This can be desirable if not supported by the backing store."""
    endpoint: str
    """Sets custom endpoint for communicating with AWS S3."""
    imdsv1_fallback: bool
    """Fall back to ImdsV1"""
    metadata_endpoint: str
    """Set the instance metadata endpoint"""
    region: S3Regions | str
    """Region"""
    request_payer: bool
    """If `True`, enable operations on requester-pays buckets."""
    s3_express: bool
    """Enable Support for S3 Express One Zone"""
    secret_access_key: str
    """Secret Access Key"""
    server_side_encryption: S3EncryptionAlgorithm | str
    """Type of encryption to use.

    If set, must be one of:

    - `"AES256"` (SSE-S3)
    - `"aws:kms"` (SSE-KMS)
    - `"aws:kms:dsse"` (DSSE-KMS)
    - `"sse-c"`
    """
    session_token: str
    """Token to use for requests (passed to underlying provider)"""
    skip_signature: bool
    """If `True`, S3Store will not fetch credentials and will not sign requests."""
    sse_bucket_key_enabled: bool
    """
    If set to `True`, will use the bucket's default KMS key for server-side encryption.
    If set to `False`, will disable the use of the bucket's default KMS key for server-side encryption.
    """
    sse_customer_key_base64: str
    """
    The base64 encoded, 256-bit customer encryption key to use for server-side
    encryption. If set, the server side encryption config value must be `"sse-c"`.
    """
    sse_kms_key_id: str
    """
    The KMS key ID to use for server-side encryption.

    If set, the server side encryption config value must be `"aws:kms"` or `"aws:kms:dsse"`.
    """
    unsigned_payload: bool
    """Avoid computing payload checksum when calculating signature."""
    virtual_hosted_style_request: bool
    """If virtual hosted style request has to be used."""

class S3Credential(TypedDict):
    """An S3 credential.

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import S3Credential
        ```
    """

    access_key_id: str
    """AWS access key ID."""

    secret_access_key: str
    """AWS secret access key"""

    token: NotRequired[str | None]
    """AWS token."""

    expires_at: datetime | None
    """Expiry datetime of credential. The datetime should have time zone set.

    If None, the credential will never expire.
    """

class S3CredentialProvider(Protocol):
    """A type hint for a synchronous or asynchronous callback to provide custom S3 credentials.

    This should be passed into the `credential_provider` parameter of `S3Store`.

    **Examples:**

    Return static credentials that don't expire:
    ```py
    def get_credentials() -> S3Credential:
        return {
            "access_key_id": "...",
            "secret_access_key": "...",
            "token": None,
            "expires_at": None,
        }
    ```

    Return static credentials that are valid for 5 minutes:
    ```py
    from datetime import datetime, timedelta, UTC

    async def get_credentials() -> S3Credential:
        return {
            "access_key_id": "...",
            "secret_access_key": "...",
            "token": None,
            "expires_at": datetime.now(UTC) + timedelta(minutes=5),
        }
    ```

    A class-based credential provider with state:

    ```py
    from __future__ import annotations

    from typing import TYPE_CHECKING

    import boto3
    import botocore.credentials

    if TYPE_CHECKING:
        from obstore.store import S3Credential


    class Boto3CredentialProvider:
        credentials: botocore.credentials.Credentials

        def __init__(self, session: boto3.session.Session) -> None:
            credentials = session.get_credentials()
            if credentials is None:
                raise ValueError("Received None from session.get_credentials")

            self.credentials = credentials

        def __call__(self) -> S3Credential:
            frozen_credentials = self.credentials.get_frozen_credentials()
            return {
                "access_key_id": frozen_credentials.access_key,
                "secret_access_key": frozen_credentials.secret_key,
                "token": frozen_credentials.token,
                "expires_at": None,
            }
    ```

    !!! warning "Not importable at runtime"

        To use this type hint in your code, import it within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore.store import S3CredentialProvider
        ```
    """

    @staticmethod
    def __call__() -> S3Credential | Coroutine[Any, Any, S3Credential]:
        """Return an `S3Credential`."""

class S3Store:
    """Interface to an Amazon S3 bucket.

    All constructors will check for environment variables. All environment variables
    starting with `AWS_` will be evaluated. Names must match keys from
    [`S3Config`][obstore.store.S3Config]. Only upper-case environment
    variables are accepted.

    Some examples of variables extracted from environment:

    - `AWS_ACCESS_KEY_ID` -> access_key_id
    - `AWS_SECRET_ACCESS_KEY` -> secret_access_key
    - `AWS_DEFAULT_REGION` -> region
    - `AWS_ENDPOINT` -> endpoint
    - `AWS_SESSION_TOKEN` -> token
    - `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` -> <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
    - `AWS_REQUEST_PAYER` -> set to "true" to permit requester-pays connections.

    **Examples**:

    **Using requester-pays buckets**:

    Pass `request_payer=True` as a keyword argument or have `AWS_REQUESTER_PAYS=True`
    set in the environment.

    **Anonymous requests**:

    Pass `skip_signature=True` as a keyword argument or have `AWS_SKIP_SIGNATURE=True`
    set in the environment.
    """

    def __init__(
        self,
        bucket: str | None = None,
        *,
        prefix: str | None = None,
        config: S3Config | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: S3CredentialProvider | None = None,
        **kwargs: Unpack[S3Config],  # type: ignore[GeneralTypeIssues] (bucket key overlaps with positional arg)
    ) -> None:
        """Create a new S3Store.

        Args:
            bucket: The AWS bucket to use.

        Keyword Args:
            prefix: A prefix within the bucket to use for all operations.
            config: AWS configuration. Values in this config will override values inferred from the environment. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.
            credential_provider: A callback to provide custom S3 credentials.
            kwargs: AWS configuration values. Supports the same values as `config`, but as named keyword args.

        Returns:
            S3Store

        """
    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: S3Config | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        credential_provider: S3CredentialProvider | None = None,
        **kwargs: Unpack[S3Config],
    ) -> S3Store:
        """Parse available connection info from a well-known storage URL.

        The supported url schemes are:

        - `s3://<bucket>/<path>`
        - `s3a://<bucket>/<path>`
        - `https://s3.<region>.amazonaws.com/<bucket>`
        - `https://<bucket>.s3.<region>.amazonaws.com`
        - `https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket`

        Args:
            url: well-known storage URL.

        Keyword Args:
            config: AWS Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.
            credential_provider: A callback to provide custom S3 credentials.
            kwargs: AWS configuration values. Supports the same values as `config`, but as named keyword args.


        Returns:
            S3Store

        """

    def __getnewargs_ex__(self): ...
    @property
    def prefix(self) -> str | None:
        """Get the prefix applied to all operations in this store, if any."""
    @property
    def config(self) -> S3Config:
        """Get the underlying S3 config parameters."""
    @property
    def client_options(self) -> ClientConfig | None:
        """Get the store's client configuration."""
    @property
    def retry_config(self) -> RetryConfig | None:
        """Get the store's retry configuration."""
