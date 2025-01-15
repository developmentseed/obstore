from typing import TypedDict, Unpack

import boto3
import boto3.session
import botocore
import botocore.session

from ._client import ClientConfig
from ._retry import RetryConfig

# Note: we removed `bucket` because it overlaps with an existing named arg in the
# constructors
class S3Config(TypedDict, total=False):
    """Configuration parameters for S3Store.

    There are duplicates of many parameters, and parameters can be either upper or lower
    case. Not all parameters are required.
    """

    access_key_id: str
    """AWS Access Key"""
    aws_access_key_id: str
    """AWS Access Key"""
    aws_allow_http: bool
    aws_bucket_name: str
    """Bucket name"""
    aws_bucket: str
    """Bucket name"""
    aws_checksum_algorithm: str
    aws_conditional_put: str
    aws_container_credentials_relative_uri: str
    aws_copy_if_not_exists: str
    aws_default_region: str
    """Default region"""
    aws_disable_tagging: bool
    """Disable tagging objects. This can be desirable if not supported by the backing store."""
    aws_endpoint_url: str
    """Sets custom endpoint for communicating with AWS S3."""
    aws_endpoint: str
    """Sets custom endpoint for communicating with AWS S3."""
    aws_imdsv1_fallback: str
    """Fall back to ImdsV1"""
    aws_metadata_endpoint: str
    """Set the instance metadata endpoint"""
    aws_region: str
    """Region"""
    aws_request_payer: bool
    """If `True`, enable operations on requester-pays buckets."""
    aws_s3_express: bool
    """Enable Support for S3 Express One Zone"""
    aws_secret_access_key: str
    """Secret Access Key"""
    aws_server_side_encryption: str
    aws_session_token: str
    """Token to use for requests (passed to underlying provider)"""
    aws_skip_signature: bool
    """If `True`, S3Store will not fetch credentials and will not sign requests."""
    aws_sse_bucket_key_enabled: bool
    aws_sse_kms_key_id: str
    aws_token: str
    """Token to use for requests (passed to underlying provider)"""
    aws_unsigned_payload: bool
    """Avoid computing payload checksum when calculating signature."""
    aws_virtual_hosted_style_request: bool
    """If virtual hosted style request has to be used."""
    bucket_name: str
    """Bucket name"""
    checksum_algorithm: str
    conditional_put: str
    copy_if_not_exists: str
    default_region: str
    """Default region"""
    disable_tagging: bool
    """Disable tagging objects. This can be desirable if not supported by the backing store."""
    endpoint_url: str
    """Sets custom endpoint for communicating with AWS S3."""
    endpoint: str
    """Sets custom endpoint for communicating with AWS S3."""
    imdsv1_fallback: str
    """Fall back to ImdsV1"""
    metadata_endpoint: str
    """Set the instance metadata endpoint"""
    region: str
    """Region"""
    request_payer: bool
    """If `True`, enable operations on requester-pays buckets."""
    s3_express: bool
    """Enable Support for S3 Express One Zone"""
    secret_access_key: str
    """Secret Access Key"""
    session_token: str
    """Token to use for requests (passed to underlying provider)"""
    skip_signature: bool
    """If `True`, S3Store will not fetch credentials and will not sign requests."""
    token: str
    """Token to use for requests (passed to underlying provider)"""
    unsigned_payload: bool
    """Avoid computing payload checksum when calculating signature."""
    virtual_hosted_style_request: bool
    """If virtual hosted style request has to be used."""
    ACCESS_KEY_ID: str
    """AWS Access Key"""
    AWS_ACCESS_KEY_ID: str
    """AWS Access Key"""
    AWS_ALLOW_HTTP: bool
    AWS_BUCKET_NAME: str
    """Bucket name"""
    AWS_BUCKET: str
    """Bucket name"""
    AWS_CHECKSUM_ALGORITHM: str
    AWS_CONDITIONAL_PUT: str
    AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: str
    AWS_COPY_IF_NOT_EXISTS: str
    AWS_DEFAULT_REGION: str
    """Default region"""
    AWS_DISABLE_TAGGING: bool
    """Disable tagging objects. This can be desirable if not supported by the backing store."""
    AWS_ENDPOINT_URL: str
    """Sets custom endpoint for communicating with AWS S3."""
    AWS_ENDPOINT: str
    """Sets custom endpoint for communicating with AWS S3."""
    AWS_IMDSV1_FALLBACK: str
    """Fall back to ImdsV1"""
    AWS_METADATA_ENDPOINT: str
    """Set the instance metadata endpoint"""
    AWS_REGION: str
    """Region"""
    AWS_REQUEST_PAYER: bool
    """If `True`, enable operations on requester-pays buckets."""
    AWS_S3_EXPRESS: str
    """Enable Support for S3 Express One Zone"""
    AWS_SECRET_ACCESS_KEY: str
    """Secret Access Key"""
    AWS_SERVER_SIDE_ENCRYPTION: str
    AWS_SESSION_TOKEN: str
    """Token to use for requests (passed to underlying provider)"""
    AWS_SKIP_SIGNATURE: bool
    """If `True`, S3Store will not fetch credentials and will not sign requests."""
    AWS_SSE_BUCKET_KEY_ENABLED: bool
    AWS_SSE_KMS_KEY_ID: str
    AWS_TOKEN: str
    """Token to use for requests (passed to underlying provider)"""
    AWS_UNSIGNED_PAYLOAD: bool
    """Avoid computing payload checksum when calculating signature."""
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST: bool
    """If virtual hosted style request has to be used."""
    BUCKET_NAME: str
    """Bucket name"""
    BUCKET: str
    """Bucket name"""
    CHECKSUM_ALGORITHM: str
    CONDITIONAL_PUT: str
    COPY_IF_NOT_EXISTS: str
    DEFAULT_REGION: str
    """Default region"""
    DISABLE_TAGGING: bool
    """Disable tagging objects. This can be desirable if not supported by the backing store."""
    ENDPOINT_URL: str
    """Sets custom endpoint for communicating with AWS S3."""
    ENDPOINT: str
    """Sets custom endpoint for communicating with AWS S3."""
    IMDSV1_FALLBACK: str
    """Fall back to ImdsV1"""
    METADATA_ENDPOINT: str
    """Set the instance metadata endpoint"""
    REGION: str
    """Region"""
    REQUEST_PAYER: bool
    """If `True`, enable operations on requester-pays buckets."""
    S3_EXPRESS: str
    """Enable Support for S3 Express One Zone"""
    SECRET_ACCESS_KEY: str
    """Secret Access Key"""
    SESSION_TOKEN: str
    """Token to use for requests (passed to underlying provider)"""
    SKIP_SIGNATURE: bool
    """If `True`, S3Store will not fetch credentials and will not sign requests."""
    TOKEN: str
    """Token to use for requests (passed to underlying provider)"""
    UNSIGNED_PAYLOAD: bool
    """Avoid computing payload checksum when calculating signature."""
    VIRTUAL_HOSTED_STYLE_REQUEST: bool
    """If virtual hosted style request has to be used."""

class S3Store:
    """
    Configure a connection to Amazon S3 using the specified credentials in the specified
    Amazon region and bucket.

    **Examples**:

    **Using requester-pays buckets**:

    Pass `request_payer=True` as a keyword argument. Or, if you're using
    `S3Store.from_env`, have `AWS_REQUESTER_PAYS=True` set in the environment.

    **Anonymous requests**:

    Pass `skip_signature=True` as a keyword argument. Or, if you're using
    `S3Store.from_env`, have `AWS_SKIP_SIGNATURE=True` set in the environment.
    """

    def __init__(
        self,
        bucket: str,
        *,
        config: S3Config | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[S3Config],
    ) -> None:
        """Create a new S3Store

        Args:
            bucket: The AWS bucket to use.

        Keyword Args:
            config: AWS Configuration. Values in this config will override values inferred from the environment. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            S3Store
        """

    @classmethod
    def from_env(
        cls,
        bucket: str | None = None,
        *,
        config: S3Config | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[S3Config],
    ) -> S3Store:
        """Construct a new S3Store with regular AWS environment variables

        All environment variables starting with `AWS_` will be evaluated. Names must
        match items from `S3ConfigKey`. Only upper-case environment variables are
        accepted.

        Some examples of variables extracted from environment:

        - `AWS_ACCESS_KEY_ID` -> access_key_id
        - `AWS_SECRET_ACCESS_KEY` -> secret_access_key
        - `AWS_DEFAULT_REGION` -> region
        - `AWS_ENDPOINT` -> endpoint
        - `AWS_SESSION_TOKEN` -> token
        - `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` -> <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
        - `AWS_ALLOW_HTTP` -> set to "true" to permit HTTP connections without TLS
        - `AWS_REQUEST_PAYER` -> set to "true" to permit requester-pays connections.

        Args:
            bucket: The AWS bucket to use.

        Keyword Args:
            config: AWS Configuration. Values in this config will override values inferred from the environment. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            S3Store
        """

    @classmethod
    def from_session(
        cls,
        session: boto3.session.Session | botocore.session.Session,
        bucket: str,
        *,
        config: S3Config | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[S3Config],
    ) -> S3Store:
        """Construct a new S3Store with credentials inferred from a boto3 Session

        This can be useful to read S3 credentials from [disk-based credentials sources](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html).

        !!! note
            This is a convenience function for users who are already using `boto3` or
            `botocore`. If you're not already using `boto3` or `botocore`, use other
            constructors, which do not need `boto3` or `botocore` to be installed.

        Examples:

        ```py
        import boto3

        session = boto3.Session()
        store = S3Store.from_session(session, "bucket-name", region="us-east-1")
        ```

        Args:
            session: The boto3.Session or botocore.session.Session to infer credentials from.
            bucket: The AWS bucket to use.

        Keyword Args:
            config: AWS Configuration. Values in this config will override values inferred from the session. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

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
        **kwargs: Unpack[S3Config],
    ) -> S3Store:
        """
        Parse available connection info from a well-known storage URL.

        The supported url schemes are:

        - `s3://<bucket>/<path>`
        - `s3a://<bucket>/<path>`
        - `https://s3.<region>.amazonaws.com/<bucket>`
        - `https://<bucket>.s3.<region>.amazonaws.com`
        - `https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket`

        !!! note
            Note that `from_url` will not use any additional parts of the path as a
            bucket prefix. It will only extract the bucket, region, and endpoint. If you
            wish to use a path prefix, consider wrapping this with `PrefixStore`.

        Args:
            url: well-known storage URL.

        Keyword Args:
            config: AWS Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.


        Returns:
            S3Store
        """

    def __repr__(self) -> str: ...
