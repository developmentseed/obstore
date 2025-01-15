from typing import Dict, TypedDict, Unpack

import boto3
import boto3.session
import botocore
import botocore.session

from ._client import ClientConfigKey
from ._retry import RetryConfig

# Note: we removed `bucket` because it overlaps with an existing named arg in the
# constructors
class S3Config(TypedDict, total=False):
    access_key_id: str
    aws_access_key_id: str
    aws_allow_http: str
    aws_bucket_name: str
    aws_bucket: str
    aws_checksum_algorithm: str
    aws_conditional_put: str
    aws_container_credentials_relative_uri: str
    aws_copy_if_not_exists: str
    aws_default_region: str
    aws_disable_tagging: str
    aws_endpoint_url: str
    aws_endpoint: str
    aws_imdsv1_fallback: str
    aws_metadata_endpoint: str
    aws_region: str
    aws_request_payer: str
    aws_s3_express: str
    aws_secret_access_key: str
    aws_server_side_encryption: str
    aws_session_token: str
    aws_skip_signature: str
    aws_sse_bucket_key_enabled: str
    aws_sse_kms_key_id: str
    aws_token: str
    aws_unsigned_payload: str
    aws_virtual_hosted_style_request: str
    bucket_name: str
    checksum_algorithm: str
    conditional_put: str
    copy_if_not_exists: str
    default_region: str
    disable_tagging: str
    endpoint_url: str
    endpoint: str
    imdsv1_fallback: str
    metadata_endpoint: str
    region: str
    request_payer: str
    s3_express: str
    secret_access_key: str
    session_token: str
    skip_signature: str
    token: str
    unsigned_payload: str
    virtual_hosted_style_request: str
    ACCESS_KEY_ID: str
    AWS_ACCESS_KEY_ID: str
    AWS_ALLOW_HTTP: str
    AWS_BUCKET_NAME: str
    AWS_BUCKET: str
    AWS_CHECKSUM_ALGORITHM: str
    AWS_CONDITIONAL_PUT: str
    AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: str
    AWS_COPY_IF_NOT_EXISTS: str
    AWS_DEFAULT_REGION: str
    AWS_DISABLE_TAGGING: str
    AWS_ENDPOINT_URL: str
    AWS_ENDPOINT: str
    AWS_IMDSV1_FALLBACK: str
    AWS_METADATA_ENDPOINT: str
    AWS_REGION: str
    AWS_REQUEST_PAYER: str
    AWS_S3_EXPRESS: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SERVER_SIDE_ENCRYPTION: str
    AWS_SESSION_TOKEN: str
    AWS_SKIP_SIGNATURE: str
    AWS_SSE_BUCKET_KEY_ENABLED: str
    AWS_SSE_KMS_KEY_ID: str
    AWS_TOKEN: str
    AWS_UNSIGNED_PAYLOAD: str
    AWS_VIRTUAL_HOSTED_STYLE_REQUEST: str
    BUCKET_NAME: str
    BUCKET: str
    CHECKSUM_ALGORITHM: str
    CONDITIONAL_PUT: str
    COPY_IF_NOT_EXISTS: str
    DEFAULT_REGION: str
    DISABLE_TAGGING: str
    ENDPOINT_URL: str
    ENDPOINT: str
    IMDSV1_FALLBACK: str
    METADATA_ENDPOINT: str
    REGION: str
    REQUEST_PAYER: str
    S3_EXPRESS: str
    SECRET_ACCESS_KEY: str
    SESSION_TOKEN: str
    SKIP_SIGNATURE: str
    TOKEN: str
    UNSIGNED_PAYLOAD: str
    VIRTUAL_HOSTED_STYLE_REQUEST: str

"""Valid AWS S3 configuration keys.

Either lower case or upper case strings are accepted.

- `aws_access_key_id`, `access_key_id`: AWS Access Key
- `aws_secret_access_key`, `secret_access_key`: Secret Access Key
- `aws_region`, `region`: Region
- `aws_request_payer`, `request_payer`: if `True`, enable operations on requester-pays buckets.
- `aws_default_region`, `default_region`: Default region
- `aws_bucket`, `aws_bucket_name`, `bucket`, `bucket_name`: Bucket name
- `aws_endpoint`, `aws_endpoint_url`, `endpoint`, `endpoint_url`: Sets custom endpoint for communicating with AWS S3.
- `aws_session_token`, `aws_token`, `session_token`, `token`: Token to use for requests (passed to underlying provider)
- `aws_imdsv1_fallback`, `imdsv1_fallback`: Fall back to ImdsV1
- `aws_virtual_hosted_style_request`, `virtual_hosted_style_request`: If virtual hosted style request has to be used
- `aws_unsigned_payload`, `unsigned_payload`: Avoid computing payload checksum when calculating signature.
- `aws_metadata_endpoint`, `metadata_endpoint`: Set the instance metadata endpoint
- `aws_disable_tagging`, `disable_tagging`: Disable tagging objects. This can be desirable if not supported by the backing store.
- `aws_s3_express`, `s3_express`: Enable Support for S3 Express One Zone
"""

class S3Store:
    """
    Configure a connection to Amazon S3 using the specified credentials in the specified
    Amazon region and bucket.

    **Examples**:

    **Using requester-pays buckets**:

    Include `'AWS_REQUESTER_PAYS': True` as an element in the `config`. Or, if you're
    using `S3Store.from_env`, have `AWS_REQUESTER_PAYS=True` set in the environment.
    """

    def __init__(
        self,
        bucket: str,
        *,
        config: S3Config | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
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
    def anon(
        cls,
        bucket: str,
        *,
        config: S3Config | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
        **kwargs: Unpack[S3Config],
    ) -> S3Store:
        """Construct a new anonymous S3Store.

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
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
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
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store:
        """Construct a new S3Store with credentials inferred from a boto3 Session

        !!! note
            This is a convenience function for users who are already using `boto3` or
            `botocore`. If you're not already using `boto3` or `botocore`, use other
            classmethods, which do not need `boto3` or `botocore` to be installed.

        Examples:

        ```py
        import boto3

        session = boto3.Session()
        store = S3Store.from_session(
            session,
            "bucket-name",
            config={"AWS_REGION": "us-east-1"},
        )
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
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
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
