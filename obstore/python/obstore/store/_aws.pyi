from typing import Dict, Literal

import boto3
import botocore
import botocore.session

from ._client import ClientConfigKey
from ._retry import RetryConfig

S3ConfigKey = Literal[
    "access_key_id",
    "aws_access_key_id",
    "aws_allow_http",
    "aws_bucket_name",
    "aws_bucket",
    "aws_checksum_algorithm",
    "aws_conditional_put",
    "aws_container_credentials_relative_uri",
    "aws_copy_if_not_exists",
    "aws_default_region",
    "aws_disable_tagging",
    "aws_endpoint_url",
    "aws_endpoint",
    "aws_imdsv1_fallback",
    "aws_metadata_endpoint",
    "aws_region",
    "aws_s3_express",
    "aws_secret_access_key",
    "aws_server_side_encryption",
    "aws_session_token",
    "aws_skip_signature",
    "aws_sse_bucket_key_enabled",
    "aws_sse_kms_key_id",
    "aws_token",
    "aws_unsigned_payload",
    "aws_virtual_hosted_style_request",
    "bucket_name",
    "bucket",
    "checksum_algorithm",
    "conditional_put",
    "copy_if_not_exists",
    "default_region",
    "disable_tagging",
    "endpoint_url",
    "endpoint",
    "imdsv1_fallback",
    "metadata_endpoint",
    "region",
    "s3_express",
    "secret_access_key",
    "session_token",
    "skip_signature",
    "token",
    "unsigned_payload",
    "virtual_hosted_style_request",
    "ACCESS_KEY_ID",
    "AWS_ACCESS_KEY_ID",
    "AWS_ALLOW_HTTP",
    "AWS_BUCKET_NAME",
    "AWS_BUCKET",
    "AWS_CHECKSUM_ALGORITHM",
    "AWS_CONDITIONAL_PUT",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_COPY_IF_NOT_EXISTS",
    "AWS_DEFAULT_REGION",
    "AWS_DISABLE_TAGGING",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT",
    "AWS_IMDSV1_FALLBACK",
    "AWS_METADATA_ENDPOINT",
    "AWS_REGION",
    "AWS_S3_EXPRESS",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SERVER_SIDE_ENCRYPTION",
    "AWS_SESSION_TOKEN",
    "AWS_SKIP_SIGNATURE",
    "AWS_SSE_BUCKET_KEY_ENABLED",
    "AWS_SSE_KMS_KEY_ID",
    "AWS_TOKEN",
    "AWS_UNSIGNED_PAYLOAD",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
    "BUCKET_NAME",
    "BUCKET",
    "CHECKSUM_ALGORITHM",
    "CONDITIONAL_PUT",
    "COPY_IF_NOT_EXISTS",
    "DEFAULT_REGION",
    "DISABLE_TAGGING",
    "ENDPOINT_URL",
    "ENDPOINT",
    "IMDSV1_FALLBACK",
    "METADATA_ENDPOINT",
    "REGION",
    "S3_EXPRESS",
    "SECRET_ACCESS_KEY",
    "SESSION_TOKEN",
    "SKIP_SIGNATURE",
    "TOKEN",
    "UNSIGNED_PAYLOAD",
    "VIRTUAL_HOSTED_STYLE_REQUEST",
]
"""Valid AWS S3 configuration keys.

Either lower case or upper case strings are accepted.

- `aws_access_key_id`, `access_key_id`: AWS Access Key
- `aws_secret_access_key`, `secret_access_key`: Secret Access Key
- `aws_region`, `region`: Region
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
    """

    @classmethod
    def from_env(
        cls,
        bucket: str,
        *,
        config: Dict[S3ConfigKey | str, str] | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store:
        """Construct a new S3Store with regular AWS environment variables

        Variables extracted from environment:

        - `AWS_ACCESS_KEY_ID` -> access_key_id
        - `AWS_SECRET_ACCESS_KEY` -> secret_access_key
        - `AWS_DEFAULT_REGION` -> region
        - `AWS_ENDPOINT` -> endpoint
        - `AWS_SESSION_TOKEN` -> token
        - `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` -> <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
        - `AWS_ALLOW_HTTP` -> set to "true" to permit HTTP connections without TLS

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
        session: boto3.Session | botocore.session.Session,
        bucket: str,
        *,
        config: Dict[S3ConfigKey | str, str] | None = None,
        client_options: Dict[ClientConfigKey, str | bool] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store:
        """Construct a new S3Store with credentials inferred from a boto3 Session

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
        config: Dict[S3ConfigKey | str, str] | None = None,
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
