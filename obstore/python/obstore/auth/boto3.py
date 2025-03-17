"""Credential providers for Amazon S3."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, TypedDict

import boto3
import boto3.session
import botocore.credentials

if TYPE_CHECKING:
    import sys
    from collections.abc import Sequence

    import botocore.session

    from obstore.store import S3Config, S3Credential

    if sys.version_info >= (3, 11):
        from typing import NotRequired, Unpack
    else:
        from typing_extensions import NotRequired, Unpack


class PolicyDescriptorTypeTypeDef(TypedDict):  # noqa: D101
    arn: NotRequired[str]


class TagTypeDef(TypedDict):  # noqa: D101
    Key: str
    Value: str


class ProvidedContextTypeDef(TypedDict):  # noqa: D101
    ProviderArn: NotRequired[str]
    ContextAssertion: NotRequired[str]


# Note: this is vendored from types-boto3-sts
class AssumeRoleRequestRequestTypeDef(TypedDict):  # noqa: D101
    RoleArn: str
    RoleSessionName: str
    PolicyArns: NotRequired[Sequence[PolicyDescriptorTypeTypeDef]]
    Policy: NotRequired[str]
    DurationSeconds: NotRequired[int]
    Tags: NotRequired[Sequence[TagTypeDef]]
    TransitiveTagKeys: NotRequired[Sequence[str]]
    ExternalId: NotRequired[str]
    SerialNumber: NotRequired[str]
    TokenCode: NotRequired[str]
    SourceIdentity: NotRequired[str]
    ProvidedContexts: NotRequired[Sequence[ProvidedContextTypeDef]]


# TODO: should these two classes be merged?
class Boto3CredentialProvider:
    """A CredentialProvider for [S3Store][obstore.store.S3Store] that uses [`boto3.session.Session`][].

    If the provided session has a `region_name` set, that will be passed down to the
    store.
    """  # noqa: E501

    credentials: botocore.credentials.Credentials
    config: S3Config
    ttl: timedelta

    def __init__(
        self,
        session: boto3.session.Session | botocore.session.Session | None = None,
        *,
        # https://github.com/boto/botocore/blob/8d851f1ed7e7b73b1c56dd6ea18d17eeb0331277/botocore/credentials.py#L619-L631
        ttl: timedelta = timedelta(minutes=30),
    ) -> None:
        """Create a new Boto3CredentialProvider.

        This will call `session.get_credentials` to get a
        `botocore.credentials.Credentials` object. Each token refresh will call
        `credentials.get_frozen_credentials`.

        Args:
            session: A boto3 session to use for providing credentials. Defaults to None,
                in which case a new `boto3.Session` will be used.

        Keyword Args:
            ttl: The length of time each result from `get_frozen_credentials` should
                live. Defaults to timedelta(minutes=30).

        """
        if session is None:
            session = boto3.Session()

        self.config = {}
        if isinstance(session, boto3.Session) and session.region_name is not None:
            self.config["region"] = session.region_name

        credentials = session.get_credentials()
        if credentials is None:
            raise ValueError("Received None from session.get_credentials")

        self.credentials = credentials
        self.ttl = ttl

    def __call__(self) -> S3Credential:
        """Fetch credentials."""
        expires_at = datetime.now(timezone.utc) + self.ttl
        frozen_credentials = self.credentials.get_frozen_credentials()
        return {
            "access_key_id": frozen_credentials.access_key,
            "secret_access_key": frozen_credentials.secret_key,
            "token": frozen_credentials.token,
            "expires_at": expires_at,
        }


class StsCredentialProvider:
    """A CredentialProvider for [S3Store][obstore.store.S3Store] that uses [`STS.Client.assume_role`][].

    If the provided session has a `region_name` set, that will be passed down to the
    store.
    """  # noqa: E501

    config: S3Config
    session: boto3.session.Session

    def __init__(
        self,
        session: boto3.session.Session | None = None,
        **kwargs: Unpack[AssumeRoleRequestRequestTypeDef],
    ) -> None:
        """Create a new StsCredentialProvider.

        Args:
            session: A boto3 session to use for providing credentials. Defaults to None,
                in which case a new `boto3.Session` will be used.

        Keyword Args:
            kwargs: arguments passed on to [`STS.Client.assume_role`][].

        """
        if session is None:
            session = boto3.Session()

        self.config = {}
        if isinstance(session, boto3.Session) and session.region_name is not None:
            self.config["region"] = session.region_name

        self.session = session
        self.kwargs = kwargs

    def __call__(self) -> S3Credential:
        """Fetch credentials."""
        client = self.session.client("sts")

        sts_response = client.assume_role(**self.kwargs)
        creds = sts_response["Credentials"]

        expiry = creds["Expiration"]

        if expiry.tzinfo is None:
            msg = "expiration time in STS response did not contain timezone information"
            raise ValueError(msg)

        return {
            "access_key_id": creds["AccessKeyId"],
            "secret_access_key": creds["SecretAccessKey"],
            "token": creds["SessionToken"],
            "expires_at": expiry,
        }
