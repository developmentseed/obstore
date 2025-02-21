"""Credential providers for Amazon S3."""

from __future__ import annotations

from typing import TYPE_CHECKING, NotRequired, TypedDict, Unpack

import boto3
import boto3.session
import botocore.credentials

if TYPE_CHECKING:
    from collections.abc import Sequence

    import botocore.session

    from obstore.store import S3ConfigInput, S3Credential


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
    """A CredentialProvider for S3Store that uses [`boto3`][]."""

    credentials: botocore.credentials.Credentials
    config: S3ConfigInput

    def __init__(
        self,
        session: boto3.session.Session | botocore.session.Session | None = None,
        *,
        ttl: ...,
        # https://github.com/boto/botocore/blob/8d851f1ed7e7b73b1c56dd6ea18d17eeb0331277/botocore/credentials.py#L619-L631
    ) -> None:
        """Create a new Boto3CredentialProvider."""
        if session is None:
            session = boto3.Session()

        self.config = {}
        if isinstance(session, boto3.Session) and session.region_name is not None:
            self.config["region"] = session.region_name

        credentials = session.get_credentials()
        if credentials is None:
            raise ValueError("Received None from session.get_credentials")

        self.credentials = credentials

    def __call__(self) -> S3Credential:
        """Fetch credentials."""
        frozen_credentials = self.credentials.get_frozen_credentials()
        return {
            "access_key_id": frozen_credentials.access_key,
            "secret_access_key": frozen_credentials.secret_key,
            "token": frozen_credentials.token,
            # TODO: pass a ttl here
            "expires_at": None,
        }


class StsCredentialProvider:
    """A CredentialProvider for S3Store that uses [`STS.Client.assume_role`][]."""

    def __init__(
        self,
        session: boto3.session.Session | None = None,
        **kwargs: Unpack[AssumeRoleRequestRequestTypeDef],
    ) -> None:
        """Create a new Boto3CredentialProvider."""
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
            "expires_at": creds["Expiration"],
        }
