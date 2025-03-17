import sys
from collections.abc import Sequence
from datetime import timedelta
from typing import Literal, overload

from .store import AzureStore, GCSStore, S3Store

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

HTTP_METHOD: TypeAlias = Literal[
    "GET",
    "PUT",
    "POST",
    "HEAD",
    "PATCH",
    "TRACE",
    "DELETE",
    "OPTIONS",
    "CONNECT",
]
"""Allowed HTTP Methods for signing."""

SignCapableStore: TypeAlias = AzureStore | GCSStore | S3Store
"""ObjectStore instances that are capable of signing."""

@overload
def sign(  # type: ignore # noqa: PGH003
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str,
    expires_in: timedelta,
) -> str: ...
@overload
def sign(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: Sequence[str],
    expires_in: timedelta,
) -> list[str]: ...
def sign(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
) -> str | list[str]:
    """Create a signed URL.

    Given the intended `method` and `paths` to use and the desired length of time for
    which the URL should be valid, return a signed URL created with the object store
    implementation's credentials such that the URL can be handed to something that
    doesn't have access to the object store's credentials, to allow limited access to
    the object store.

    Args:
        store: The ObjectStore instance to use.
        method: The HTTP method to use.
        paths: The path(s) within ObjectStore to retrieve. If
        expires_in: How long the signed URL(s) should be valid.

    Returns:
        _description_

    """

@overload
async def sign_async(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str,
    expires_in: timedelta,
) -> str: ...
@overload
async def sign_async(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: Sequence[str],
    expires_in: timedelta,
) -> list[str]: ...
async def sign_async(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
) -> str | list[str]:
    """Call `sign` asynchronously.

    Refer to the documentation for [sign][obstore.sign].
    """
