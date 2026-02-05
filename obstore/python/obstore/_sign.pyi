from collections.abc import Sequence
from datetime import timedelta
from typing import overload

from ._sign_types import HTTP_METHOD, SignCapableStore

@overload
def sign(
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
) -> Sequence[str]: ...
def sign(  # type: ignore[misc] # docstring in pyi file
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
) -> str | Sequence[str]:
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
        Signed URL

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
) -> Sequence[str]: ...
async def sign_async(  # type: ignore[misc] # docstring in pyi file
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
) -> str | Sequence[str]:
    """Call `sign` asynchronously.

    Refer to the documentation for [sign][obstore.sign].
    """
