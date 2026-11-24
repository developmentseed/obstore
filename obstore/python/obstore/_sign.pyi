from collections.abc import Sequence
from datetime import timedelta
from typing import Literal, TypeAlias, overload

from .store import AzureStore, GCSStore, S3Store

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
def sign(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str,
    expires_in: timedelta,
    *,
    extra_query: dict[str, str] | None = None,
    signed_headers: dict[str, str] | None = None,
) -> str: ...
@overload
def sign(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: Sequence[str],
    expires_in: timedelta,
    *,
    extra_query: dict[str, str] | None = None,
    signed_headers: dict[str, str] | None = None,
) -> Sequence[str]: ...
def sign(  # type: ignore[misc] # docstring in pyi file
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
    *,
    extra_query: dict[str, str] | None = None,
    signed_headers: dict[str, str] | None = None,
) -> str | Sequence[str]:
    """Create a signed URL.

    Given the intended `method` and `paths` to use and the desired length of time for
    which the URL should be valid, return a signed URL created with the object store
    implementation's credentials such that the URL can be handed to something that
    doesn't have access to the object store's credentials, to allow limited access to
    the object store.

    `extra_query` and `signed_headers` add query parameters and request headers to the
    signature. A request made with the URL is only accepted if it sends exactly those
    query parameters and headers, with those values. For example, to let a client
    without credentials upload one part of an S3 multipart upload:

    ```py
    url = obstore.sign(
        store,
        "PUT",
        "big.bin",
        timedelta(hours=1),
        extra_query={"partNumber": "1", "uploadId": upload_id},
    )
    ```

    These are supported by [`S3Store`][obstore.store.S3Store] and
    [`GCSStore`][obstore.store.GCSStore].
    [`AzureStore`][obstore.store.AzureStore] raises
    [`NotSupportedError`][obstore.exceptions.NotSupportedError] if either is non-empty.

    When `paths` is a sequence, the same `extra_query` and `signed_headers` are signed
    into the URL for every path.

    Args:
        store: The ObjectStore instance to use.
        method: The HTTP method to use.
        paths: The path(s) within ObjectStore to sign. If this is a sequence of paths, a
            sequence of signed URLs is returned, in the same order.
        expires_in: How long the signed URL(s) should be valid.

    Keyword Args:
        extra_query: Query parameters to sign and append to the URL. For example,
            `partNumber` and `uploadId` for an S3 multipart `UploadPart` request, or
            `response-content-disposition` and `response-content-type` to set those
            headers on the response to a presigned `GET`. Defaults to `None`.
        signed_headers: Request headers to bind to the signature, such as
            `{"content-type": "text/plain"}`. The request must send each of these
            headers with exactly this value. Defaults to `None`.

    Returns:
        The signed URL, or a sequence of signed URLs if `paths` is a sequence.

    """

@overload
async def sign_async(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str,
    expires_in: timedelta,
    *,
    extra_query: dict[str, str] | None = None,
    signed_headers: dict[str, str] | None = None,
) -> str: ...
@overload
async def sign_async(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: Sequence[str],
    expires_in: timedelta,
    *,
    extra_query: dict[str, str] | None = None,
    signed_headers: dict[str, str] | None = None,
) -> Sequence[str]: ...
async def sign_async(  # type: ignore[misc] # docstring in pyi file
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
    *,
    extra_query: dict[str, str] | None = None,
    signed_headers: dict[str, str] | None = None,
) -> str | Sequence[str]:
    """Call `sign` asynchronously.

    Refer to the documentation for [sign][obstore.sign].
    """
