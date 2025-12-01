"""Aiohttp client implementation."""

# ruff: noqa: PLR2004

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from aiohttp import ClientSession
from multidict import MultiDict

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Iterable

    from aiohttp import ClientResponse

    from obstore.store import ClientConfig, ClientFactory, HttpRequest, HttpResponse


class AiohttpClientFactory:
    """A client factory for Aiohttp."""

    def connect(self, options: ClientConfig) -> _AiohttpService:  # noqa: ARG002
        """Create a new HTTP Client."""
        return _AiohttpService(ClientSession())


class _AiohttpService:
    session: ClientSession

    def __init__(self, session: ClientSession) -> None:
        self.session = session

    async def __call__(self, req: HttpRequest) -> HttpResponse:
        method = req["method"]
        url = req["uri"].geturl()

        headers: MultiDict[str] = MultiDict()
        for header_name, header_value in req["headers"]:
            # TODO: it seems like aiohttp only allows string header values?
            headers.add(header_name, str(header_value))

        async with self.session.request(method, url, headers=headers) as resp:
            version = _get_http_version_from_response(resp)
            return _AiohttpResponse(
                status=resp.status,
                version=version,
                headers=resp.headers.items(),
                body=resp.content,
            )


def _get_http_version_from_response(
    resp: ClientResponse,
) -> Literal["0.9", "1.0", "1.1", "2.0", "3.0"]:
    v = resp.version
    if v is not None:
        if v.major == 0 and v.minor == 9:
            return "0.9"
        if v.major == 1 and v.minor == 0:
            return "1.0"
        if v.major == 1 and v.minor == 1:
            return "1.1"
        if v.major == 2 and v.minor == 0:
            return "2.0"
        if v.major == 3 and v.minor == 0:
            return "3.0"

    return "1.1"


@dataclass
class _AiohttpResponse:
    status: int
    version: Literal["0.9", "1.0", "1.1", "2.0", "3.0"]
    headers: Iterable[tuple[str, str | bytes]]
    body: AsyncIterable


if TYPE_CHECKING:
    # Just for testing
    def _accepts_factory(factory: ClientFactory) -> None:
        pass

    aiohttp_factory = AiohttpClientFactory()
    _accepts_factory(aiohttp_factory)
