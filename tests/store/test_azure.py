from __future__ import annotations

import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlsplit

import pytest

from obstore.exceptions import BaseError, UnauthenticatedError
from obstore.store import AzureStore

if TYPE_CHECKING:
    from collections.abc import Iterator

    from obstore.store import AzureSASToken


def test_overlapping_config_keys():
    with pytest.raises(BaseError, match="Duplicate key"):
        AzureStore(container_name="test", AZURE_CONTAINER_NAME="test")  # type: ignore intentional test

    with pytest.raises(BaseError, match="Duplicate key"):
        AzureStore(
            config={"azure_container_name": "test", "AZURE_CONTAINER_NAME": "test"},  # type: ignore (intentional test)
        )


def test_eq():
    store = AzureStore(
        "container",
        account_name="account_name",
        client_options={"timeout": "10s"},
    )
    store2 = AzureStore(
        "container",
        account_name="account_name",
        client_options={"timeout": "10s"},
    )
    store3 = AzureStore(
        "container",
        account_name="account_name",
    )
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3


def test_from_url():
    # https://github.com/developmentseed/obstore/issues/477
    url = "https://overturemapswestus2.blob.core.windows.net/release"
    store = AzureStore.from_url(url, skip_signature=True)

    assert store.config.get("container_name") == "release"
    assert store.config.get("account_name") == "overturemapswestus2"
    assert store.prefix is None


@contextmanager
def _recording_server() -> Iterator[tuple[str, list[str]]]:
    """Serve 404 to every HEAD request on localhost, recording each request path."""
    paths: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:
            paths.append(self.path)
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", paths
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _sas_token_store(endpoint: str, sas_token: str | list[tuple[str, str]]):
    def credential_provider() -> AzureSASToken:
        return {"sas_token": sas_token, "expires_at": None}

    return AzureStore(
        "container",
        account_name="account",
        endpoint=endpoint,
        client_options={"allow_http": True},
        credential_provider=credential_provider,
    )


@pytest.mark.parametrize(
    "sas_token",
    [
        "?sv=2021-10-04&sp=r&sig=abc%2Bdef%3D",
        "sv=2021-10-04&sp=r&sig=abc+def=",
        [("sv", "2021-10-04"), ("sp", "r"), ("sig", "abc+def=")],
    ],
)
def test_credential_provider_sas_token(sas_token: str | list[tuple[str, str]]):
    with _recording_server() as (endpoint, paths):
        store = _sas_token_store(endpoint, sas_token)
        with pytest.raises(FileNotFoundError):
            store.head("file.txt")

    assert len(paths) == 1
    url = urlsplit(paths[0])
    assert url.path == "/container/file.txt"
    assert parse_qsl(url.query, strict_parsing=True) == [
        ("sv", "2021-10-04"),
        ("sp", "r"),
        ("sig", "abc+def="),
    ]


@pytest.mark.parametrize(
    ("sas_token", "match"),
    [
        ("sv=2021-10-04&sig=%FF", "Failed parsing an SAS key"),
        ("sv=2021-10-04&bogus", "Missing component in SAS query pair"),
    ],
)
def test_credential_provider_invalid_sas_token(sas_token: str, match: str):
    with _recording_server() as (endpoint, paths):
        store = _sas_token_store(endpoint, sas_token)
        with pytest.raises(UnauthenticatedError, match=match):
            store.head("file.txt")

    assert paths == []
