from __future__ import annotations

import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import ClassVar
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from obstore.store import RemoteSignedS3Store

DATA = b"abcdefghijklmnopqrstuvwxyz"


class SignedS3Handler(BaseHTTPRequestHandler):
    """Reject unsigned requests and serve one in-memory object."""

    requests: ClassVar[list[dict[str, str]]] = []

    def do_GET(self) -> None:
        """Serve a complete or ranged signed object request."""
        if self.headers.get("x-signed") != "yes":
            self.send_response(403)
            self.end_headers()
            return

        query = parse_qs(urlsplit(self.path).query)
        if query.get("list-type") == ["2"]:
            self.serve_list(query.get("prefix", [""])[0])
            return

        SignedS3Handler.requests.append(
            {key: self.headers.get(key) for key in ("Range", "X-Signed")},
        )
        start, end = 0, len(DATA)
        if range_header := self.headers.get("range"):
            start, end = map(int, range_header.removeprefix("bytes=").split("-"))
            end += 1
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{len(DATA)}")
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(end - start))
        self.send_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
        self.send_header("ETag", '"etag"')
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(DATA[start:end])

    def serve_list(self, prefix: str) -> None:
        """Serve a minimal ListObjectsV2 response for one object."""
        body = (
            '<?xml version="1.0"?>'
            "<ListBucketResult><Contents>"
            f"<Key>{prefix}chunk</Key>"
            "<LastModified>2015-10-21T07:28:00.000Z</LastModified>"
            f"<Size>{len(DATA)}</Size><ETag>&quot;etag&quot;</ETag>"
            "</Contents></ListBucketResult>"
        ).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self) -> None:
        """Serve a signed object metadata request."""
        self.do_GET()

    def log_message(self, _format: str, *_args: object) -> None:
        """Keep the test server quiet."""


@contextmanager
def signed_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), SignedS3Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/bucket"
    finally:
        server.shutdown()
        thread.join()


@pytest.mark.asyncio
async def test_signed_range_request():
    SignedS3Handler.requests.clear()
    with signed_server() as url:
        response = await asyncio.to_thread(requests.get, f"{url}/chunk", timeout=1)
        assert response.status_code == 403

        calls: list[tuple[str, str, dict[str, str]]] = []

        async def signer(method: str, uri: str, headers: dict[str, str]):
            calls.append((method, uri, headers))
            return uri, {**headers, "x-signed": "yes"}

        store = RemoteSignedS3Store(url, signer)
        assert await store.get_range_async("chunk", start=5, end=10) == DATA[5:10]
        assert (await store.head_async("chunk"))["size"] == len(DATA)

    assert calls == [
        ("GET", f"{url}/chunk", {"range": "bytes=5-9"}),
        ("HEAD", f"{url}/chunk", {}),
    ]
    assert SignedS3Handler.requests == [
        {"Range": "bytes=5-9", "X-Signed": "yes"},
        {"Range": None, "X-Signed": "yes"},
    ]


def test_signed_list():
    with signed_server() as url:
        calls: list[str] = []

        def signer(_method: str, uri: str, headers: dict[str, str]):
            calls.append(uri)
            return uri, {**headers, "x-signed": "yes"}

        store = RemoteSignedS3Store(f"{url}/prefix", signer)
        objects = store.list().collect()

    assert [meta["path"] for meta in objects] == ["chunk"]
    assert objects[0]["size"] == len(DATA)
    assert calls == [f"{url}/?list-type=2&prefix=prefix%2F"]
