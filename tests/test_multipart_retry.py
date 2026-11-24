from __future__ import annotations

import threading
from collections import Counter
from contextlib import contextmanager
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

import pytest

import obstore as obs
from obstore.exceptions import GenericError, PermissionDeniedError
from obstore.store import MemoryStore, S3Store

if TYPE_CHECKING:
    from collections.abc import Iterator

    from obstore.store import RetryConfig

MULTIPART_RETRY: RetryConfig = {
    "max_retries": 2,
    "backoff": {
        "init_backoff": timedelta(milliseconds=1),
        "max_backoff": timedelta(milliseconds=1),
    },
}

DATA = b"0123456789"
"""Three parts with `chunk_size=4`."""


class _MockS3:
    """S3 multipart endpoints that fail the first attempts at uploading each part."""

    def __init__(self, part_failures: int, failure_status: int) -> None:
        self.part_failures = part_failures
        self.failure_status = failure_status
        self.part_attempts: Counter[int] = Counter()
        self.operations: list[str] = []
        self.lock = threading.Lock()

    def respond(
        self,
        method: str,
        query: dict[str, list[str]],
    ) -> tuple[int, dict[str, str], bytes]:
        with self.lock:
            if "partNumber" in query:
                part = int(query["partNumber"][0])
                self.part_attempts[part] += 1
                if self.part_attempts[part] <= self.part_failures:
                    return self.failure_status, {}, b""
                return 200, {"ETag": f'"part-{part}"'}, b""
            if method == "POST" and "uploads" in query:
                self.operations.append("initiate")
                body = (
                    b"<InitiateMultipartUploadResult><UploadId>upload-id</UploadId>"
                    b"</InitiateMultipartUploadResult>"
                )
                return 200, {}, body
            if method == "POST" and "uploadId" in query:
                self.operations.append("complete")
                body = (
                    b"<CompleteMultipartUploadResult><ETag>object-etag</ETag>"
                    b"</CompleteMultipartUploadResult>"
                )
                return 200, {}, body
            if method == "DELETE" and "uploadId" in query:
                self.operations.append("abort")
                return 204, {}, b""
            self.operations.append(f"unexpected {method}")
            return 400, {}, b""


@contextmanager
def mock_s3(
    part_failures: int,
    failure_status: int = 503,
) -> Iterator[tuple[S3Store, _MockS3]]:
    """Serve a _MockS3 and yield an S3Store that does not retry requests itself."""
    mock = _MockS3(part_failures, failure_status)

    class Handler(BaseHTTPRequestHandler):
        def do_DELETE(self) -> None:
            self.handle_s3_request()

        def do_POST(self) -> None:
            self.handle_s3_request()

        def do_PUT(self) -> None:
            self.handle_s3_request()

        def handle_s3_request(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            status, headers, body = mock.respond(self.command, query)
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        store = S3Store(
            "test-bucket",
            endpoint=f"http://127.0.0.1:{server.server_port}",
            region="us-east-1",
            skip_signature=True,
            client_options={"allow_http": True},
            retry_config={"max_retries": 0},
        )
        yield store, mock
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_failed_part_raises_without_multipart_retry():
    with (
        mock_s3(part_failures=1) as (store, mock),
        pytest.raises(GenericError, match="503"),
    ):
        store.put("file", DATA[:4], use_multipart=True)

    assert mock.part_attempts == {1: 1}
    assert mock.operations == ["initiate", "abort"]


def test_multipart_retry_retries_failed_parts():
    with mock_s3(part_failures=1) as (store, mock):
        result = store.put(
            "file",
            DATA,
            use_multipart=True,
            chunk_size=4,
            multipart_retry=MULTIPART_RETRY,
        )

    assert result["e_tag"] == "object-etag"
    assert mock.part_attempts == {1: 2, 2: 2, 3: 2}
    assert mock.operations == ["initiate", "complete"]


@pytest.mark.asyncio
async def test_multipart_retry_retries_failed_parts_async():
    with mock_s3(part_failures=1) as (store, mock):
        result = await obs.put_async(
            store,
            "file",
            DATA,
            use_multipart=True,
            chunk_size=4,
            multipart_retry=MULTIPART_RETRY,
        )

    assert result["e_tag"] == "object-etag"
    assert mock.part_attempts == {1: 2, 2: 2, 3: 2}
    assert mock.operations == ["initiate", "complete"]


def test_multipart_retry_stops_after_max_retries():
    with (
        mock_s3(part_failures=100) as (store, mock),
        pytest.raises(GenericError, match="503"),
    ):
        store.put("file", DATA[:4], use_multipart=True, multipart_retry=MULTIPART_RETRY)

    assert mock.part_attempts == {1: 3}
    assert mock.operations == ["initiate", "abort"]


def test_multipart_retry_does_not_retry_client_errors():
    with (
        mock_s3(part_failures=1, failure_status=403) as (store, mock),
        pytest.raises(PermissionDeniedError),
    ):
        store.put("file", DATA[:4], use_multipart=True, multipart_retry=MULTIPART_RETRY)

    assert mock.part_attempts == {1: 1}
    assert mock.operations == ["initiate", "abort"]


@pytest.mark.parametrize("use_multipart", [True, False])
def test_multipart_retry_ignored_by_other_stores(use_multipart: bool):  # noqa: FBT001
    store = MemoryStore()
    store.put("file", DATA, use_multipart=use_multipart, multipart_retry={})
    assert store.get("file").bytes() == DATA


def test_writer_multipart_retry():
    with (
        mock_s3(part_failures=1) as (store, mock),
        obs.open_writer(
            store,
            "file",
            buffer_size=4,
            multipart_retry=MULTIPART_RETRY,
        ) as writer,
    ):
        writer.write(DATA)

    assert mock.part_attempts == {1: 2, 2: 2, 3: 2}
    assert mock.operations == ["initiate", "complete"]
