"""Tests for `RemoteSignedS3Store` against an in-process, signing-aware S3 server."""

from __future__ import annotations

import asyncio
import pickle
import re
from contextlib import contextmanager
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import TYPE_CHECKING, ClassVar, NamedTuple
from urllib.parse import parse_qs, quote, unquote, urlsplit

import pytest
import requests

import obstore as obs
from obstore.exceptions import AlreadyExistsError, GenericError, PermissionDeniedError
from obstore.store import RemoteSignedS3Store

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from obstore.store import ClientConfig, RetryConfig

DATA = b"abcdefghijklmnopqrstuvwxyz"


class Request(NamedTuple):
    """One request that reached the server, recorded for assertions."""

    method: str
    path: str
    headers: dict[str, str]


CLIENT_OPTIONS: ClientConfig = {"allow_http": True}
"""The test server speaks plain HTTP, which `allow_http` must opt into."""

NO_WAIT_RETRIES: RetryConfig = {
    "max_retries": 3,
    "backoff": {"init_backoff": timedelta(0), "max_backoff": timedelta(0), "base": 1},
}
"""Retry immediately, so retry tests do not spend real time sleeping."""


class FakeS3(BaseHTTPRequestHandler):
    """A minimal S3 that rejects unsigned requests.

    Only the subset of the S3 REST API that `RemoteSignedS3Store` issues is implemented.
    State is class-level because `ThreadingHTTPServer` builds a handler per request.
    """

    lock: ClassVar[Lock] = Lock()
    objects: ClassVar[dict[str, bytes]] = {}
    uploads: ClassVar[dict[str, dict[int, bytes]]] = {}
    page_size: ClassVar[int] = 1000
    fail_next: ClassVar[int] = 0
    """Number of upcoming requests to fail with a retryable 503."""
    signed_requests: ClassVar[list[Request]] = []
    """Every request that passed signature checking, in arrival order."""

    @classmethod
    def reset(cls) -> None:
        """Forget all state from a previous test."""
        cls.objects = {}
        cls.uploads = {}
        cls.page_size = 1000
        cls.fail_next = 0
        cls.signed_requests = []

    # ---- request plumbing ----

    @property
    def key(self) -> str:
        """The object key addressed by this request, with the bucket stripped."""
        path = unquote(urlsplit(self.path).path)
        return path.removeprefix("/bucket").lstrip("/")

    @property
    def query(self) -> dict[str, list[str]]:
        """The parsed query string, keeping valueless keys such as `?uploads`."""
        return parse_qs(urlsplit(self.path).query, keep_blank_values=True)

    def read_body(self) -> bytes:
        """Read exactly the body the client announced."""
        return self.rfile.read(int(self.headers.get("Content-Length", 0)))

    def error(self, status: int, code: str, message: str) -> None:
        """Reply with an S3 `<Error>` document, as a real S3 would."""
        body = (
            f'<?xml version="1.0"?><Error><Code>{code}</Code>'
            f"<Message>{message}</Message></Error>"
        ).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def reply(self, status: int, body: bytes = b"", **headers: str) -> None:
        """Reply with `body`, omitting it for a HEAD as HTTP requires."""
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name.replace("_", "-"), value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def authorize(self) -> bool:
        """Reject any request the signer did not sign, and fail injected requests."""
        if self.headers.get("x-signed") != "yes":
            self.error(403, "AccessDenied", "Request was not signed")
            return False
        with FakeS3.lock:
            FakeS3.signed_requests.append(
                Request(self.command, self.path, dict(self.headers.items())),
            )
            if FakeS3.fail_next:
                FakeS3.fail_next -= 1
                self.error(503, "SlowDown", "Please retry")
                return False
        return True

    # ---- verbs ----

    def do_GET(self) -> None:
        """Serve a list, or a whole or ranged object."""
        if not self.authorize():
            return
        if self.query.get("list-type") == ["2"]:
            self.serve_list()
            return

        with FakeS3.lock:
            body = FakeS3.objects.get(self.key)
        if body is None:
            self.error(404, "NoSuchKey", "The specified key does not exist")
            return

        common = {
            "Last_Modified": "Wed, 21 Oct 2015 07:28:00 GMT",
            "ETag": '"etag"',
        }
        if range_header := self.headers.get("range"):
            start, _, end = range_header.removeprefix("bytes=").partition("-")
            start, end = int(start), int(end) + 1
            self.reply(
                206,
                body[start:end],
                Content_Range=f"bytes {start}-{end - 1}/{len(body)}",
                **common,
            )
        else:
            self.reply(200, body, **common)

    def do_HEAD(self) -> None:
        """Serve object metadata."""
        self.do_GET()

    def do_PUT(self) -> None:
        """Store a whole object or one part of a multipart upload."""
        if not self.authorize():
            return
        body = self.read_body()
        query = self.query

        if upload_id := query.get("uploadId"):
            part_number = int(query["partNumber"][0])
            with FakeS3.lock:
                FakeS3.uploads[upload_id[0]][part_number] = body
            self.reply(200, ETag=f'"part-{part_number}"')
            return

        with FakeS3.lock:
            exists = self.key in FakeS3.objects
            if self.headers.get("if-none-match") == "*" and exists:
                self.error(412, "PreconditionFailed", "Object already exists")
                return
            FakeS3.objects[self.key] = body
        self.reply(200, ETag='"etag"')

    def do_POST(self) -> None:
        """Initiate or complete a multipart upload."""
        if not self.authorize():
            return
        body = self.read_body()
        query = self.query

        if "uploads" in query:
            upload_id = f"upload-{len(FakeS3.uploads)}"
            with FakeS3.lock:
                FakeS3.uploads[upload_id] = {}
            self.reply(
                200,
                (
                    '<?xml version="1.0"?><InitiateMultipartUploadResult>'
                    f"<UploadId>{upload_id}</UploadId>"
                    "</InitiateMultipartUploadResult>"
                ).encode(),
            )
            return

        upload_id = query["uploadId"][0]
        # Assemble the object from the parts the client listed, in the order it listed
        # them, so that a wrongly ordered CompleteMultipartUpload produces wrong bytes.
        part_numbers = [
            int(match) for match in re.findall(r"<PartNumber>(\d+)<", body.decode())
        ]
        with FakeS3.lock:
            parts = FakeS3.uploads.pop(upload_id)
            FakeS3.objects[self.key] = b"".join(
                parts[number] for number in part_numbers
            )
        self.reply(
            200,
            (
                b'<?xml version="1.0"?><CompleteMultipartUploadResult>'
                b"<ETag>&quot;multipart-etag&quot;</ETag>"
                b"</CompleteMultipartUploadResult>"
            ),
        )

    def do_DELETE(self) -> None:
        """Delete an object or abort a multipart upload."""
        if not self.authorize():
            return
        with FakeS3.lock:
            if upload_id := self.query.get("uploadId"):
                FakeS3.uploads.pop(upload_id[0], None)
            else:
                FakeS3.objects.pop(self.key, None)
        self.reply(204)

    def serve_list(self) -> None:
        """Serve one page of a `ListObjectsV2` response."""
        query = self.query
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [None])[0]
        start_after = query.get("continuation-token", query.get("start-after", [""]))[0]

        with FakeS3.lock:
            keys = sorted(key for key in FakeS3.objects if key.startswith(prefix))
        keys = [key for key in keys if key > start_after]

        contents, prefixes = [], []
        for key in keys:
            tail = key[len(prefix) :]
            if delimiter and delimiter in tail:
                prefixes.append(prefix + tail.split(delimiter)[0] + delimiter)
            else:
                contents.append(key)

        truncated = len(contents) > FakeS3.page_size
        page = contents[: FakeS3.page_size]
        body = ['<?xml version="1.0"?><ListBucketResult>']
        for key in page:
            with FakeS3.lock:
                size = len(FakeS3.objects[key])
            body.append(
                f"<Contents><Key>{quote(key)}</Key>"
                "<LastModified>2015-10-21T07:28:00.000Z</LastModified>"
                f"<Size>{size}</Size><ETag>&quot;etag&quot;</ETag></Contents>",
            )
        body.extend(
            f"<CommonPrefixes><Prefix>{quote(value)}</Prefix></CommonPrefixes>"
            for value in dict.fromkeys(prefixes)
        )
        if truncated:
            body.append(f"<NextContinuationToken>{page[-1]}</NextContinuationToken>")
        body.append("</ListBucketResult>")
        self.reply(200, "".join(body).encode())

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Keep the test server quiet."""


@contextmanager
def signed_server() -> Iterator[str]:
    """Run `FakeS3` on a free port and yield its bucket URL."""
    FakeS3.reset()
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeS3)
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/bucket"
    finally:
        server.shutdown()
        thread.join()


def signer(
    _method: str,
    uri: str,
    headers: dict[str, str],
) -> tuple[str, dict[str, str]]:
    """Sign every request by stamping it with the header `FakeS3` demands."""
    return uri, {**headers, "x-signed": "yes"}


def make_store(
    url: str,
    *,
    virtual_hosted_style_request: bool = False,
    retry_config: RetryConfig | None = None,
) -> RemoteSignedS3Store:
    """Build a store against the test server."""
    return RemoteSignedS3Store(
        url,
        signer,
        virtual_hosted_style_request=virtual_hosted_style_request,
        client_options=CLIENT_OPTIONS,
        retry_config=retry_config,
    )


def select(method: str, query_key: str) -> list[Request]:
    """Return the signed requests using `method` whose query mentions `query_key`."""
    return [
        request
        for request in FakeS3.signed_requests
        if request.method == method and query_key in urlsplit(request.path).query
    ]


def one(requests: Iterable[Request]) -> Request:
    """Return the single matching request, asserting that there is exactly one."""
    matched = list(requests)
    assert len(matched) == 1, f"expected exactly one request, got {len(matched)}"
    return matched[0]


@pytest.mark.asyncio
async def test_signed_range_request() -> None:
    with signed_server() as url:
        response = await asyncio.to_thread(requests.get, f"{url}/chunk", timeout=1)
        assert response.status_code == 403

        calls: list[tuple[str, str, dict[str, str]]] = []

        async def recording_signer(method: str, uri: str, headers: dict[str, str]):
            calls.append((method, uri, headers))
            return signer(method, uri, headers)

        store = RemoteSignedS3Store(
            url,
            recording_signer,
            client_options=CLIENT_OPTIONS,
        )
        FakeS3.objects["chunk"] = DATA

        assert await store.get_range_async("chunk", start=5, end=10) == DATA[5:10]
        assert (await store.head_async("chunk"))["size"] == len(DATA)

    assert calls == [
        ("GET", f"{url}/chunk", {"range": "bytes=5-9"}),
        ("HEAD", f"{url}/chunk", {}),
    ]


def test_unsigned_request_is_rejected_with_server_message() -> None:
    with signed_server() as url:
        store = RemoteSignedS3Store(
            url,
            lambda _method, uri, headers: (uri, headers),
            client_options=CLIENT_OPTIONS,
        )
        with pytest.raises(
            PermissionDeniedError,
            match="AccessDenied: Request was not signed",
        ):
            store.get("chunk").bytes()


def test_put_and_get_roundtrip() -> None:
    with signed_server() as url:
        store = make_store(url)
        store.put("chunk", DATA)
        assert store.get("chunk").bytes() == DATA


def test_get_missing_object_raises_not_found() -> None:
    with signed_server() as url:
        store = make_store(url)
        with pytest.raises(FileNotFoundError, match="NoSuchKey"):
            store.get("missing").bytes()


def test_key_with_reserved_characters_is_encoded() -> None:
    """A `?` in a key must be percent-encoded, not read as the start of a query."""
    with signed_server() as url:
        store = make_store(url)
        store.put("a b?c#d/chunk", DATA)
        assert store.get("a b?c#d/chunk").bytes() == DATA
        assert [meta["path"] for meta in store.list().collect()] == ["a b?c#d/chunk"]


def test_signed_list_paginates() -> None:
    with signed_server() as url:
        FakeS3.page_size = 2
        store = make_store(f"{url}/prefix")
        for index in range(5):
            store.put(f"chunk-{index}", DATA)

        objects = store.list().collect()

    assert [meta["path"] for meta in objects] == [
        f"chunk-{index}" for index in range(5)
    ]
    assert objects[0]["size"] == len(DATA)
    list_requests = [
        request.path
        for request in FakeS3.signed_requests
        if "list-type" in request.path
    ]
    assert len(list_requests) == 3, "expected three pages"
    assert "prefix=prefix%2F" in list_requests[0]


def test_list_prefix_matches_on_segment_boundaries() -> None:
    """Listing `a/b` must not also return `a/bc/...`.

    The listed prefix is sent with a trailing delimiter for exactly this reason.
    """
    with signed_server() as url:
        store = make_store(f"{url}/root")
        store.put("a/b/inside", DATA)
        store.put("a/bc/outside", DATA)

        assert [meta["path"] for meta in store.list("a/b").collect()] == ["a/b/inside"]

    assert FakeS3.objects.keys() == {"root/a/b/inside", "root/a/bc/outside"}


def test_list_with_delimiter_returns_common_prefixes() -> None:
    with signed_server() as url:
        store = make_store(f"{url}/prefix")
        store.put("top", DATA)
        store.put("nested/inner", DATA)

        result = store.list_with_delimiter()

    assert [meta["path"] for meta in result["objects"]] == ["top"]
    assert [str(prefix) for prefix in result["common_prefixes"]] == ["nested"]


def test_conditional_create_conflict_raises_already_exists() -> None:
    with signed_server() as url:
        store = make_store(url)
        store.put("chunk", DATA, mode="create")
        with pytest.raises(AlreadyExistsError):
            store.put("chunk", DATA, mode="create")


def test_delete_removes_object() -> None:
    with signed_server() as url:
        store = make_store(url)
        store.put("chunk", DATA)
        store.delete("chunk")
        assert store.list().collect() == []


def test_copy_addresses_the_source_by_bucket_and_key() -> None:
    with signed_server() as url:
        store = make_store(f"{url}/prefix")
        store.put("source", DATA)
        store.copy("source", "target")

    copy = one(
        request
        for request in FakeS3.signed_requests
        if "x-amz-copy-source" in request.headers
    )
    assert urlsplit(copy.path).path == "/bucket/prefix/target"
    assert copy.headers["x-amz-copy-source"] == "/bucket/prefix/source"


@pytest.mark.parametrize(
    ("location", "endpoint", "vhost", "expected_url", "expected_prefix"),
    [
        (
            "s3://warehouse/zarr/array",
            "https://s3.eu-west-1.amazonaws.com",
            False,
            "https://s3.eu-west-1.amazonaws.com/warehouse/zarr/array",
            "zarr/array",
        ),
        # A bucket-only location has no prefix.
        (
            "s3://warehouse",
            "http://minio:9000",
            False,
            "http://minio:9000/warehouse/",
            None,
        ),
        # `s3a://` is accepted, as Hadoop-style catalogs emit it.
        (
            "s3a://warehouse/zarr",
            "http://minio:9000/",
            False,
            "http://minio:9000/warehouse/zarr",
            "zarr",
        ),
        # Virtual-hosted style puts the bucket in the host instead.
        (
            "s3://warehouse/zarr",
            "https://s3.eu-west-1.amazonaws.com",
            True,
            "https://warehouse.s3.eu-west-1.amazonaws.com/zarr",
            "zarr",
        ),
    ],
)
def test_from_s3_url_resolves_the_location_against_the_endpoint(
    location: str,
    endpoint: str,
    vhost: bool,  # noqa: FBT001
    expected_url: str,
    expected_prefix: str | None,
) -> None:
    store = RemoteSignedS3Store.from_s3_url(
        location,
        signer,
        endpoint=endpoint,
        virtual_hosted_style_request=vhost,
    )
    assert store.url == expected_url
    assert store.bucket == "warehouse"
    assert store.prefix == expected_prefix


def test_from_s3_url_rejects_a_non_s3_location() -> None:
    with pytest.raises(ValueError, match="Expected an s3:// or s3a:// URL"):
        RemoteSignedS3Store.from_s3_url(
            "https://s3.example.com/warehouse",
            signer,
            endpoint="https://s3.example.com",
        )


def test_from_s3_url_rejects_an_endpoint_with_a_path() -> None:
    """A path on the endpoint would make the bucket/prefix split ambiguous."""
    with pytest.raises(ValueError, match="endpoint must not include a path"):
        RemoteSignedS3Store.from_s3_url(
            "s3://warehouse/zarr",
            signer,
            endpoint="https://gateway.example.com/s3",
        )


def test_from_s3_url_rejects_a_non_http_endpoint() -> None:
    with pytest.raises(ValueError, match="endpoint must be an http"):
        RemoteSignedS3Store.from_s3_url(
            "s3://warehouse/zarr",
            signer,
            endpoint="s3://warehouse",
        )


def test_from_s3_url_reads_and_writes() -> None:
    """A store from an `s3://` location addresses the same keys as the plain one."""
    with signed_server() as url:
        origin, _, bucket = url.rpartition("/")
        store = RemoteSignedS3Store.from_s3_url(
            f"s3://{bucket}/prefix",
            signer,
            endpoint=origin,
            client_options=CLIENT_OPTIONS,
        )
        store.put("chunk", DATA)
        assert store.get("chunk").bytes() == DATA
        assert [meta["path"] for meta in store.list().collect()] == ["chunk"]

    assert FakeS3.objects == {"prefix/chunk": DATA}


def test_from_s3_url_store_is_picklable() -> None:
    with signed_server() as url:
        origin, _, bucket = url.rpartition("/")
        store = RemoteSignedS3Store.from_s3_url(
            f"s3://{bucket}/prefix",
            signer,
            endpoint=origin,
            client_options=CLIENT_OPTIONS,
        )
        store.put("chunk", DATA)

        restored = pickle.loads(pickle.dumps(store))
        assert restored == store
        assert restored.get("chunk").bytes() == DATA


def test_virtual_hosted_style_takes_the_bucket_from_the_host() -> None:
    """`x-amz-copy-source` names the bucket even when the request URL never does."""
    store = RemoteSignedS3Store(
        "https://mybucket.s3.eu-west-1.amazonaws.com/prefix",
        signer,
        virtual_hosted_style_request=True,
    )
    assert store.bucket == "mybucket"
    assert store.prefix == "prefix"


def test_multipart_upload_roundtrip() -> None:
    """Every part is uploaded and completed through independently signed requests."""
    payload = bytes(range(256)) * 40  # 10 KiB
    with signed_server() as url:
        store = make_store(url)
        obs.put(
            store,
            "big",
            payload,
            use_multipart=True,
            chunk_size=4096,
            max_concurrency=4,
        )

        assert store.get("big").bytes() == payload

    assert len(select("POST", "uploads")) == 1
    assert len(select("PUT", "partNumber")) == 3, (
        "10 KiB in 4 KiB chunks is three parts"
    )
    assert len(select("POST", "uploadId")) == 1


@pytest.mark.asyncio
async def test_multipart_upload_async() -> None:
    payload = bytes(range(256)) * 40
    with signed_server() as url:
        store = make_store(url)
        await obs.put_async(store, "big", payload, use_multipart=True, chunk_size=4096)
        assert (await store.get_async("big")).bytes() == payload


def test_multipart_upload_of_empty_object() -> None:
    """A completion with no buffered parts still has to produce a readable object."""
    with signed_server() as url:
        store = make_store(url)
        with obs.open_writer(store, "empty", buffer_size=4096) as writer:
            del writer  # Closed without any data written.
        assert store.get("empty").bytes() == b""


def test_multipart_abort_discards_upload() -> None:
    """A failure mid-upload must abort, so no parts are left behind on the bucket."""

    def failing_chunks() -> Iterator[bytes]:
        yield b"x" * 8192
        msg = "input went away"
        raise RuntimeError(msg)

    with signed_server() as url:
        store = make_store(url)
        with pytest.raises(RuntimeError, match="input went away"):
            obs.put(
                store,
                "aborted",
                failing_chunks(),
                use_multipart=True,
                chunk_size=4096,
            )

        assert FakeS3.uploads == {}, "the upload was aborted, not left dangling"
        assert "aborted" not in FakeS3.objects

    assert len(select("DELETE", "uploadId")) == 1


def test_retry_resigns_each_attempt() -> None:
    """A retried request must be signed again, never reuse the previous signature."""
    with signed_server() as url:
        signed: list[str] = []

        def counting_signer(method: str, uri: str, headers: dict[str, str]):
            signed.append(uri)
            return signer(method, uri, headers)

        store = RemoteSignedS3Store(
            url,
            counting_signer,
            client_options=CLIENT_OPTIONS,
            retry_config=NO_WAIT_RETRIES,
        )
        FakeS3.objects["chunk"] = DATA
        FakeS3.fail_next = 2

        assert store.get("chunk").bytes() == DATA

    assert len(signed) == 3, "two failed attempts plus the successful one"


def test_retries_exhausted_surfaces_server_error() -> None:
    with signed_server() as url:
        store = make_store(url, retry_config={**NO_WAIT_RETRIES, "max_retries": 1})
        FakeS3.objects["chunk"] = DATA
        FakeS3.fail_next = 5

        with pytest.raises(GenericError, match="SlowDown"):
            store.get("chunk").bytes()


def test_attributes_and_tags_are_sent_as_signed_headers() -> None:
    with signed_server() as url:
        store = make_store(url)
        obs.put(
            store,
            "chunk",
            DATA,
            attributes={"Content-Type": "application/json", "Content-Encoding": "gzip"},
            tags={"project": "zarr"},
        )

    put = one(request for request in FakeS3.signed_requests if request.method == "PUT")
    assert put.headers["content-type"] == "application/json"
    assert put.headers["content-encoding"] == "gzip"
    assert put.headers["x-amz-tagging"] == "project=zarr"


def test_pickle_roundtrip() -> None:
    """Zarr with dask or multiprocessing pickles the store, so it must survive that."""
    with signed_server() as url:
        store = make_store(url)
        store.put("chunk", DATA)

        restored = pickle.loads(pickle.dumps(store))
        assert restored == store
        assert restored.get("chunk").bytes() == DATA


def test_virtual_hosted_style_lists_from_bucket_root() -> None:
    """With the bucket in the host, every path segment is part of the key prefix."""
    with signed_server() as url:
        # The bucket lives in the host, so the URL carries only the key prefix. In path
        # style `prefix` would have been mistaken for the bucket and dropped from keys.
        origin = url.removesuffix("/bucket")
        store = make_store(f"{origin}/prefix", virtual_hosted_style_request=True)
        store.put("chunk", DATA)

        assert [meta["path"] for meta in store.list().collect()] == ["chunk"]
    assert FakeS3.objects == {"prefix/chunk": DATA}


def test_async_signer_with_sync_method_errors() -> None:
    """A coroutine signer cannot be awaited from the synchronous methods."""
    with signed_server() as url:

        async def async_signer(method: str, uri: str, headers: dict[str, str]):
            return signer(method, uri, headers)

        store = RemoteSignedS3Store(
            url,
            async_signer,
            client_options=CLIENT_OPTIONS,
        )
        with pytest.raises(GenericError, match="signer callback failed"):
            store.get("chunk").bytes()


def test_http_url_requires_allow_http() -> None:
    with signed_server() as url:
        store = RemoteSignedS3Store(url, signer)
        with pytest.raises(GenericError):
            store.put("chunk", DATA)
