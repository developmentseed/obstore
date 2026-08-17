"""An in-process S3 that rejects unsigned requests.

Used by the `RemoteSignedS3Store` tests. Only the subset of the S3 REST API that the
store issues is implemented, and every response is deliberately close to what a real S3
returns — including its `<Error>` documents and its opaque, `+`-bearing upload ids —
because the store's correctness depends on those details.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import timedelta
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import TYPE_CHECKING, ClassVar, NamedTuple
from urllib.parse import parse_qs, quote, unquote, urlsplit

from obstore.store import RemoteSignedS3Store

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from obstore.store import ClientConfig, RetryConfig

DATA = b"abcdefghijklmnopqrstuvwxyz"

LAST_MODIFIED = "Wed, 21 Oct 2015 07:28:00 GMT"
"""Every object reports this mtime, so date preconditions are predictable."""

LAST_MODIFIED_AT = parsedate_to_datetime(LAST_MODIFIED)

STORED_HEADERS = frozenset(
    {
        "cache-control",
        "content-disposition",
        "content-encoding",
        "content-language",
        "content-type",
        "x-amz-storage-class",
    },
)
"""Headers a real S3 stores with the object and echoes back on GET."""


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
    attributes: ClassVar[dict[str, dict[str, str]]] = {}
    """Per-object headers a real S3 would store and echo back on GET."""
    versions: ClassVar[dict[str, dict[str, bytes]]] = {}
    """Superseded object versions, keyed by object key then version id."""
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
        cls.attributes = {}
        cls.versions = {}
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

    def stored_attributes(self, key: str) -> dict[str, str]:
        """Return `key`'s attribute headers, in `reply()`'s underscore form."""
        with FakeS3.lock:
            stored = FakeS3.attributes.get(key, {})
        return {name.replace("-", "_"): value for name, value in stored.items()}

    def precondition_failure(self) -> bool:
        """Answer any date precondition the fixed `Last-Modified` fails to satisfy."""
        since = self.headers.get("if-modified-since")
        if since and parsedate_to_datetime(since) >= LAST_MODIFIED_AT:
            self.reply(304)
            return True
        since = self.headers.get("if-unmodified-since")
        if since and parsedate_to_datetime(since) < LAST_MODIFIED_AT:
            self.error(412, "PreconditionFailed", "Object modified since then")
            return True
        return False

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

        if version := self.query.get("versionId"):
            with FakeS3.lock:
                body = FakeS3.versions.get(self.key, {}).get(version[0])
            if body is None:
                self.error(404, "NoSuchVersion", f"No version {version[0]}")
                return
        else:
            with FakeS3.lock:
                body = FakeS3.objects.get(self.key)
            if body is None:
                self.error(404, "NoSuchKey", "The specified key does not exist")
                return

        if self.precondition_failure():
            return

        common = {
            "Last_Modified": LAST_MODIFIED,
            "ETag": '"etag"',
            **self.stored_attributes(self.key),
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

        if source := self.headers.get("x-amz-copy-source"):
            # A real copy takes the object and its attributes from the source key.
            source_key = unquote(source).removeprefix("/bucket").lstrip("/")
            with FakeS3.lock:
                if source_key not in FakeS3.objects:
                    self.error(404, "NoSuchKey", f"No such source {source_key}")
                    return
                body = FakeS3.objects[source_key]
                attributes = dict(FakeS3.attributes.get(source_key, {}))
        else:
            attributes = {
                name: value
                for name, value in self.headers.items()
                if name.lower() in STORED_HEADERS
                or name.lower().startswith("x-amz-meta-")
            }

        with FakeS3.lock:
            exists = self.key in FakeS3.objects
            if self.headers.get("if-none-match") == "*" and exists:
                self.error(412, "PreconditionFailed", "Object already exists")
                return
            if exists:
                # Keep the superseded bytes addressable by version id.
                previous = FakeS3.versions.setdefault(self.key, {})
                previous[f"v{len(previous)}"] = FakeS3.objects[self.key]
            FakeS3.objects[self.key] = body
            FakeS3.attributes[self.key] = attributes
            version = f"v{len(FakeS3.versions.get(self.key, {}))}"
        self.reply(200, ETag='"etag"', x_amz_version_id=version)

    def do_POST(self) -> None:
        """Initiate or complete a multipart upload."""
        if not self.authorize():
            return
        body = self.read_body()
        query = self.query

        if "uploads" in query:
            # Real S3 upload ids are opaque base64-ish strings, so they routinely
            # contain `+`, `/` and `=`. Using one here keeps the client honest about
            # encoding them: sent raw in a query string, `+` arrives as a space and
            # then addresses no upload at all.
            upload_id = f"up+{len(FakeS3.uploads)}/id=="
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
        if upload_id not in FakeS3.uploads:
            # Answer like S3 rather than raising: an unhandled exception closes the
            # connection, which the client treats as retryable and then backs off for
            # minutes, turning a regression into a hang instead of a failure.
            self.error(404, "NoSuchUpload", f"Unknown upload {upload_id}")
            return
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
