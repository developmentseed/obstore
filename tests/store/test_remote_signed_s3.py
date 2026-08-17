"""Tests for `RemoteSignedS3Store`, against the signing-aware server in `fake_s3`."""

from __future__ import annotations

import asyncio
import pickle
from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import pytest
import requests

import obstore as obs
from obstore.exceptions import (
    AlreadyExistsError,
    GenericError,
    NotModifiedError,
    PermissionDeniedError,
    PreconditionError,
)
from obstore.store import RemoteSignedS3Store

from .fake_s3 import (
    CLIENT_OPTIONS,
    DATA,
    LAST_MODIFIED_AT,
    NO_WAIT_RETRIES,
    FakeS3,
    make_store,
    one,
    select,
    signed_server,
    signer,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


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


@pytest.mark.parametrize(
    ("encoded", "decoded"),
    [
        ("pre%20fix", "pre fix"),
        ("caf%C3%A9", "café"),
        ("a%23b", "a#b"),
        ("p%25c", "p%c"),
    ],
)
def test_url_prefix_is_percent_decoded(encoded: str, decoded: str) -> None:
    """A URL can only express these prefixes encoded; keys must come out decoded.

    Taking the segment literally would double-encode it and address a different object.
    """
    with signed_server() as url:
        store = make_store(f"{url}/{encoded}")
        assert store.prefix == decoded
        store.put("chunk", DATA)
        assert store.get("chunk").bytes() == DATA
        assert [meta["path"] for meta in store.list().collect()] == ["chunk"]

    assert FakeS3.objects.keys() == {f"{decoded}/chunk"}


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


def test_get_with_version_reads_the_superseded_object() -> None:
    """`GetOptions.version` must reach S3 as `?versionId=`, not be silently ignored."""
    with signed_server() as url:
        store = make_store(url)
        first = store.put("chunk", b"first")
        store.put("chunk", b"second")

        assert store.get("chunk").bytes() == b"second"
        version = first["version"]
        assert version is not None, "PUT should report x-amz-version-id"
        assert store.get("chunk", options={"version": version}).bytes() == b"first"

    assert "versionId=" in one(select("GET", "versionId")).path


def test_get_with_unknown_version_is_not_found() -> None:
    with signed_server() as url:
        store = make_store(url)
        store.put("chunk", DATA)
        with pytest.raises(FileNotFoundError, match="NoSuchVersion"):
            store.get("chunk", options={"version": "v99"}).bytes()


def test_if_modified_since_is_sent_as_an_http_date() -> None:
    """The store must format the datetime as an HTTP date the server can parse."""
    after = LAST_MODIFIED_AT + timedelta(days=1)
    with signed_server() as url:
        store = make_store(url)
        store.put("chunk", DATA)
        with pytest.raises(NotModifiedError):
            store.get("chunk", options={"if_modified_since": after}).bytes()

    sent = one(request for request in FakeS3.signed_requests if request.method == "GET")
    assert sent.headers["if-modified-since"] == "Thu, 22 Oct 2015 07:28:00 GMT"


def test_if_unmodified_since_rejects_a_newer_object() -> None:
    before = LAST_MODIFIED_AT - timedelta(days=1)
    with signed_server() as url:
        store = make_store(url)
        store.put("chunk", DATA)
        with pytest.raises(PreconditionError, match="PreconditionFailed"):
            store.get("chunk", options={"if_unmodified_since": before}).bytes()


def test_attributes_survive_a_put_and_get_roundtrip() -> None:
    """What `put` stores as headers, `get` must recover as attributes."""
    sent = {
        "Content-Type": "application/json",
        "Content-Encoding": "gzip",
        "Cache-Control": "max-age=60",
    }
    with signed_server() as url:
        store = make_store(url)
        obs.put(store, "chunk", DATA, attributes=sent)

        got = dict(store.get("chunk").attributes)

    assert {key: got[key] for key in sent} == sent


def test_user_metadata_survives_a_roundtrip() -> None:
    with signed_server() as url:
        store = make_store(url)
        obs.put(store, "chunk", DATA, attributes={"owner": "nathan"})

        assert dict(store.get("chunk").attributes)["owner"] == "nathan"

    put = one(request for request in FakeS3.signed_requests if request.method == "PUT")
    assert put.headers["x-amz-meta-owner"] == "nathan"


def test_list_with_offset_is_pushed_down_to_start_after() -> None:
    """The offset must reach S3, not be applied client-side after listing all keys."""
    with signed_server() as url:
        store = make_store(f"{url}/root")
        for index in range(5):
            store.put(f"c-{index}", DATA)

        got = [meta["path"] for meta in store.list(offset="c-2").collect()]

    assert got == ["c-3", "c-4"]
    assert "start-after=root%2Fc-2" in one(select("GET", "start-after")).path


def test_copy_copies_the_bytes() -> None:
    with signed_server() as url:
        store = make_store(url)
        store.put("source", b"payload")
        store.copy("source", "target")

        assert store.get("target").bytes() == b"payload"


def test_copy_if_not_exists_refuses_to_overwrite() -> None:
    with signed_server() as url:
        store = make_store(url)
        store.put("source", b"payload")
        store.put("target", b"existing")

        with pytest.raises(AlreadyExistsError):
            obs.copy(store, "source", "target", overwrite=False)

        assert store.get("target").bytes() == b"existing"
    assert (
        one(
            request
            for request in FakeS3.signed_requests
            if "x-amz-copy-source" in request.headers
        ).headers["if-none-match"]
        == "*"
    )


def test_rename_moves_the_object() -> None:
    """`rename` has no S3 primitive; it must fall back to copy-then-delete."""
    with signed_server() as url:
        store = make_store(url)
        store.put("source", b"payload")
        store.rename("source", "target")

        assert store.get("target").bytes() == b"payload"
        assert [meta["path"] for meta in store.list().collect()] == ["target"]


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
