from __future__ import annotations

import json
from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlsplit

import boto3
import pytest
import requests

import obstore
from obstore.exceptions import NotSupportedError
from obstore.store import AzureStore, GCSStore, S3Store

if TYPE_CHECKING:
    from collections.abc import Callable

    from obstore.store import ClientConfig, S3Config

EXPIRES_IN = timedelta(minutes=5)


def test_sign_includes_prefix_in_url() -> None:
    """Signing must apply the store prefix to the path (regression for #682).

    This needs no live server: object_store builds the SigV4 presigned URL
    locally from the static credentials, so we can just inspect the path.
    """
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
        prefix="some/prefix",
    )
    url = str(obstore.sign(store, "GET", "filename.txt", timedelta(seconds=3600)))
    assert "/my-bucket/some/prefix/filename.txt?" in url


def test_sign_roundtrip_with_prefix(
    minio_bucket: tuple[S3Config, ClientConfig],
) -> None:
    """End-to-end: a presigned GET for a prefixed store actually fetches the object.

    Before the fix, put() wrote to ``some/prefix/filename.txt`` but sign() pointed
    at ``filename.txt``, so this GET would 404.
    """
    s3_config, client_options = minio_bucket
    store = S3Store(
        config=s3_config,
        client_options=client_options,
        prefix="some/prefix",
    )

    store.put("filename.txt", b"hello world")

    url = str(obstore.sign(store, "GET", "filename.txt", timedelta(seconds=3600)))
    assert "/some/prefix/filename.txt?" in url

    resp = requests.get(url, timeout=30)
    assert resp.status_code == 200, resp.text
    assert resp.content == b"hello world"


def test_sign_batch_includes_prefix_in_urls() -> None:
    """Signing a list of paths applies the prefix to each (regression for #682).

    Exercises the batch ``signed_urls`` path rather than the single ``signed_url``.
    """
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
        prefix="some/prefix",
    )
    paths = ["one.txt", "nested/two.txt"]
    urls = obstore.sign(store, "GET", paths, timedelta(seconds=3600))
    assert isinstance(urls, list)
    assert len(urls) == len(paths)
    assert "/my-bucket/some/prefix/one.txt?" in str(urls[0])
    assert "/my-bucket/some/prefix/nested/two.txt?" in str(urls[1])


def test_sign_batch_roundtrip_with_prefix(
    minio_bucket: tuple[S3Config, ClientConfig],
) -> None:
    """End-to-end batch: presigned GETs for a prefixed store fetch each object."""
    s3_config, client_options = minio_bucket
    store = S3Store(
        config=s3_config,
        client_options=client_options,
        prefix="some/prefix",
    )

    store.put("one.txt", b"first")
    store.put("nested/two.txt", b"second")

    paths = ["one.txt", "nested/two.txt"]
    urls = obstore.sign(store, "GET", paths, timedelta(seconds=3600))
    assert "/some/prefix/one.txt?" in str(urls[0])
    assert "/some/prefix/nested/two.txt?" in str(urls[1])

    for url, expected in zip(
        [str(u) for u in urls],
        [b"first", b"second"],
        strict=True,
    ):
        resp = requests.get(url, timeout=30)
        assert resp.status_code == 200, resp.text
        assert resp.content == expected


def _offline_s3_store(prefix: str | None = None) -> S3Store:
    return S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
        prefix=prefix,
    )


def _query(url: str) -> dict[str, str]:
    return dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))


def _sign_in_same_second(*calls: Callable[[], str]) -> list[str]:
    """Call each signing function, retrying until all URLs share one X-Amz-Date.

    SigV4 signatures include the signing time, so two URLs can only be compared
    when they were signed within the same second.
    """
    for _ in range(3):
        urls = [call() for call in calls]
        if len({_query(url)["X-Amz-Date"] for url in urls}) == 1:
            return urls
    pytest.fail("could not sign the URLs within one second")


def test_sign_extra_query() -> None:
    store = _offline_s3_store(prefix="some/prefix")
    url_1, url_2 = _sign_in_same_second(
        lambda: obstore.sign(
            store,
            "PUT",
            "big.bin",
            EXPIRES_IN,
            extra_query={"partNumber": "1", "uploadId": "abc"},
        ),
        lambda: obstore.sign(
            store,
            "PUT",
            "big.bin",
            EXPIRES_IN,
            extra_query={"partNumber": "2", "uploadId": "abc"},
        ),
    )
    assert urlsplit(url_1).path == "/my-bucket/some/prefix/big.bin"
    query_1, query_2 = _query(url_1), _query(url_2)
    assert query_1["partNumber"] == "1"
    assert query_1["uploadId"] == "abc"
    assert query_2["partNumber"] == "2"
    assert query_1["X-Amz-SignedHeaders"] == "host"
    assert query_1["X-Amz-Signature"] != query_2["X-Amz-Signature"]


def test_sign_signed_headers() -> None:
    store = _offline_s3_store()
    plain_url, header_url = _sign_in_same_second(
        lambda: obstore.sign(store, "PUT", "file.txt", EXPIRES_IN),
        lambda: obstore.sign(
            store,
            "PUT",
            "file.txt",
            EXPIRES_IN,
            signed_headers={"Content-Type": "text/plain"},
        ),
    )
    plain_query, header_query = _query(plain_url), _query(header_url)
    assert plain_query["X-Amz-SignedHeaders"] == "host"
    assert header_query["X-Amz-SignedHeaders"] == "content-type;host"
    assert plain_query["X-Amz-Signature"] != header_query["X-Amz-Signature"]


def test_sign_batch_applies_options_to_each_path() -> None:
    store = _offline_s3_store(prefix="some/prefix")
    paths = ["one.txt", "nested/two.txt"]
    urls = obstore.sign(
        store,
        "GET",
        paths,
        EXPIRES_IN,
        extra_query={"response-content-disposition": "attachment"},
        signed_headers={"content-type": "text/plain"},
    )
    assert len(urls) == len(paths)
    for url, path in zip(urls, paths, strict=True):
        assert urlsplit(url).path == f"/my-bucket/some/prefix/{path}"
        query = _query(url)
        assert query["response-content-disposition"] == "attachment"
        assert query["X-Amz-SignedHeaders"] == "content-type;host"


@pytest.mark.asyncio
async def test_sign_async_options() -> None:
    store = _offline_s3_store(prefix="some/prefix")
    extra_query = {"partNumber": "1", "uploadId": "abc"}
    signed_headers = {"content-type": "text/plain"}
    for _ in range(3):
        sync_url = obstore.sign(
            store,
            "PUT",
            "big.bin",
            EXPIRES_IN,
            extra_query=extra_query,
            signed_headers=signed_headers,
        )
        async_url = await obstore.sign_async(
            store,
            "PUT",
            "big.bin",
            EXPIRES_IN,
            extra_query=extra_query,
            signed_headers=signed_headers,
        )
        if _query(sync_url)["X-Amz-Date"] == _query(async_url)["X-Amz-Date"]:
            assert async_url == sync_url
            break
    else:
        pytest.fail("could not sign the URLs within one second")

    async_urls = await obstore.sign_async(
        store,
        "PUT",
        ["one.bin", "two.bin"],
        EXPIRES_IN,
        extra_query=extra_query,
    )
    assert [_query(url)["partNumber"] for url in async_urls] == ["1", "1"]


@pytest.mark.parametrize(
    ("signed_headers", "match"),
    [
        ({"content type": "text/plain"}, "Invalid signed header name"),
        ({"content-type": "text/plain\n"}, "Invalid value for signed header"),
    ],
)
def test_sign_invalid_signed_headers(signed_headers: dict[str, str], match: str):
    store = _offline_s3_store()
    with pytest.raises(ValueError, match=match):
        obstore.sign(
            store,
            "PUT",
            "file.txt",
            EXPIRES_IN,
            signed_headers=signed_headers,
        )


def test_sign_gcs_options() -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    # GCS signs locally only with a service account private key. Any key works,
    # because nothing verifies the signature here.
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    service_account_key = {
        "private_key": private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode(),
        "private_key_id": "test",
        "client_email": "test@example.iam.gserviceaccount.com",
    }
    store = GCSStore(
        "my-bucket",
        service_account_key=json.dumps(service_account_key),
        prefix="some/prefix",
    )
    url = obstore.sign(
        store,
        "GET",
        "report.csv",
        EXPIRES_IN,
        extra_query={"response-content-disposition": "attachment"},
        signed_headers={"x-goog-meta-owner": "me"},
    )
    assert urlsplit(url).path == "/my-bucket/some/prefix/report.csv"
    query = _query(url)
    assert query["response-content-disposition"] == "attachment"
    assert query["X-Goog-SignedHeaders"] == "host;x-goog-meta-owner"


def _offline_azure_store() -> AzureStore:
    return AzureStore(
        "container",
        account_name="account",
        account_key="dGVzdA==",
    )


@pytest.mark.parametrize(
    ("extra_query", "signed_headers"),
    [
        ({"comp": "block"}, None),
        (None, {"content-type": "text/plain"}),
    ],
)
def test_sign_azure_options_not_supported(
    extra_query: dict[str, str] | None,
    signed_headers: dict[str, str] | None,
):
    store = _offline_azure_store()
    with pytest.raises(NotSupportedError):
        obstore.sign(
            store,
            "PUT",
            "file.txt",
            EXPIRES_IN,
            extra_query=extra_query,
            signed_headers=signed_headers,
        )


def test_sign_azure_empty_options() -> None:
    store = _offline_azure_store()
    url = obstore.sign(
        store,
        "GET",
        "file.txt",
        EXPIRES_IN,
        extra_query={},
        signed_headers={},
    )
    assert urlsplit(url).path == "/container/file.txt"
    assert "sig" in _query(url)


def test_sign_presigned_multipart_upload(
    minio_bucket: tuple[S3Config, ClientConfig],
) -> None:
    """Presign each part of a multipart upload and upload it with a plain HTTP client.

    Creating and completing the upload needs credentials and happens on the server
    (here with boto3). The parts are uploaded through presigned URLs that carry
    `partNumber` and `uploadId` (#778).
    """
    s3_config, client_options = minio_bucket
    store = S3Store(
        config=s3_config,
        client_options=client_options,
        prefix="uploads",
    )
    bucket = s3_config.get("bucket")
    assert bucket is not None
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_config.get("endpoint"),
        aws_access_key_id=s3_config.get("access_key_id"),
        aws_secret_access_key=s3_config.get("secret_access_key"),
        region_name="us-east-1",
    )
    key = "uploads/big.bin"
    upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]

    # Every part but the last must be at least 5 MiB.
    parts = [b"a" * (5 * 1024 * 1024), b"b" * 1024]
    urls = [
        obstore.sign(
            store,
            "PUT",
            "big.bin",
            EXPIRES_IN,
            extra_query={"partNumber": str(part_number), "uploadId": upload_id},
        )
        for part_number in range(1, len(parts) + 1)
    ]

    # partNumber is covered by the signature, so the recipient cannot change it.
    tampered = urls[0].replace("partNumber=1", "partNumber=3")
    resp = requests.put(tampered, data=b"x", timeout=30)
    assert resp.status_code == 403, resp.text

    etags = []
    for url, data in zip(urls, parts, strict=True):
        resp = requests.put(url, data=data, timeout=30)
        assert resp.status_code == 200, resp.text
        etags.append(resp.headers["ETag"])

    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"ETag": etag, "PartNumber": part_number}
                for part_number, etag in enumerate(etags, 1)
            ],
        },
    )
    assert store.get("big.bin").bytes() == b"".join(parts)


def test_sign_signed_header_is_enforced(minio_store: S3Store) -> None:
    url = obstore.sign(
        minio_store,
        "PUT",
        "file.txt",
        EXPIRES_IN,
        signed_headers={"content-type": "text/plain"},
    )

    resp = requests.put(
        url,
        data=b"hello",
        headers={"content-type": "application/json"},
        timeout=30,
    )
    assert resp.status_code == 403, resp.text

    resp = requests.put(
        url,
        data=b"hello",
        headers={"content-type": "text/plain"},
        timeout=30,
    )
    assert resp.status_code == 200, resp.text
    assert minio_store.get("file.txt").attributes["Content-Type"] == "text/plain"


def test_sign_get_response_headers(minio_store: S3Store) -> None:
    """A presigned GET can set the response's Content-Disposition (#746)."""
    minio_store.put("report.csv", b"a,b\n1,2\n")
    disposition = 'attachment; filename="report.csv"'
    url = obstore.sign(
        minio_store,
        "GET",
        "report.csv",
        EXPIRES_IN,
        extra_query={
            "response-content-disposition": disposition,
            "response-content-type": "text/csv",
        },
    )

    resp = requests.get(url, timeout=30)
    assert resp.status_code == 200, resp.text
    assert resp.headers["Content-Disposition"] == disposition
    assert resp.headers["Content-Type"] == "text/csv"
    assert resp.content == b"a,b\n1,2\n"
