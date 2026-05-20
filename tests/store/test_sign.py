from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import requests

import obstore
from obstore.store import S3Store

if TYPE_CHECKING:
    from obstore.store import ClientConfig, S3Config


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

    obstore.put(store, "filename.txt", b"hello world")

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

    obstore.put(store, "one.txt", b"first")
    obstore.put(store, "nested/two.txt", b"second")

    paths = ["one.txt", "nested/two.txt"]
    urls = obstore.sign(store, "GET", paths, timedelta(seconds=3600))
    assert "/some/prefix/one.txt?" in str(urls[0])
    assert "/some/prefix/nested/two.txt?" in str(urls[1])

    for url, expected in zip([str(u) for u in urls], [b"first", b"second"]):
        resp = requests.get(url, timeout=30)
        assert resp.status_code == 200, resp.text
        assert resp.content == expected
