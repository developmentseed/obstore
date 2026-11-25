from __future__ import annotations

import pickle
import ssl
import sys
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest

from obstore.exceptions import BaseError
from obstore.store import HTTPStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

# Self-signed CAs generated with:
#   openssl req -x509 -newkey rsa:2048 -nodes -days 36500 -subj /CN=obstore-test-ca-N
CERT_1 = b"""-----BEGIN CERTIFICATE-----
MIIDGzCCAgOgAwIBAgIUBqLgQSw4wiW06IhiKHNN5NyztG8wDQYJKoZIhvcNAQEL
BQAwHDEaMBgGA1UEAwwRb2JzdG9yZS10ZXN0LWNhLTEwIBcNMjYwNjE2MTQxMjEz
WhgPMjEyNjA1MjMxNDEyMTNaMBwxGjAYBgNVBAMMEW9ic3RvcmUtdGVzdC1jYS0x
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxw/NGsR0jwDKOSyXsfeT
WvniRh+bozmvQuyxW/qZwuNrgrkPdZbSM3j3eaqFOa9iqSds17u3bTYnC08SGyjo
hecOuja2KP5lYUJ31Vfad4KlkYVuNBjQ3FZb71jbwXuwKnhcrYlMUM8Vt/Oay/q+
uKDG0kMXTdjXButCFE+s8oTk0dlGEjQ6IuI3/0l5cv631iMIaIW13CDG1mMJwu9Q
yco8/J6aK6ZQFa0T9TRm2v3Y/3qiX+WryQakX72IY9DbRtxwjh0Lm81M46DChsE3
FyZB69aaKPdSkWUmNAWBa3H8cQziUv2SLz6DAG7r5cJZ0xkbqgiaQR8P1ud8yeL+
LwIDAQABo1MwUTAdBgNVHQ4EFgQUJGhd1ORfcw1Ov3/fljWfEoUJSx8wHwYDVR0j
BBgwFoAUJGhd1ORfcw1Ov3/fljWfEoUJSx8wDwYDVR0TAQH/BAUwAwEB/zANBgkq
hkiG9w0BAQsFAAOCAQEAurAH4Gvomh4QffpZf6s3/TPXHGM2wuuuOKB7QD7sfp8V
nlFthbcpEd0SRaH2T4fIeVDuHcMx/F7GYBaVOjXqRq9N6+zeutpPvu7YAoE4zbLz
Wnrn2fZG5uAO+HW26QXOsZU2zJHpHzZWZ3G1dV/C97k8hmSjkH7OOZSlZ/qIN4+p
5t5qBUJek66luxnEyfOdFitiJe9Ri6sj2ffT0aXejZCu2boO+3Szm6boLu3kCC7l
Latj8uDxEm6HsD4gxn61zMmmSjitTJYJt+lW8+3tlSf17tXd1TLG/cF/0gD28OhL
UmOHI1HSNtVKwotBkpOrlby1hWOX7IMtqPRmEJzz6Q==
-----END CERTIFICATE-----
"""

CERT_2 = b"""-----BEGIN CERTIFICATE-----
MIIDGzCCAgOgAwIBAgIUTxck2vXxjoPXiA51CCIuOodX9AMwDQYJKoZIhvcNAQEL
BQAwHDEaMBgGA1UEAwwRb2JzdG9yZS10ZXN0LWNhLTIwIBcNMjYwNjE2MTQxMjEz
WhgPMjEyNjA1MjMxNDEyMTNaMBwxGjAYBgNVBAMMEW9ic3RvcmUtdGVzdC1jYS0y
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAswXoqgwFuG/CD9IYB+Jt
mCU7gPinn3XLUNVo9K+8Z+C1AvZvuW0EIxmbE+vUjM3u0PbNf0auATOdfIeT30uo
Rr69C1Qu56I5gJP8nWPscDXyEyMOSDxlhSycObwlKM9dlCIz3fwYlU5ukPw+e+tU
kBPuHFbtElXM4u68BdTTjWNZzA4xplSFc5RCcrNnhYZPCINPRRkrjA27F7o1lwQ+
ibzg5CquFSnDdojN8edzIE+xGLznOpTiuu17VqljIkT1bxm9qUZ5fOxsKfTHMfQ2
gBM7lzF1suVWjhCjxKtJwd5JWQyo/ynR2FsAMfxtUXH1GVYrfEm6RUAQQ2hnzFue
wwIDAQABo1MwUTAdBgNVHQ4EFgQU/48jUGMAG9VCesLJ7lSh5TOs+wgwHwYDVR0j
BBgwFoAU/48jUGMAG9VCesLJ7lSh5TOs+wgwDwYDVR0TAQH/BAUwAwEB/zANBgkq
hkiG9w0BAQsFAAOCAQEAiJvYmLrqG/bMWyb3u77tLSfwxGftIo5HApgKt6vgtUZr
0qhgf1scto1lIworYX+MJe/dqHVZC6IjiXOO77HCJlOQoPd3byvV+fu3sU1P79ah
tIFKyRN6cGfAXlPYhR3oB30KEHIMZKve/Y9c1IGv7cogO0/VDXXTe3YcWDLtjzd8
2wGP3vlKhbzV2ZigRtO37fCTBGJRlpmUT+KWQ5roSSbutyYNhAAAOMoM+M6uD4Mm
H3ZwuubjcsNBfC/YQabjFuDrIUAYUS7CWDVv1Qi/26PlRQJEFoVayDT4aO86cAop
Tg1+pQESP0OOub4TswJ0qs0Xx2umJdos/XOaf3xfpQ==
-----END CERTIFICATE-----
"""

CERT_BUNDLE = CERT_1 + CERT_2

# Test-only CA and a certificate for 127.0.0.1 signed by it, used by the local HTTPS
# server below. Generated with:
#   openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
#     -days 36500 -subj /CN=obstore-test-ca -keyout ca.key -out ca.pem
#   openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
#     -subj /CN=localhost -keyout server.key -out server.csr
#   openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -days 36500 \
#     -extfile <(echo subjectAltName=IP:127.0.0.1,DNS:localhost) -out server.pem
SERVER_CA = b"""-----BEGIN CERTIFICATE-----
MIIBizCCATGgAwIBAgIUcJuMaosWSBAgQedRzBwk80n4WJ8wCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPb2JzdG9yZS10ZXN0LWNhMCAXDTI2MTEyNDE4Mzk1MloYDzIx
MjYxMDMxMTgzOTUyWjAaMRgwFgYDVQQDDA9vYnN0b3JlLXRlc3QtY2EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAASw7bv5yJMz+jHhw5DPsQDozQEH2TPVkF9caVNz
xjPaMbDB5xft0ymrJISMi0nHXU2TxQhrgNGvTrzJpAQ3lj7Io1MwUTAdBgNVHQ4E
FgQU0XbiUSJ87q14tkYGaKrMo09mzrAwHwYDVR0jBBgwFoAU0XbiUSJ87q14tkYG
aKrMo09mzrAwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiEA1UZW
rkVdq7+/b8HaVbttW6ttGNHj+PA7ZJpvcQ5XSRkCIA5UXLE+OtfKw6CdfhQT7WJc
hoaderwK+77uV7PGwZaA
-----END CERTIFICATE-----
"""

SERVER_CERT = b"""-----BEGIN CERTIFICATE-----
MIIBkDCCATagAwIBAgIUK2DWcmwHrHVJyROZY/2aXfof5agwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPb2JzdG9yZS10ZXN0LWNhMCAXDTI2MTEyNDE4Mzk1MloYDzIx
MjYxMDMxMTgzOTUyWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAAT62Q33xGPYBNGeaYcyaebrSzO8yM+IreJDwNqf8uOxswUY
FXaTbX98jKl5w0KGBlE5WMVGQVx1om4Yrrfc3Nh9o14wXDAaBgNVHREEEzARhwR/
AAABgglsb2NhbGhvc3QwHQYDVR0OBBYEFGyE8ksGnZXMzQ0sAzedSeGfMudZMB8G
A1UdIwQYMBaAFNF24lEifO6teLZGBmiqzKNPZs6wMAoGCCqGSM49BAMCA0gAMEUC
IQCPBlbJFk3ECop0A9xGvbJk+PhxMDxfNQHROzoG5pBiDQIgIggzJWwGKpkyhllF
RknRP+Ylc1ArNdSeGVN2MOTjG6c=
-----END CERTIFICATE-----
"""

SERVER_KEY = b"""-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgunyoURimXSkCvdHT
F83OTgwrZSxGYDwahueb7Rf7oIGhRANCAAT62Q33xGPYBNGeaYcyaebrSzO8yM+I
reJDwNqf8uOxswUYFXaTbX98jKl5w0KGBlE5WMVGQVx1om4Yrrfc3Nh9
-----END PRIVATE KEY-----
"""


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass


@pytest.fixture
def https_url(tmp_path: Path) -> Generator[str, None, None]:
    """Serve a `hello.txt` file over HTTPS with a certificate signed by SERVER_CA."""
    (tmp_path / "server.pem").write_bytes(SERVER_CERT)
    (tmp_path / "server.key").write_bytes(SERVER_KEY)
    root = tmp_path / "www"
    root.mkdir()
    (root / "hello.txt").write_bytes(b"hello")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "server.pem", tmp_path / "server.key")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        partial(_QuietHandler, directory=root),
    )
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield f"https://127.0.0.1:{server.server_port}"

    server.shutdown()
    server.server_close()
    thread.join()


def test_proxy_excludes():
    HTTPStore.from_url(
        "https://example.com",
        client_options={
            "proxy_url": "https://proxy.example.com:8080",
            "proxy_excludes": "localhost,127.0.0.1,.svc.cluster.local",
        },
    )


def test_proxy_ca_certificate():
    HTTPStore.from_url(
        "https://example.com",
        client_options={
            "proxy_url": "https://proxy.example.com:8080",
            "proxy_ca_certificate": CERT_1.decode(),
        },
    )


def test_root_certificate_bytes():
    HTTPStore.from_url(
        "https://example.com",
        client_options={"root_certificate": CERT_1},
    )


def test_root_certificate_str():
    HTTPStore.from_url(
        "https://example.com",
        client_options={"root_certificate": CERT_1.decode()},
    )


def test_root_certificate_bundle():
    HTTPStore.from_url(
        "https://example.com",
        client_options={"root_certificate": CERT_BUNDLE},
    )


def test_root_certificate_additive_to_public_host():
    HTTPStore.from_url(
        "https://s3.amazonaws.com",
        client_options={"root_certificate": CERT_BUNDLE},
    )


def test_root_certificate_no_pem_blocks_raises():
    # Input that contains no PEM blocks parses to zero certificates; rather than
    # silently trusting nothing, this should surface as an error.
    with pytest.raises(ValueError, match="No certificates found"):
        HTTPStore.from_url(
            "https://example.com",
            client_options={"root_certificate": b"not a real certificate"},
        )


def test_root_certificate_malformed_block_raises():
    with pytest.raises(BaseError):
        HTTPStore.from_url(
            "https://example.com",
            client_options={
                "root_certificate": (
                    b"-----BEGIN CERTIFICATE-----\n"
                    b"not-valid-base64!!!\n"
                    b"-----END CERTIFICATE-----\n"
                ),
            },
        )


def test_root_certificate_roundtrip_pickle():
    store = HTTPStore.from_url(
        "https://example.com",
        client_options={"root_certificate": CERT_BUNDLE},
    )
    restored: HTTPStore = pickle.loads(pickle.dumps(store))
    assert restored.url == store.url


def test_disable_system_certificates_roundtrip():
    store = HTTPStore.from_url(
        "https://example.com",
        client_options={"disable_system_certificates": True},
    )
    assert store.client_options == {"disable_system_certificates": "true"}
    restored: HTTPStore = pickle.loads(pickle.dumps(store))
    assert restored.client_options == store.client_options


def test_disable_system_certificates_trusts_root_certificate(https_url: str):
    store = HTTPStore.from_url(
        https_url,
        client_options={
            "disable_system_certificates": True,
            "root_certificate": SERVER_CA,
        },
    )
    assert store.get("hello.txt").bytes() == b"hello"


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="SSL_CERT_FILE replaces the system trust store only on Linux",
)
def test_disable_system_certificates_ignores_system_trust_store(
    https_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(SERVER_CA)
    monkeypatch.setenv("SSL_CERT_FILE", str(ca_file))

    store = HTTPStore.from_url(https_url)
    assert store.get("hello.txt").bytes() == b"hello"

    store = HTTPStore.from_url(
        https_url,
        client_options={"disable_system_certificates": True},
        retry_config={"max_retries": 0},
    )
    with pytest.raises(BaseError, match="UnknownIssuer"):
        store.get("hello.txt")


def test_disable_system_certificates_with_allow_invalid_certificates(https_url: str):
    store = HTTPStore.from_url(
        https_url,
        client_options={
            "disable_system_certificates": True,
            "allow_invalid_certificates": True,
        },
    )
    assert store.get("hello.txt").bytes() == b"hello"


def test_http2_max_frame_size_roundtrip():
    store = HTTPStore.from_url(
        "https://example.com",
        client_options={"http2_max_frame_size": "32768"},
    )
    assert store.client_options == {"http2_max_frame_size": "32768"}
    restored: HTTPStore = pickle.loads(pickle.dumps(store))
    assert restored.client_options == store.client_options


def test_http2_max_frame_size_invalid():
    with pytest.raises(BaseError, match='failed to parse "abc" as u32'):
        HTTPStore.from_url(
            "https://example.com",
            client_options={"http2_max_frame_size": "abc"},
        )
