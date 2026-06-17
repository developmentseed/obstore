import pickle

import pytest

from obstore.exceptions import BaseError
from obstore.store import HTTPStore

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


def test_root_certificate_no_pem_blocks_is_noop():
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
