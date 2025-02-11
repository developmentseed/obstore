from __future__ import annotations

import pytest

from obstore import Bytes


def test_empty_eq() -> None:
    assert Bytes(b"") == b""


def test_repr():
    py_buf = b"foo\nbar\nbaz"
    rust_buf = Bytes(py_buf)
    # Assert reprs are the same excluding the prefix and suffix
    assert repr(py_buf)[2:-1] == repr(rust_buf)[8:-2]


@pytest.mark.parametrize(
    "b",
    [bytes([i]) for i in range(256)],
)
def test_uno_byte_bytes_repr(b: bytes) -> None:
    rust_bytes = Bytes(b)
    rust_bytes_str = repr(rust_bytes)
    rust_bytes_str_eval = eval(rust_bytes_str)  # noqa: S307
    assert rust_bytes_str_eval == rust_bytes == b
