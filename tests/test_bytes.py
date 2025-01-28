from __future__ import annotations

import pytest

from obstore import Bytes


@pytest.mark.skip(reason="Not implemented *yet*")
def test_decode() -> None:
    py_bytes = b"asdf"
    ry_bytes = Bytes(py_bytes)
    assert ry_bytes.decode() == "asdf"


# TODO: figure out if accepts empty bytes (i jessekrubin think it should if it is trying to be `bytes`)
def test_empty_eq() -> None:
    py_bytes = b""
    ry_bytes = Bytes(py_bytes)
    assert py_bytes == ry_bytes


@pytest.mark.parametrize(
    "b",
    [bytes([i]) for i in range(256)],
)
def test_uno_byte_bytes_repr(b: bytes) -> None:
    ry_bytes = Bytes(b)
    ry_bytes_str = str(ry_bytes)
    ry_bytes_str_eval = eval(ry_bytes_str)
    assert ry_bytes_str_eval == ry_bytes
