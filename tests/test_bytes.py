from __future__ import annotations

import typing as t

import pytest

from obstore import Bytes


def test_decode() -> None:
    py_bytes = b"asdf"
    ry_bytes = Bytes(py_bytes)
    assert ry_bytes.decode() == "asdf"


# TODO: figure out if accepts empty bytes (i jessekrubin think it should if it is trying to be `bytes`)
def test_empty_eq() -> None:
    py_bytes = b""
    ry_bytes = Bytes(py_bytes)
    assert py_bytes == ry_bytes


class TestBytesRemovePrefixSuffix:
    def test_remove_prefix(self) -> None:
        ry_bytes = Bytes(b"asdf")
        assert ry_bytes.removeprefix(b"as") == Bytes(b"df")
        assert ry_bytes.removeprefix(b"asdf") == Bytes(b"")

    def test_remove_suffix(self) -> None:
        ry_bytes = Bytes(b"asdf")
        assert ry_bytes.removesuffix(b"df") == Bytes(b"as")
        assert ry_bytes.removesuffix(b"asdf") == Bytes(b"")


@pytest.mark.parametrize(
    "b",
    [bytes([i]) for i in range(256)],
)
def test_uno_byte_bytes_repr(b: bytes) -> None:
    ry_bytes = Bytes(b)
    ry_bytes_str = str(ry_bytes)
    ry_bytes_str_eval = eval(ry_bytes_str)
    assert ry_bytes_str_eval == ry_bytes


class TestBytesSlice:
    def test_zero_step_value_err(self) -> None:
        ry_bytes = Bytes(b"abcdefg")
        py_bytes = b"abcdefg"
        with pytest.raises(ValueError):
            _py_new = py_bytes[0:4:0]

        with pytest.raises(ValueError):
            _ry_new = ry_bytes[0:4:0]

    def test_slice_forward(self) -> None:
        ry_bytes = Bytes(b"abcdefg")
        py_bytes = b"abcdefg"
        for start, stop, step, _sliced in _bytes_slices(py_bytes):
            new_py = py_bytes[start:stop:step]
            new_ry = ry_bytes[start:stop:step]
            assert new_ry == new_py


def _bytes_slices(
    b: bytes, range_buffer: int = 3
) -> t.Generator[tuple[int, int, int, bytes], None, None]:
    """yield tuples of (start, stop, step, sliced_result) for all possible slices of b."""
    b_len = len(b)
    indices_range = range(-b_len - (range_buffer - 1), b_len + range_buffer)
    steps = (i for i in range(-(b_len + 2), b_len + 3) if i != 0)
    return (
        (start, stop, step, b[start:stop:step])
        for start in indices_range
        for stop in indices_range
        for step in steps
    )


@pytest.mark.parametrize("b", [bytes([i]) for i in range(256)])
def test_hex_str(b: bytes) -> None:
    ry_bytes = Bytes(b)
    ry_hex_str = ry_bytes.hex()
    py_hex_str = b.hex()
    assert ry_hex_str == py_hex_str
