from __future__ import annotations

from typing import Iterable

import pytest

from obstore import Bytes

ALL_BYTES = b"".join([bytes([i]) for i in range(256)])


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


class TestBytesRemovePrefixSuffix:
    def test_remove_prefix(self) -> None:
        rust_bytes = Bytes(b"asdf")
        assert rust_bytes.removeprefix(b"as") == Bytes(b"df")
        assert rust_bytes.removeprefix(b"asdf") == Bytes(b"")

    def test_remove_suffix(self) -> None:
        rust_bytes = Bytes(b"asdf")
        assert rust_bytes.removesuffix(b"df") == Bytes(b"as")
        assert rust_bytes.removesuffix(b"asdf") == Bytes(b"")


class TestBytesSlice:
    def test_zero_step_value_err(self) -> None:
        rs_bytes = Bytes(b"abcdefg")
        py_bytes = b"abcdefg"
        with pytest.raises(ValueError):
            _py_new = py_bytes[0:4:0]

        with pytest.raises(ValueError):
            _rs_bytes = rs_bytes[0:4:0]

    @pytest.mark.parametrize(
        "py_bytes",
        [b"abcdefg", b"", ALL_BYTES],
    )
    def test_slice_o_bytes(self, py_bytes: bytes) -> None:
        """Run slicing on both bytes and Bytes and assert they are equiv"""
        rs_bytes = Bytes(py_bytes)
        for start, stop, step, _sliced in _bytes_slices(py_bytes):
            new_py = py_bytes[start:stop:step]
            new_rs = rs_bytes[start:stop:step]
            assert new_rs == new_py


def _bytes_slices(
    b: bytes, range_buffer: int = 3
) -> Iterable[tuple[int, int, int, bytes]]:
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
