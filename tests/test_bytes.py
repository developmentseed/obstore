from __future__ import annotations

import pickle
from typing import TYPE_CHECKING

import pytest

from obstore import Bytes

if TYPE_CHECKING:
    from collections.abc import Iterable

ALL_BYTES = b"".join([bytes([i]) for i in range(256)])


def test_empty_eq() -> None:
    """Test that empty bytes and Bytes are equal."""
    assert Bytes(b"") == b""


def test_repr():
    """Test the repr of Bytes and bytes."""
    py_buf = b"foo\nbar\nbaz"
    rust_buf = Bytes(py_buf)
    # Assert reprs are the same excluding the prefix and suffix
    assert repr(py_buf)[2:-1] == repr(rust_buf)[8:-2]


@pytest.mark.parametrize(
    "b",
    [bytes([i]) for i in range(256)],
)
def test_uno_byte_bytes_repr(b: bytes) -> None:
    """Test the repr of Bytes and bytes for single byte values."""
    rust_bytes = Bytes(b)
    rust_bytes_str = repr(rust_bytes)
    rust_bytes_str_eval = eval(rust_bytes_str)  # noqa: S307
    assert rust_bytes_str_eval == rust_bytes == b


class TestBytesRemovePrefixSuffix:
    """Test the remove_prefix and remove_suffix methods."""

    def test_remove_prefix(self) -> None:
        """Test that remove_prefix works as expected."""
        rust_bytes = Bytes(b"asdf")
        assert rust_bytes.removeprefix(b"as") == Bytes(b"df")
        assert rust_bytes.removeprefix(b"asdf") == Bytes(b"")

    def test_remove_suffix(self) -> None:
        """Test that remove_suffix works as expected."""
        rust_bytes = Bytes(b"asdf")
        assert rust_bytes.removesuffix(b"df") == Bytes(b"as")
        assert rust_bytes.removesuffix(b"asdf") == Bytes(b"")


class TestBytesSlice:
    """Test suite for Bytes slicing."""

    def test_zero_step_value_err(self) -> None:
        """Test that slicing with step=0 raises ValueError."""
        rs_bytes = Bytes(b"abcdefg")
        py_bytes = b"abcdefg"
        with pytest.raises(ValueError, match="slice step cannot be zero"):
            _py_new = py_bytes[0:4:0]

        with pytest.raises(ValueError, match="slice step cannot be zero"):
            _rs_bytes = rs_bytes[0:4:0]

    @pytest.mark.parametrize(
        "py_bytes",
        [b"abcdefg", b"", ALL_BYTES],
    )
    def test_slice_o_bytes(self, py_bytes: bytes) -> None:
        """Run slicing on both bytes and Bytes and assert they are equal."""
        rs_bytes = Bytes(py_bytes)
        for start, stop, step, _sliced in self._bytes_slices(py_bytes):
            new_py = py_bytes[start:stop:step]
            new_rs = rs_bytes[start:stop:step]
            assert new_rs == new_py

    @staticmethod
    def _bytes_slices(
        b: bytes,
        range_buffer: int = 3,
    ) -> Iterable[tuple[int, int, int, bytes]]:
        """Yield tuples (start, stop, step, sliced_result) for all slices of b."""
        b_len = len(b)
        indices_range = range(-b_len - (range_buffer - 1), b_len + range_buffer)
        steps = (i for i in range(-(b_len + 2), b_len + 3) if i != 0)
        return (
            (start, stop, step, b[start:stop:step])
            for start in indices_range
            for stop in indices_range
            for step in steps
        )


def test_pickle():
    b = Bytes(b"hello_world")
    assert b == pickle.loads(pickle.dumps(b))


@pytest.mark.parametrize(
    ("needle", "haystack"),
    [
        (b"", b"abc"),
        (b"", b""),
        (b"b", b"abc"),
        (b"bc", b"abc"),
        (b"abc", b"abc"),
        (b"abcd", b"abc"),
        (b"x", b"abc"),
        (b"a", b""),
    ],
)
def test_contains(needle: bytes, haystack: bytes) -> None:
    """Test that `in` matches bytes, including for an empty needle."""
    assert (needle in Bytes(haystack)) == (needle in haystack)


@pytest.mark.parametrize(("value", "count"), [(b"abc", 3), (b"abc", 0), (b"", 3)])
def test_mul(value: bytes, count: int) -> None:
    """Test that repetition matches bytes."""
    assert Bytes(value) * count == value * count


def test_mul_too_long_raises_overflow_error() -> None:
    """Test that a result longer than the maximum size raises OverflowError."""
    with pytest.raises(OverflowError):
        Bytes(b"abc") * (1 << 62)


def test_mul_allocation_failure_raises_memory_error() -> None:
    """Test that a failed allocation raises MemoryError instead of aborting."""
    with pytest.raises(MemoryError):
        Bytes(b"a") * (1 << 62)


def test_mul_empty_by_large_count() -> None:
    """Test that repeating an empty Bytes many times returns immediately."""
    assert Bytes(b"") * (1 << 62) == b""
