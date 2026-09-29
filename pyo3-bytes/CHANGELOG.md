# Changelog

## 0.7.2 - 2026-09-29

- `b"" in Bytes(...)` returns `True` instead of panicking.
- `Bytes * n` raises `OverflowError` when the result would be too long and `MemoryError` when the allocation fails, instead of panicking or aborting the process. Repeating an empty `Bytes` returns immediately for any `n`.
- The `Bytes.__mul__` type hint now takes an `int` and returns `Bytes`.

## 0.7.1 - 2026-06-17

Add `#[derive(Clone)]` and `#[derive(Default)]` to `PyBytes`.
