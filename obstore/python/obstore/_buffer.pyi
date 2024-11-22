import sys

if sys.version_info >= (3, 12):
    from collections.abc import Buffer as _Buffer
else:
    from typing_extensions import Buffer as _Buffer

class Buffer(_Buffer):
    """
    A buffer implementing the Python buffer protocol, allowing zero-copy access to the
    underlying memory provided by Rust.

    You can pass this to [`memoryview`][] for a zero-copy view into the underlying data.
    """

    def as_bytes(self) -> bytes:
        """Copy this buffer into a Python `bytes` object."""
    def __len__(self) -> int: ...
