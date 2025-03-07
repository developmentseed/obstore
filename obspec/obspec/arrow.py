"""Arrow type hints."""

from __future__ import annotations

from typing import Protocol


class ArrowArrayExportable(Protocol):
    """An object with an `__arrow_c_array__` method.

    Supported objects include:

    - arro3 `Array` or `RecordBatch` objects.
    - pyarrow `Array` or `RecordBatch` objects

    Such an object implements the [Arrow C Data Interface
    interface](https://arrow.apache.org/docs/format/CDataInterface.html) via the
    [Arrow PyCapsule
    Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    This allows for zero-copy Arrow data interchange across libraries.
    """

    def __arrow_c_array__(
        self,
        requested_schema: object | None = None,
    ) -> tuple[object, object]:
        """Return Arrow C data interface PyCapsules for the object."""
        ...


class ArrowStreamExportable(Protocol):
    """An object with an `__arrow_c_stream__` method.

    Supported objects include:

    - arro3 `Table`, `RecordBatchReader`, `ChunkedArray`, or `ArrayReader` objects.
    - Polars `Series` or `DataFrame` objects (polars v1.2 or higher)
    - pyarrow `RecordBatchReader`, `Table`, or `ChunkedArray` objects (pyarrow v14 or
        higher)
    - pandas `DataFrame`s  (pandas v2.2 or higher)
    - ibis `Table` objects.

    For an up to date list of supported objects, see [this
    issue](https://github.com/apache/arrow/issues/39195#issuecomment-2245718008).

    Such an object implements the [Arrow C Stream
    interface](https://arrow.apache.org/docs/format/CStreamInterface.html) via the
    [Arrow PyCapsule
    Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    This allows for zero-copy Arrow data interchange across libraries.
    """

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """Return an Arrow C stream interface PyCapsule for the object."""
        ...
