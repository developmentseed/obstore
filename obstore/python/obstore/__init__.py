from typing import TYPE_CHECKING

from . import store
from ._obstore import (
    Bytes,
    ___version,
    copy,
    copy_async,
    delete,
    delete_async,
    get,
    get_async,
    get_range,
    get_range_async,
    get_ranges,
    get_ranges_async,
    head,
    head_async,
    list,  # noqa: A004
    list_with_delimiter,
    list_with_delimiter_async,
    open_reader,
    open_reader_async,
    open_writer,
    open_writer_async,
    put,
    put_async,
    rename,
    rename_async,
    sign,
    sign_async,
)

if TYPE_CHECKING:
    from . import _exceptions, _store
    from ._obstore import (
        HTTP_METHOD,
        AsyncReadableFile,
        AsyncWritableFile,
        Bytes,
        BytesStream,
        GetResult,
        ReadableFile,
        SignCapableStore,
        WritableFile,
    )
__version__: str = ___version()
