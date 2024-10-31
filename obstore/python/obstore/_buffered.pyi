from typing import List

from .store import ObjectStore

def open(store: ObjectStore, path: str) -> ReadableFile:
    """Open a file object from the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Returns:
        ReadableFile
    """

async def open_async(store: ObjectStore, path: str) -> ReadableFile:
    """Call `open` asynchronously.

    Refer to the documentation for [open][object_store_rs.open].
    """

class ReadableFile:
    def close(self): ...
    async def read(self, size: int) -> bytes: ...
    async def readall(self) -> bytes: ...
    async def readline(self) -> str: ...
    async def readlines(self) -> List[str]: ...
    async def seek(self) -> None: ...
    @staticmethod
    def seekable() -> bool: ...
    async def tell(self) -> int: ...
