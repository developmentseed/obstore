from ._list import ObjectMeta
from .store import ObjectStore

def head(store: ObjectStore, path: str) -> ObjectMeta:
    """Return the metadata for the specified location.

    Args:
        store: The ObjectStore instance to use.
        path: The path within ObjectStore to retrieve.

    Returns:
        ObjectMeta

    """

async def head_async(store: ObjectStore, path: str) -> ObjectMeta:
    """Call `head` asynchronously.

    Refer to the documentation for [head][obstore.head].
    """
