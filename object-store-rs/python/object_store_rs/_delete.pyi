from typing import Sequence

from .store import ObjectStore

def delete(store: ObjectStore, locations: str | Sequence[str]) -> None:
    """Delete the object at the specified location(s).

    Args:
        store: The ObjectStore instance to use.
        locations: The path or paths within ObjectStore to delete.
    """

async def delete_async(store: ObjectStore, locations: str | Sequence[str]) -> None:
    """Call `delete` asynchronously.

    Refer to the documentation for [delete][object_store_rs.delete].
    """
