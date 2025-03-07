from collections.abc import Sequence

from ._store import ObjectStore

def delete(store: ObjectStore, paths: str | Sequence[str]) -> None:
    """Delete the object at the specified location(s).

    Args:
        store: The ObjectStore instance to use.
        paths: The path or paths within the store to delete.

            When supported by the underlying store, this method will use bulk operations
            that delete more than one object per a request.

            If the object did not exist, the result may be an error or a success,
            depending on the behavior of the underlying store. For example, local
            filesystems, GCP, and Azure return an error, while S3 and in-memory will
            return Ok.

    """

async def delete_async(store: ObjectStore, paths: str | Sequence[str]) -> None:
    """Call `delete` asynchronously.

    Refer to the documentation for [delete][obstore.delete].
    """
