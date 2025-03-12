from ._store import ObjectStore

def copy(store: ObjectStore, from_: str, to: str, *, overwrite: bool = True) -> None:
    """Copy an object from one path to another in the same object store.

    Args:
        store: The ObjectStore instance to use.
        from_: Source path
        to: Destination path

    Keyword Args:
        overwrite: If `True`, if there exists an object at the destination, it will
                be overwritten.

            If `False`: will copy only if destination is empty. Performs an atomic operation if the underlying object storage supports it. If atomic operations are not supported by the underlying object storage (like S3) it will return an error.

            Will return an error if the destination already has an object.

    """

async def copy_async(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
) -> None:
    """Call `copy` asynchronously.

    Refer to the documentation for [copy][obstore.copy].
    """
