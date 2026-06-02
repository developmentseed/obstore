from collections.abc import Sequence

from ._store import ObjectStore

def delete(store: ObjectStore, paths: str | Sequence[str]) -> None:
    """Delete the object at the specified location(s).

    Args:
        store: The ObjectStore instance to use.
        paths: The path or paths within the store to delete.

    When supported by the underlying store, this method will use bulk operations
    that delete more than one object per a request.

    The following backends support native bulk delete operations:

    - **AWS (S3)**: Uses the native [DeleteObjects] API with batches of up to 1000 objects
    - **Azure**: Uses the native [Blob Batch] API with batches of up to 256 objects

    The following backends use concurrent individual delete operations:

    - **GCP**: Performs individual delete requests with up to 10 concurrent operations
    - **HTTP**: Performs individual delete requests with up to 10 concurrent operations
    - **Local**: Performs individual file deletions with up to 10 concurrent operations
    - **Memory**: Performs individual in-memory deletions sequentially

    [DeleteObjects]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    [Blob Batch]: https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch

    If the object did not exist, the result may be an error or a success,
    depending on the behavior of the underlying store. For example, local
    filesystems, GCP, and Azure return an error, while S3 and in-memory will
    return Ok.

    """

async def delete_async(store: ObjectStore, paths: str | Sequence[str]) -> None:
    """Call `delete` asynchronously.

    Refer to the documentation for [delete][obstore.delete].
    """
