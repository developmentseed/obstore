from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence


class Delete(Protocol):
    def delete(self, paths: str | Sequence[str]) -> None:
        """Delete the object at the specified location(s).

        Args:
            paths: The path or paths within the store to delete.

                When supported by the underlying store, this method will use bulk
                operations that delete more than one object per a request.

                If the object did not exist, the result may be an error or a success,
                depending on the behavior of the underlying store. For example, local
                filesystems, GCP, and Azure return an error, while S3 and in-memory will
                return Ok.

        """


class DeleteAsync(Protocol):
    async def delete_async(self, paths: str | Sequence[str]) -> None:
        """Call `delete` asynchronously.

        Refer to the documentation for [Delete][obspec.Delete].
        """
