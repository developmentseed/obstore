from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from ._meta import ObjectMeta


class Head(Protocol):
    def head(self, path: str) -> ObjectMeta:
        """Return the metadata for the specified location.

        Args:
            path: The path within ObjectStore to retrieve.

        Returns:
            ObjectMeta

        """
        ...


class HeadAsync(Protocol):
    async def head_async(self, path: str) -> ObjectMeta:
        """Call `head` asynchronously.

        Refer to the documentation for [Head][obspec.Head].
        """
        ...
