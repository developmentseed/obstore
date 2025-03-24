from typing import TYPE_CHECKING

from . import _obstore, store
from ._obstore import *  # noqa: F403
from ._obstore import ___version

if TYPE_CHECKING:
    from . import exceptions  # noqa: TC004

__version__: str = ___version()

__all__ = ["__version__", "exceptions", "store"]
__all__ += _obstore.__all__
