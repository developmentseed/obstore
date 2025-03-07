from typing import TYPE_CHECKING

from ._obstore import *
from ._obstore import ___version

if TYPE_CHECKING:
    from . import _store, exceptions

__version__: str = ___version()
