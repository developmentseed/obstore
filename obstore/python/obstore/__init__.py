from typing import TYPE_CHECKING

from ._obstore import *
from ._obstore import ___version

if TYPE_CHECKING:
    from . import exceptions, store

__version__: str = ___version()
