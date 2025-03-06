from typing import TYPE_CHECKING

from .__get import GetOptions, OffsetRange, SuffixRange
from .__put import PutMode, PutResult, UpdateVersion
from .__sign import HTTP_METHOD
from ._attributes import Attribute, Attributes
from ._obstore import *
from ._obstore import ___version

if TYPE_CHECKING:
    from . import exceptions, store

__version__: str = ___version()
