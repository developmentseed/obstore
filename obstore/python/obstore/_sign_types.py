"""Importable type hints for _sign."""

from __future__ import annotations

import sys
from typing import Literal

from .store import AzureStore, GCSStore, S3Store

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

HTTP_METHOD: TypeAlias = Literal[
    "GET",
    "PUT",
    "POST",
    "HEAD",
    "PATCH",
    "TRACE",
    "DELETE",
    "OPTIONS",
    "CONNECT",
]
"""Allowed HTTP Methods for signing."""

SignCapableStore: TypeAlias = AzureStore | GCSStore | S3Store
"""ObjectStore instances that are capable of signing."""
