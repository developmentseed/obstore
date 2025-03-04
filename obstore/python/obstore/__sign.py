from typing import Literal, TypeAlias

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
