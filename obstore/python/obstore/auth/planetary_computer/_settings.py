"""Settings for planetary computer.

This is copied almost exactly from the planetary-computer PyPI package.
"""

from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import dotenv

SETTINGS_ENV_FILE = Path("~/.planetarycomputer/settings.env").expanduser()
SETTINGS_ENV_PREFIX = "PC_SDK_"

DEFAULT_SAS_TOKEN_ENDPOINT = "https://planetarycomputer.microsoft.com/api/sas/v1/token"  # noqa: S105


def set_subscription_key(key: str) -> None:
    """Set the Planetary Computer API subscription key.

    To use within the process that loaded this module. Ths does not write
    to the settings file.

    Args:
      key: The Planetary Computer API subscription key to use
        for methods inside this library that can utilize the key,
        such as SAS token generation.

    """
    Settings.get().subscription_key = key


def _from_env(key: str) -> str | None:
    value = os.environ.get(key)
    if value is None:
        dotenv.load_dotenv(SETTINGS_ENV_FILE)
        value = os.environ.get(key)
    return value


def _subscription_key_default() -> str | None:
    return _from_env("PC_SDK_SUBSCRIPTION_KEY")


def _sas_url_default() -> str:
    return _from_env("PC_SDK_SAS_URL") or DEFAULT_SAS_TOKEN_ENDPOINT


@dataclass
class Settings:
    """PC SDK configuration settings.

    Settings defined here are attempted to be read in two ways, in this order:
      * environment variables
      * environment file: ~/.planetarycomputer/settings.env

    That is, any settings defined via environment variables will take precedence
    over settings defined in the environment file, so can be used to override.

    All settings are prefixed with `PC_SDK_`
    """

    subscription_key: str | None = dataclasses.field(
        default_factory=_subscription_key_default,
    )
    sas_url: str | None = dataclasses.field(default_factory=_sas_url_default)

    @staticmethod
    @lru_cache(maxsize=1)
    def get() -> Settings:
        return Settings()
