from __future__ import annotations

from typing import TYPE_CHECKING
from warnings import warn

if TYPE_CHECKING:
    import aiohttp
    import aiohttp_retry
    import requests


def default_requests_session() -> requests.Session:
    import requests
    import requests.adapters
    import urllib3
    import urllib3.util.retry

    # retry_total: The number of allowable retry attempts for REST API calls.
    #     Use retry_total=0 to disable retries. A backoff factor to apply
    #     between attempts.
    # retry_backoff_factor: A backoff factor to apply between attempts
    #     after the second try (most errors are resolved immediately by a second
    #     try without a delay). Retry policy will sleep for:

    #     ``{backoff factor} * (2 ** ({number of total retries} - 1))`` seconds.
    #     If the backoff_factor is 0.1, then the retry will sleep for
    #     [0.0s, 0.2s, 0.4s, ...] between retries. The default value is 0.8.
    retry_total = 10
    retry_backoff_factor = 0.8

    session = requests.Session()
    retry = urllib3.util.retry.Retry(
        total=retry_total,
        backoff_factor=retry_backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def default_aiohttp_session() -> aiohttp_retry.RetryClient | aiohttp.ClientSession:
    try:
        from aiohttp_retry import ExponentialRetry, RetryClient

        return RetryClient(
            raise_for_status=False,
            retry_options=ExponentialRetry(attempts=1),
        )
    except ImportError:
        from aiohttp import ClientSession

        # Put this after validating that we can import aiohttp
        warn(
            "aiohttp_retry not installed and custom aiohttp session not provided. "
            "Authentication will not be retried.",
            RuntimeWarning,
            stacklevel=3,
        )

        return ClientSession()
