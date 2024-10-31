from __future__ import annotations

from datetime import timedelta
from typing import TypedDict

class BackoffConfig(TypedDict):
    """
    Exponential backoff with jitter

    See <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>
    """

    init_backoff: timedelta
    """The initial backoff duration"""

    max_backoff: timedelta
    """The maximum backoff duration"""

    base: int | float
    """The base of the exponential to use"""

class RetryConfig(TypedDict):
    """
    The configuration for how to respond to request errors

    The following categories of error will be retried:

    * 5xx server errors
    * Connection errors
    * Dropped connections
    * Timeouts for [safe] / read-only requests

    Requests will be retried up to some limit, using exponential
    backoff with jitter. See [`BackoffConfig`][obstore.store.BackoffConfig] for
    more information

    [safe]: https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.1
    """

    backoff: BackoffConfig
    """The backoff configuration"""

    max_retries: int
    """
    The maximum number of times to retry a request

    Set to 0 to disable retries
    """

    retry_timeout: timedelta
    """
    The maximum length of time from the initial request
    after which no further retries will be attempted

    This not only bounds the length of time before a server
    error will be surfaced to the application, but also bounds
    the length of time a request's credentials must remain valid.

    As requests are retried without renewing credentials or
    regenerating request payloads, this number should be kept
    below 5 minutes to avoid errors due to expired credentials
    and/or request payloads
    """
