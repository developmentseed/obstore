from typing import TypedDict

class ClientConfig(TypedDict, total=False):
    """HTTP client configuration"""

    allow_http: bool
    """Allow non-TLS, i.e. non-HTTPS connections."""
    allow_invalid_certificates: bool
    """Skip certificate validation on https connections.

    !!! warning

        You should think very carefully before using this method. If
        invalid certificates are trusted, *any* certificate for *any* site
        will be trusted for use. This includes expired certificates. This
        introduces significant vulnerabilities, and should only be used
        as a last resort or for testing
    """
    connect_timeout: str
    """Timeout for only the connect phase of a Client"""
    default_content_type: str
    """default `CONTENT_TYPE` for uploads"""
    http1_only: bool
    """Only use http1 connections."""
    http2_keep_alive_interval: str
    """Interval for HTTP2 Ping frames should be sent to keep a connection alive."""
    http2_keep_alive_timeout: str
    """Timeout for receiving an acknowledgement of the keep-alive ping."""
    http2_keep_alive_while_idle: str
    """Enable HTTP2 keep alive pings for idle connections"""
    http2_only: bool
    """Only use http2 connections"""
    pool_idle_timeout: str
    """The pool max idle timeout.

    This is the length of time an idle connection will be kept alive.
    """
    pool_max_idle_per_host: str
    """Maximum number of idle connections per host."""
    proxy_url: str
    """HTTP proxy to use for requests."""
    timeout: str
    """Request timeout.

    The timeout is applied from when the request starts connecting until the
    response body has finished.
    """
    user_agent: str
    """User-Agent header to be used by this client."""
