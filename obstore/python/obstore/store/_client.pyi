from typing import Literal

ClientConfigKey = Literal[
    "allow_http",
    "allow_invalid_certificates",
    "connect_timeout",
    "default_content_type",
    "http1_only",
    "http2_keep_alive_interval",
    "http2_keep_alive_timeout",
    "http2_keep_alive_while_idle",
    "http2_only",
    "pool_idle_timeout",
    "pool_max_idle_per_host",
    "proxy_url",
    "timeout",
    "user_agent",
    "ALLOW_HTTP",
    "ALLOW_INVALID_CERTIFICATES",
    "CONNECT_TIMEOUT",
    "DEFAULT_CONTENT_TYPE",
    "HTTP1_ONLY",
    "HTTP2_KEEP_ALIVE_INTERVAL",
    "HTTP2_KEEP_ALIVE_TIMEOUT",
    "HTTP2_KEEP_ALIVE_WHILE_IDLE",
    "HTTP2_ONLY",
    "POOL_IDLE_TIMEOUT",
    "POOL_MAX_IDLE_PER_HOST",
    "PROXY_URL",
    "TIMEOUT",
    "USER_AGENT",
]
"""Allowed client configuration keys

Either lower case or upper case strings are accepted.

- `"allow_http"`: Allow non-TLS, i.e. non-HTTPS connections.
- `"allow_invalid_certificates"`: Skip certificate validation on https connections.

    !!! warning

        You should think very carefully before using this method. If
        invalid certificates are trusted, *any* certificate for *any* site
        will be trusted for use. This includes expired certificates. This
        introduces significant vulnerabilities, and should only be used
        as a last resort or for testing

- `"connect_timeout"`: Timeout for only the connect phase of a Client
- `"default_content_type"`: default `CONTENT_TYPE` for uploads
- `"http1_only"`: Only use http1 connections.
- `"http2_keep_alive_interval"`: Interval for HTTP2 Ping frames should be sent to keep a connection alive.
- `"http2_keep_alive_timeout"`: Timeout for receiving an acknowledgement of the keep-alive ping.
- `"http2_keep_alive_while_idle"`: Enable HTTP2 keep alive pings for idle connections
- `"http2_only"`: Only use http2 connections
- `"pool_idle_timeout"`: The pool max idle timeout.
    This is the length of time an idle connection will be kept alive.
- `"pool_max_idle_per_host"`: maximum number of idle connections per host.
- `"proxy_url"`: HTTP proxy to use for requests.
- `"timeout"`: Request timeout.
    The timeout is applied from when the request starts connecting until the
    response body has finished.
- `"user_agent"`: User-Agent header to be used by this client.
"""
