from datetime import timedelta

from obstore.store import HTTPStore


def test_construction_with_backoff_config():
    HTTPStore.from_url(
        "https://...",
        client_options={
            "connect_timeout": "4 seconds",
            "timeout": "16 seconds",
        },
        retry_config={
            "max_retries": 10,
            "backoff": {
                "base": 2,
                "init_backoff": timedelta(seconds=1),
                "max_backoff": timedelta(seconds=16),
            },
            "retry_timeout": timedelta(minutes=3),
        },
    )


def test_construction_partial_retry_config():
    HTTPStore.from_url(
        "https://...",
        client_options={
            "connect_timeout": "4 seconds",
            "timeout": "16 seconds",
        },
        retry_config={
            "max_retries": 10,
        },
    )
    HTTPStore.from_url(
        "https://...",
        client_options={
            "connect_timeout": "4 seconds",
            "timeout": "16 seconds",
        },
        retry_config={
            "max_retries": 10,
            "backoff": {
                "init_backoff": timedelta(seconds=1),
            },
            "retry_timeout": timedelta(minutes=3),
        },
    )
